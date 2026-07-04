# 筹码分析与半日频调优 — 深度解读

Status: **complete**

## 一句话总结

> **这是一套真实的 A 股实盘交易系统**（CSI500 + XtQuant/QMT 券商通道 + 5万资金），采用三专家 MWU 架构（E2E Transformer → NLP 舆情 → 筹码分析）通过 CVXPY MILP 离散优化选择 Top-K 组合并执行。1428 行 infra.py 是核心引擎，含完整风控层。附带 P0-P4 筹码增强路线子项目。**是 AllWeather 目录下唯一直接对接真实券商账户的生产系统。**

---

## 系统全景

```
CSI500 成分股 (~500只)
        │
   ┌────┼────┬──────────────┐
   ▼    ▼    ▼              ▼
 E2E  NLP  Chip          价格/风控
   │    │    │              │
   ▼    ▼    ▼              │
 三专家 MWU 动态权重 ←─── 历史收益反馈
   │
   ▼
 z-score 标准化融合
   │
   ▼
 CVXPY MILP 离散优化 (整数手数)
   │
   ├── apply_live_position_cap (≤8只)
   ├── apply_same_day_no_buyback
   ├── apply_sell_cooldown_buys
   └── apply_cost_aware_buys
   │
   ▼
 XtQuant/QMT 真实券商执行 (先卖后买)
```

---

## 逐模块分析

### 1. Config + 基础设施 (infra.py:44-104)

| 配置项 | 值 | 含义 |
|--------|-----|------|
| INDEX_CODE | 000905.SH | CSI500 |
| HOLDING_COUNT | 20 | CVXPY 优化 Top-K |
| MAX_LIVE_POSITIONS | 8 | 实盘最大持仓只数 |
| RISK_VOL_LIMIT | 0.08 | 日内振幅 8% 风控 |
| MWU_ETA | 0.05 | MWU 学习率 |
| WINDOW_SIZE | 10 | E2E 时间窗口 |
| NUM_FEATURES | 20 | E2E 特征数 |
| COMMISSION_RATE | 0.0001 | 万一佣金 |
| CHIP_WEIGHT_FLOOR | 0.05 | 专家权重下限 |
| COOLDOWN_CALENDAR_DAYS | 0 | 卖出冷却天数 |

**数据源**: Tushare Pro (宏观/基本面/筹码) + XtQuant (实时行情/交易) + akshare (新闻) + OpenAI API (情绪打分)

---

### 2. StockLLM — E2E 预测模型 (model_e2e.py:62行)

```
20维特征×10天窗口 → Linear(20→64) + 可学习位置编码
                  → ModernBlock × N (RMSNorm + MultiheadAttention + SwiGLU MLP)
                  → RMSNorm → last_token → Linear(64→32→1) → 预测收益
```

**架构亮点**:
- **RMSNorm** 替代 LayerNorm (更高效)
- **SwiGLU** 激活 (LLaMA 架构标配)
- **ModernBlock**: Pre-Norm + Attention residual + MLP residual
- 可学习位置编码 (非固定 sin/cos)
- 权重初始化: N(0, 0.02²)

**vs RV_Transformer**: StockLLM 比 VolatilityTransformer 更轻量 (64d vs 128d, 1层 vs 4层)，但使用了更现代的架构组件 (RMSNorm/SwiGLU)。这是为 CSI500 ~500 只股票的横截面预测设计的，不是时序预测。

**注意**: infra.py 中还包含一个旧版 StockLLM (line 107-123)，使用标准 TransformerEncoderLayer。实际加载的是 model_e2e.py 的版本。

---

### 3. 20维特征工程 (infra.py:261-358)

| 类别 | 特征 (7个基础+13个衍生) |
|------|------------------------|
| 价格 | open, high, low, close |
| 成交量 | vol |
| 动量 | roc_5, roc_20, macd, rsi |
| 波动率 | std_20, intraday_ret, shadow_up |
| 基本面 | turnover, ep_ratio, bp_ratio, ln_mkt_cap |
| 复合 | ret_vol, smart_money, efficiency, value_mom |

**smart_money**: close/vwap - 1 (主力资金痕迹)
**efficiency**: |close_change| / path_length (趋势效率)
**value_mom**: ep_ratio × roc_20 (价值+动量共振)

**预处理**: 截面 z-score 标准化 + clamp [-3, 3] + ffill

---

### 4. NLP Engine — 舆情专家 (infra.py:394-623)

**管线**:
```
akshare 新闻拉取 → sentence-transformers 语义相似度
    ├── 个股新闻: 股票代码 → stock_news_em → 直接关联
    └── 公共新闻: 行业关键词 → 余弦相似度路由 → stock_sim_min=0.35
         ↓
    OpenAI API 情绪打分 (概率>0时为正向，<0为负向)
         ↓
    个股情绪 = mean(个股新闻×1.0 + 公共新闻×0.6)
```

**多级缓存**: biz_desc_cache.json (业务描述) + news_cache.json (按日) + sentiment_cache.json (情绪分)

**特点**: 公共新闻按行业路由，权重折扣 0.6。这是真正的多信号融合思路——不是所有新闻都平等。

---

### 5. ChipEngine — 筹码专家 (infra.py:625-772)

**数据源**: Tushare `cyq_perf` 接口 — 获取筹码分布（winner_rate, weight_avg, cost_15pct, cost_85pct）

**筹码集中度**: `concentration = 1 - min((c85-c15)/close / 0.6, 1)` → [0,1]，越高越集中

**评分公式**:
```
score = 0.40 × (winner_rate - 0.50) × 2    # 获利盘占比 → 筹码健康度
      - 0.30 × concentration                # 集中度 → 越集中越扣分? (反直觉!)
      - 0.20 × cost_bias                    # 现价偏离平均成本 → 获利压力
      + 0.10 × profit_trend                 # 获利趋势变化 → 资金流入信号
```

**⚠️ 反直觉**: 系数 -0.30 × concentration — 越集中反而扣分？这与《需要阅读内容》中"单峰密集优先开仓"的建议**相反**。可能的解释：集中度极高可能意味着主力控盘、流动性差。或者这是一个有意设计的反向信号。

**缓存**: 7日 JSON 缓存，失败时 fallback 到缓存值。

---

### 6. MWU Manager — 权重融合 (infra.py:774-964)

**三专家初始权重**: e2e=1/3, nlp=1/3, chip=1/3

**Reward 计算**: 每个专家的 Top-20 平均次日收益

**MWU 更新**: `w(t+1) = w(t) × (1 + η × reward)`

**权重下限**: 0.05 (防止某一路完全塌陷)

**Fuse 融合**: 各路 z-score 标准化 → 加权求和

**MWU 健康检查**: 每路信号的非零率、reward、权重变化，详细日志

---

### 7. Execution — 交易执行 (infra.py:966-1354)

**组合优化 (CVXPY MILP)**:
```
max Σ(lots × price × score)
s.t. lots ≥ 0, integer
     Σ(lots × price) × 100 ≤ total_cash × 0.98
     lots_i × price_i × 100 ≤ total_cash × 0.10  (单只≤10%)
```

使用 ECOS_BB 或 SCIPY 整数规划求解器。

**五层风控过滤器**:

| 层 | 功能 | 机制 |
|----|------|------|
| 1 | 振幅风控 | 日内振幅 > 8% → 跳过该股买卖 |
| 2 | 持仓上限 | >8只触发 → 清仓最低分 K=M-8 只 |
| 3 | 当日禁回购 | 卖出过的股票当日不再买入 |
| 4 | 卖出冷却 | N 天内不再买入 (当前 N=0 未启用) |
| 5 | 成本过滤 | 融合分×期望边际 < 买入佣金 → 取消买入 |

**执行**: 先卖后买，FIX_PRICE 限价单，每次下单前检查 tick 数据有效性。

---

### 8. 筹码增强路线子项目 (P0-P4)

| 阶段 | 内容 | 状态 |
|------|------|------|
| P0 | 数据规格冻结 (5文档) | ✅ 已实施 |
| P1 | 多期筹码趋势 (chip_hist_offline.py) | ✅ 已实施 |
| P2 | 资金流 (Tushare moneyflow) | ✅ 已实施 |
| P3 | E2E 共振信号 (resonance_offline.py) | ✅ 已实施 |
| P4 | MWU/执行接入 (post_fuse_gate.py占位) | 🔒 待接入 |

**P3→P4 准入门槛**: Spearman IC 均值正增长 + 70%批次优于基线 + 并行10天模拟 + 自动回滚机制

---

## 与前面两个项目的对比

| 维度 | all_weather_hedging | RV_Transformer_CTA | **筹码分析** |
|------|--------------------|--------------------|-------------|
| 定位 | 演示/研究 | 演示/研究 | **实盘生产** |
| 数据 | 全合成 | 合成+Mock | **真实 A 股 (Tushare+XtQuant)** |
| 券商连接 | 无 | 无 | **XtQuant/QMT 真实账户** |
| 模型 | CNN 20K | Transformer 2.5M | Transformer 轻量 (~500K) |
| 专家数 | 单一 | 单一 | **三专家 MWU 动态权重** |
| 风控 | 基本 | 止损-5% | **五层过滤器** |
| 优化器 | Newton 风险平价 | 无 (直接策略) | **CVXPY MILP 整数规划** |
| 测试 | 无 | 7个单元测试 | 验收文档+离线验证 |
| 代码量 | ~600行 | ~500行 | **~2000行** |
| 复杂度 | 中 | 中高 | **高/生产级** |

---

## 8 个可迁移技术/工程模式

1. **三专家 MWU 动态权重** (infra.py:774-964) — 不是固定融合，而是用 Top-K 实际收益做 reward 动态调整专家信任度。任意多信号融合场景可直接套用。

2. **CVXPY MILP 离散组合优化** (infra.py:979-1056) — 整数手数约束的凸优化。A 股 100 股整数倍约束的标准解法。

3. **五层风控过滤器链** (infra.py:1063-1287) — 振幅→持仓上限→当日禁回购→冷却期→成本过滤。不是单一止损，而是多层防御。实盘必备。

4. **公共新闻行业路由+相似度阈值** (infra.py:448-623) — 不是简单关键词匹配，而是 embedding 余弦相似度 + 行业映射。NLP 信号质量控制的标准做法。

5. **筹码集中度 + 获利比例 + 成本偏离** (chip_tushare.py + infra.py:652-684) — Tushare cyq_perf 数据的生产级使用方法。筹码分析的量化落地模板。

6. **专家权重下限 + 健康检查** (infra.py:866-917) — MWU 权重设 floor 防塌陷，详细日志诊断信号质量。防止黑箱的工程实践。

7. **卖出冷却 + 当日禁回购** (infra.py:1122-1237) — 防止频繁交易的制度性约束。A 股 T+1 环境下的最佳实践。

8. **离线验证→主链接入的分阶段路线** (筹码增强路线/) — P0 规格冻结 → P1-P3 离线验证 → P4 才有资格动主链。**大系统迭代的标准方法论**。

---

## 需要警惕的问题

1. **筹码集中度系数为负** — 与《需要阅读内容》建议的"单峰密集=重仓"相反。可能是作者基于回测做的最优选择，也可能是 bug。
2. **Tushare Token 明文** — config 中有实际 token，需脱敏。
3. **单文件 1428 行** — infra.py 承担了全部核心逻辑，缺乏模块化。如果继续增长需要拆分。
4. **OpenAI API 依赖** — NLP 情绪打分依赖外部 LLM，成本、延迟和稳定性都是单点风险。
5. **无单元测试** — 相比 RV_Transformer 有 7 个测试，这个生产系统反而没有。

---

## 归档决策

- 研究笔记归档，不升格 durable memory
- 可升格: "三专家 MWU 动态权重是最小可行的多信号融合范式" → 写入投资记忆
