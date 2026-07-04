# RV_Transformer_CTA — 深度解读

Status: **complete**

## 一句话总结

> 一套完整的 **Transformer 期权波动率交易系统**：用 Transformer 预训练+微调预测 IV 相对 RV 的升降方向，信号驱动 Straddle/Iron Condor 策略。架构与 `all_weather_hedging` 共享同一模块化管线模式，且多了预训练-微调范式、SVD 稀疏正则化、5 个可迁移技术模式、和单元测试。

> ⚠️ 命名陷阱：项目名含 "CTA" 但实际不是商品期货趋势跟踪。它做的是**期权波动率套利** — 交易 Straddle/Iron Condor（delta中性，纯vol方向）。更准确的名字应为 `RV_Transformer_VolArb`。

---

## 系统全景

```
L2 订单簿流 → 34 维特征提取 → Transformer(128d/8头/4层)
                                    ↓
              pretrain: 掩码自编码重建 (15% mask)
              finetune: 二分类 (IV升 vs IV降)
                                    ↓
              每1秒预测 → 概率 ≥ 0.6 ?
                  ↓                ↓
           pred=1 (IV↑)      pred=0 (IV↓)
              ↓                    ↓
         Straddle buy       Iron Condor sell
              ↓                    ↓
           止损 -5%          止损 -5%
```

---

## 逐模块分析

### 1. config.py — dataclass 配置 (40行)

| 配置组 | 关键值 |
|--------|--------|
| ModelConfig | d_model=128, nhead=8, num_layers=4, dim_ff=512, seq_len=100, feature_dim=34, num_classes=2 |
| TrainingConfig | pretrain_lr=1e-4, finetune_lr=5e-5, batch_size=64, mask_ratio=0.15, svd_l1_coeff=1e-4 |
| TradingConfig | rv_iv_threshold=0.02, min_iv=0.10, max_position=1.0, stop_loss=-5%, allocation_ratio=0.1 |
| SystemConfig | depth_levels=5, rolling_rv_window=300, prediction_interval_sec=1 |

**亮点**: 使用 `dataclasses.dataclass` 而非字典，类型安全。`SystemConfig` 作为聚合容器嵌套三个子配置，设计干净。

---

### 2. data/stream_parser.py — L2订单簿引擎 (142行)

**34维特征设计**:

| 索引 | 特征 | 含义 |
|------|------|------|
| 0 | WAP | 加权平均价格 (bid_q×ask_p + ask_q×bid_p)/(bid_q+ask_q) |
| 1 | mid_price | 中间价 (bid+ask)/2 |
| 2 | spread | 买卖价差 |
| 3 | OFI | 订单流不平衡 (← delta bid qty - delta ask qty) |
| 4 | RV | 已实现波动率 (log return平方和开方) |
| 5 | trade_vol | 成交总量 |
| 6 | trade_imbal | 买卖不平衡 |
| 7 | trade_vwap | VWAP |
| 8 | trade_cnt | 成交笔数 |
| 9-13×5 | depth_N | bid_p, bid_q, ask_p, ask_q, imbalance (×5层) |

**OFI 算法** (`calculate_ofi`): 比较最近两笔报价——bid上涨→新买单，bid不变→量变化，bid下跌→撤单。这是高频微观结构研究的标准做法。

**RV 算法**: `sqrt(Σ log_return²)` 滚动窗口300个tick。常规做法。

**亮点**: 这是一个生产级 L2 特征提取器。34维特征设计覆盖了价格(WAP/mid)、流动性(spread/OFI)、波动率(RV)、交易(vol/imbal/vwap/cnt)、深度(5层×5子特征)。任何一个做limit order book预测的项目都可以复用这个特征工程。

---

### 3. data/feature_pipeline.py — 特征管线 (74行)

**两组关键设计**:

1. **`FreaturePipeline.process_live_vector`**: 实时场景下的**在线标准化**——用EMA更新均值和标准差，避免离线全量标准化依赖。`alpha = min(1/count, 0.01)` 实现渐进收敛。

2. **`VolatilityDataset`** (PyTorch Dataset):
   - pretrain模式: 返回 `(masked_seq, orig_seq, mask)` — 掩码自编码
   - finetune模式: 返回 `(seq, label)` — 标签 = `future_RV > current_IV ? 1 : 0`
   - 离线全量标准化: `(x - mean) / std`

**标签构造** (finetune 模式, `__getitem__` line 68-72):
```python
future_rv = feature_matrix[future_idx, 4]  # 索引4 = RV
current_iv = iv_series[start_idx + seq_len - 1]
label = 1 if future_rv > current_iv else 0  # RV > IV → IV被低估，预测将升
```
逻辑: 如果未来 RV 高于当前 IV，说明当前 IV 被低估，预测 IV 将上升。反之亦然。

**关键问题**: RV 索引硬编码为 4（`feature_matrix[future_idx, 4]`）。如果特征顺序改变，这里会静默出错。应该用枚举或 named index。

---

### 4. models/transformer.py — VolatilityTransformer (51行)

```
34维特征 → Linear(34→128) → PositionalEncoding(sin/cos)
         → TransformerEncoder(4 layers, 8 heads, ff=512, dropout=0.1)
              ↓
    mode="pretrain":  Linear(128→34) → 每个时间步重建原始特征
    mode="finetune":  Linear(128→2) → 取last_token → 二分类logits
```

**设计亮点**:
- **单模型双模式**: 同一架构通过 `mode` 参数切换pretrain/finetune头，无需两个模型对象
- **last_token 分类**: finetune时取序列最后一个时间步的表示做分类，符合时序预测的因果方向
- **PositionalEncoding**: 标准 sin/cos 编码，`register_buffer` 保证不参与梯度
- 约 2.5M 参数，对金融小样本场景适中

**vs all_weather_hedging 的 CNN**: Transformer 的自注意力全局感受野在理论上有优势（捕捉长程依赖），但参数量更大（2.5M vs 20K），对数据量要求更高。他们的选择合理：34维特征的时序模式比4维收益序列更复杂，值得用 Transformer。

---

### 5. models/regularization.py — SVD L1 正则化 (20行)

```python
for name, param in model.named_parameters():
    if "weight" in name and len(param.shape) == 2:  # 只正则化2D权重矩阵
        svd_vals = torch.linalg.svdvals(param + noise_1e-9)
        reg_loss += Σ |svd_vals|  # 核范数 = 奇异值L1和
```

**原理**: 核范数（Nuclear Norm）是秩的凸松弛。最小化核范数鼓励矩阵低秩，等价于权重稀疏化。这对金融模型很重要——高维金融特征中大部分是噪声，强制稀疏可以防止过拟合。

**噪声 trick**: `param + randn * 1e-9` 防止 SVD 在零奇异值时数值不稳定。巧妙但工程味重——更规范的做法是用 `torch.linalg.svdvals` 的默认容差。

---

### 6. training/pretrain.py — 掩码自编码预训练 (66行)

```
输入: 34维×100步序列，随机mask 15%
模型: mode="pretrain" → 输出100×34重建序列
损失: MSE(只计算被mask位置) + SVD L1 reg
优化: AdamW, lr=1e-4, 10 epochs
```

**为什么 pretrain 重要**: 金融数据的监督标签（IV方向）样本量有限且噪声大。先通过掩码自编码学习 L2 订单簿的底层结构（哪些特征相关、时序模式如何），再用少量标签微调——这是 NLP 已验证的范式，移植到金融时间序列是合理选择。

**损失设计细节**: `torch.sum(((pred - orig) * mask)^2) / clamp(sum(mask), min=1)` — 只计算被mask位置的MSE，不计算未mask位置。这确保模型真正学习"恢复缺失信息"，而不是简单复制输入。

---

### 7. training/finetune.py — 分类微调 (82行)

```
输入: 34维×100步序列 + IV序列
标签: future_RV > current_IV → 1, else → 0
模型: mode="finetune" → 输出2类logits
损失: CrossEntropy + SVD L1 reg
优化: AdamW, lr=5e-5, 15 epochs
```

**权重迁移**: 只加载 pretrain 的 Transformer encoder 权重，不加载 pretrain_head 和 classifier_head。标准的迁移学习做法。

**horizon 参数**: `horizon=60` — 在序列结束后60步看 RV 是否超过当前 IV。60步×1秒 = 60秒预测窗口。这是一个关键超参数，决定了交易信号的时效性和噪声比。

---

### 8. trading/executor.py — 执行器接口 (18行)

```python
class AbstractTradingExecutor(ABC):
    @abstractmethod
    def execute_strategy(strategy_type, action, allocation) -> dict
    @abstractmethod
    def get_positions() -> dict
    @abstractmethod
    def get_portfolio_value() -> float
    @abstractmethod
    def cancel_all_orders()
```

**设计价值**: 抽象执行层 = 可插拔后端。开发时用 Mock，实盘时替换为真实券商 API。这比 `all_weather_hedging` 更工程化。

---

### 9. trading/strategy.py — 波动率交易策略 (42行)

**状态机逻辑**:

```
当前无持仓:
  pred=1 AND prob≥0.6 → buy Straddle (delta中性，赌vol↑)
  pred=0 AND prob≥0.6 → sell Iron Condor (delta中性，赌vol↓)

当前持仓 Straddle:
  pred=0 → close Straddle
  drawdown≤-5% → stop loss close Straddle

当前持仓 Iron Condor:
  pred=1 → close Iron Condor
  drawdown≤-5% → stop loss close Iron Condor
```

**策略逻辑正确**: Straddle 做多波动率（IV升赚钱），Iron Condor 做空波动率（IV降/稳赚钱）。两者都是 delta 中性策略，纯波动率方向。

**问题**: 没有 theta（时间衰减）管理。Straddle 和 Iron Condor 都有显著的时间价值衰减，长期持有需要更复杂的管理。

---

### 10. pipeline/realtime_engine.py — 实时引擎 (65行)

```
event_generator → for each event:
  depth event → update_order_book
  trade event → record_trade
  每1秒 → extract_34_features → normalize → 累积seq_len=100步
         → Transformer 推理 → softmax → 预测+概率
         → strategy.process_prediction
         → yield 结果
```

**设计模式**: 事件驱动架构，生成器模式。`process_event_stream` 接收 event generator，yield 每个预测结果。解耦了数据源、模型和策略。

---

### 11. main.py — CLI 入口 (135行)

三模式: `pretrain` (需要 .npy 历史L2数据) / `finetune` (需要 features+IV) / `live` (mock feed 演示)。

`LogTradingExecutor` 作为 mock 执行器，只打印不真实交易。

---

### 12. tests/test_system.py — 单元测试 (188行)

7个测试: config校验 / L2 orderbook解析 / SVD正则化 / Transformer形状 / Dataset / 交易策略状态机 / 实时引擎端到端。

**评价**: 相比 `all_weather_hedging` 完全无测试，这是显著的工程质量提升。交易策略的状态机测试覆盖了4个关键路径（买入→切换→止损→平仓），说明作者对实盘可靠性有意识。

---

## 与 all_weather_hedging 对比

| 维度 | all_weather_hedging | RV_Transformer_CTA |
|------|--------------------|--------------------|
| 策略 | 全天候宏观配置 | 期权波动率CTA |
| 模型 | 因果1D-CNN (20K) | Transformer (2.5M) |
| 训练 | 单阶段监督 | 预训练+微调两阶段 |
| 特征 | 4维收益序列 | 34维L2订单簿 |
| 正则化 | 无 | SVD L1 核范数稀疏化 |
| 执行层 | 硬编码 | AbstractTradingExecutor 抽象接口 |
| 测试 | 无 | 7个单元测试 |
| 数据 | 合成 | 合成 (但特征工程是真实市场可用的) |
| 期权 | BS Greeks + Deep OTM对冲 | Straddle/Iron Condor策略 |

---

## 5 个可迁移技术模式

1. **预训练→微调两阶段范式**: 掩码自编码预训练学底层结构 → 少量标签微调。金融数据标签稀少的通用解决方案。
2. **SVD L1 (核范数) 正则化**: 鼓励权重矩阵低秩 = 特征稀疏化。高维金融特征的防过拟合利器。
3. **AbstractTradingExecutor 抽象层**: 执行器ABC → Mock/Paper/Live 可插拔。量化系统工程化的标准做法。
4. **L2 订单簿 34 维特征工程**: WAP/mid/spread/OFI/RV/trade_metrics/5层depth。可直接复用到任何 microstructure 模型。
5. **概率门控交易**: 仅在 prob≥0.6 时入场，止损坏在 -5%。用置信度阈值防止噪声交易，用止损控制尾部风险。

---

## 归档决策

- 研究笔记归档，不升格 durable memory
- 可升格判断: "预训练→微调是金融小样本建模的首选范式" → 写入 AI 架构记忆
