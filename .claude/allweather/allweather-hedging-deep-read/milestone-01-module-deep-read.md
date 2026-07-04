# Milestone 01 — Module Deep Read: all_weather_hedging/

Status: **complete**

## Architecture Overview

```
main.py
  ├── pretrain: RiskModelPretrainer.run()
  │     ├── 合成数据生成 (5000 samples, regime switch every 500 steps)
  │     ├── AllWeatherRisk1DCNN (1D-CNN + dilated residual blocks)
  │     ├── 三任务联合训练: vol(MSE) + corr(MSE) + cvar(MSE)
  │     └── 保存 cnn_weights.pth
  └── run_live_pipeline() [async]
        ├── FinancialDataStreamer → 异步流式生成 chunk
        ├── RiskModelInference → CNN 预测 vol/corr/tail
        ├── AllWeatherRiskParityOptimizer → Newton法求风险平价权重
        ├── PortfolioRiskEngine → VaR/CVaR + MRC/RC 分解
        ├── FinancialDataGenerator.generate_option_chain → BS Greeks
        └── DeepOTMHedgingOptimizer → GD优化虚值期权对冲
```

## Module-by-Module Analysis

### 1. config.py (38 lines)

**What it does**: 定义所有超参数和常量。

| 参数组 | 关键值 | 评价 |
|--------|--------|------|
| 资产定义 | 4 macro assets: EQUITY/BOND/COMMODITY/INFLATION | 正确映射全天候四象限 |
| 股票池 | 50 synthetic tickers (STK_000~STK_049) | 纯合成，非真实A股 |
| CNN架构 | seq_len=64, channels=8→32, dilations=[1,2,4,8] | 扩张率模仿WaveNet，合理 |
| 预训练 | 5000 samples, 15 epochs, lr=0.001, 80/20 split | 数据量偏小，15 epoch应该足够过拟合合成数据 |
| 风险预算 | [0.4, 0.3, 0.15, 0.15] | EQUITY 40%最高, BOND 30%, COMMODITY+INFLATION各15% |
| 期权参数 | delta limit 0.15, 3种到期日(30/60/90天) | OTM筛选合理 |
| 流式参数 | 10ms间隔, 100 chunk size, 20 total chunks | 模拟实时环境 |

**vs 我们的框架**: 我们的风险预算是按象限变化的模板（金发姑娘/再通胀/衰退/滞胀各不同），他们是固定预算 [0.4, 0.3, 0.15, 0.15]。**他们没有做象限识别 → 预算映射这一层。**

---

### 2. models/cnn1d.py (99 lines)

**架构**: `AllWeatherRisk1DCNN`

```
输入: (batch, 4 assets, 64 timesteps)
  ↓
input_projection: Conv1d(4 → 32, k=1)   # 通道升维
  ↓
ResidualCausalBlock × 4 (dilations=[1,2,4,8])
    每块: CausalConv→BN→ReLU→CausalConv→BN→+残差→ReLU
  ↓
AdaptiveAvgPool1d(1) → (batch, 32)        # 全局池化
  ↓
┌─ vol_head:  Linear(32→16→4, Softplus)    → 4种资产波动率
├─ corr_head: Linear(32→16→6, Tanh)        → 下三角相关系数(6个)
└─ tail_head: Linear(32→16→4, Softplus)    → 4种资产尾部风险
```

**设计亮点**:
- **因果卷积** (CausalConv1d): 右侧padding后裁掉，确保t时刻只看t及之前，无未来信息泄露
- **扩张残差块**: [1,2,4,8] 感受野 = 1+2×(3-1)×(1+2+4+8) = 61，接近seq_len=64
- **多任务头**: 不同激活函数匹配输出域 (vol≥0→Softplus, corr∈[-1,1]→Tanh, tail≥0→Softplus)
- **轻量**: 约 20K 参数，适合月度宏观数据的小样本场景

**vs 我们的框架**:
- 我们的 v1 用**规则打分**（PMI/PPI/M1-M2/信用利差），不做收益序列预测
- 他们的 CNN 直接从**收益序列**预测 vol/corr/tail，跳过了**宏观象限识别**这一步
- 我们的路线图里，深度学习被明确放在 `defer`（端到端深度模型直接驱动配置权重），因为担心样本量不足和过拟合
- **关键判断**: 他们的 CNN 做的事情（收益序列→vol/corr/tail）和我们的状态识别层（宏观变量→象限→风险预算）是**正交的两个方向**。他们预测的是统计矩，我们识别的是经济 regime。两者可以结合：状态识别定预算方向，CNN 定矩估计精度。

---

### 3. models/pretrain.py (147 lines)

**预训练策略**:

```
数据生成:
  - 5064步随机游走（每500步regime switch：波动率随机缩放0.8~1.2倍）
  - 滑动窗口(64步) → X
  - 标签: 窗口内std → Y_vol, 窗口内corrcoef下三角 → Y_corr, 窗口内5% CVaR → Y_cvar

训练:
  - MSE三任务联合损失: 1.0*l_vol + 0.5*l_corr + 1.0*l_cvar
  - Adam, lr=0.001, 15 epochs
  - 80/20 train/val split
```

**评价**:
- **标签构造方式正确**: 用滑动窗口的历史std/corr/CVaR作为标签，模型学习的是"从近期收益序列推断当前统计特征"
- **Regime switch 模拟**: 每500步随机调整波动率，模拟市场从低波到高波的切换，比纯平稳随机游走更真实
- **损失权重**: vol和cvar权重1.0，corr权重0.5，合理——波动率和尾部风险对组合管理更重要
- **问题**: 5000样本对于月度数据来说约400年，但对于高频tick数据来说太少。实际训练时数据是合成tick级，所以5000样本其实覆盖了多种regime。

**vs 我们的框架**: 我们没有预训练环节的概念。我们的路线图里模型的训练是延迟到 v2 才考虑的。

---

### 4. models/inference.py (35 lines)

**推理流程**:

```python
macro_returns (list[64][4]) → tensor → CNN.forward()
  → vol (4,), corr_tri (6,), tail (4,)
  → 重建 4×4 相关矩阵 (对称化 + 对角线=1)
  → 返回 vol, corr_mat, tail
```

**评价**:
- 干净、无意外。从下三角重建完整相关矩阵的逻辑正确。
- 加载预训练权重，如果文件不存在也能运行（随机初始化）
- **关键问题**: 推理时没有置信度输出。CNN 输出的是确定性的 vol/corr/tail 预测，没有任何不确定性量化。这与我们的框架强调的"置信度只控制斜率"形成对比。

---

### 5. data/schema.py (74 lines)

**数据结构**: 纯 dataclass，类型清晰

```
StreamTick → StreamChunk → RiskReport + HedgeReport → StreamOutputChunk
```

**评价**: 设计干净，字段命名清晰。`OptionContract` 包含完整的 Greeks（delta/gamma/vega），为对冲优化提供输入。

---

### 6. data/generator.py (135 lines)

**数据生成机制**:

宏观资产收益:
```
4维多元正态分布，预设协方差矩阵:
  EQUITY-BOND: -0.00003 (微负相关，符合股债跷跷板)
  EQUITY-COMMODITY: +0.00002 (微正相关)
  COMMODITY-INFLATION: -0.00004 (微负相关)
```
→ 价格按 exp(ret) 累积更新

股票收益:
```
stock_ret = beta * equity_ret + idio_ret
  beta ∈ [0.5, 1.8] (随机分配)
  idio_vol ∈ [0.0005, 0.0015] (随机分配)
```
→ 股票池与宏观资产的 EQUITY 腿联动

期权链生成:
```
9个行权价 (80%~120% spot) × 3种到期日(30/60/90天) × CALL/PUT
= 54张合约/股票 × 50只股票 = 2700张合约/chunk
```
→ 含 volatility smile (二次调整): `iv = base_iv + 0.5*(K/S-1)^2 - 0.2*(K/S-1)`

**评价**:
- 数据生成逻辑自洽：股票的 beta 暴露将宏观风险传导到个股，期权 smile 符合市场惯例
- **但完全是合成数据**，没有任何真实金融序列的特征（肥尾、波动率聚集、相关性崩塌等 stylized facts）
- 期权链生成在每个tick重新计算（不是真的 listing），所以 ticker/spot 变化时 option universe 也在变

---

### 7. data/stream.py (53 lines)

**流式管道**:
```
async 循环生成 tick (10ms间隔)
  → 积累 deque(maxlen=64)
  → 满64个 → yield StreamChunk
  → 继续生成10个新tick（部分重叠，模拟滑动窗口）
```

**评价**: 使用 `deque` + `maxlen` 自动维护滑动窗口，简洁。`asyncio.sleep(10ms)` 模拟实时数据到达节奏。每个chunk之间10个tick的步进（不是每tick都预测），符合月度或日频调仓的实际需求。

---

### 8. risk/parity.py (49 lines)

**Newton法风险平价优化器**:

```
目标: 使 RC_i ≈ budget_i
方法: 最小化 f(x) = 0.5 * x^T Σ x - Σ budget_i * ln(x_i)
     → gradient: Σx - budget/x
     → Hessian: Σ + diag(budget/x²)
     → Newton step: x_new = x - H^{-1} * g
     → line search 保证 x > 0
     → 最终归一化
```

**评价**:
- 这是经典的 Spinu (2013) 风险平价 Newton 公式。正确。
- Line search 正确处理了非负约束。
- 如果 Hessian 奇异则 break，有防御性编程。
- **问题**: `budgets` 是固定的 `[0.4, 0.3, 0.15, 0.15]`，不随宏观环境变化。这就是我们的框架要解决的核心问题——预算应该来自状态识别层。
- 📎 补充深度解析：`supplement-newton-risk-parity.md` — Newton 法数学原理、算法步骤、实现陷阱、在全天候框架中的应用评估

---

### 9. risk/engine.py (39 lines)

**风险指标计算**:

```
Cov = diag(vol) @ corr @ diag(vol)
port_vol = sqrt(w^T Cov w)
VaR(95%) = 1.645 * port_vol           # 参数化正态VaR
CVaR = 0.5 * 参数化正态CVaR + 0.5 * CNN预测tail加权
MRC = (Cov @ w) / port_vol
RC = w * MRC
```

**评价**:
- 参数化 VaR/CVaR 假设正态分布，对于金融数据偏乐观
- **CVaR 的设计有亮点**: 50%参数化 + 50% CNN tail 预测，尝试用深度学习修正正态假设的偏差
- MRC/RC 分解正确

---

### 10. hedging/deep_otm.py (114 lines)

**Deep OTM 对冲优化器**:

```
筛选: |delta| ≤ 0.15 的虚值期权
优化: 梯度下降 500 步, lr=0.01
    目标: 最小化 |net_delta|² + 100*|net_gamma|² + L2流动性惩罚 + L1交易成本
    输出: 合约数量 q (四舍五入到整百)
```

**评价**:
- **核心思想正确**: 用低成本虚值期权对冲尾部风险，靠 gamma 提供非线性保护
- **gamma_weight=100**: 强烈偏好 gamma 中性化，因为 gamma 非线性保护是 OTM 期权的核心价值
- **L1+L2 正则化**: L1模拟交易成本（与数量成正比），L2模拟流动性冲击（与数量的平方成正比）
- **问题**: 梯度下降 500 步，lr=0.01，无 convergence check。但没有约束 q≥0（只能买不能卖），实际可能出现空头头寸
- **更根本的问题**: 合成数据的期权定价（BS模型）是市场的无套利价格，但真实市场中期权有买卖价差、流动性和偏度溢价。这个对冲优化在真实数据上可能表现很不同。

---

### 11. hedging/options.py (39 lines)

**Black-Scholes Greeks 引擎**:

标准的 BS 公式实现：price, delta, gamma, vega。正确、简洁。

**评价**: 手动实现 BS 而非使用库（如 py_vollib），代码清晰但缺 theta 和 rho。

---

### 12. pipeline/orchestrator.py (75 lines)

**实时管道编排器**:

```
对于每个 streaming chunk:
  1. CNN 推理 → vol, corr, tail
  2. 风险平价 → 最优权重
  3. 风险分析 → VaR, CVaR, MRC/RC
  4. 期权链生成 → 54张合约/股票 × 50 = 2700张
  5. 全局对冲优化 → HedgeReport
  6. yield StreamOutputChunk
```

**评价**: 管线结构清晰。`holdings = {ticker: 10000}` 是固定持仓，不做股票选择或仓位调整——这暴露了一个关键简化：**股票端的风险对冲是独立于宏观配置的**。全天候的核心应该是宏观层面的配置+对冲，而不是对每只股票做独立期权对冲。

---

### 13. main.py (61 lines)

**入口**: 先预训练 CNN，再跑实时管线演示。

输出格式: 每个chunk打印 VaR/CVaR/权重/风险贡献/对冲交易

---

## 整体架构评判

### 做得好的
1. **模块化分层清晰**: config→data→models→risk→hedging→pipeline，每层职责明确
2. **因果卷积**: 严格防止未来信息泄露
3. **多任务CNN**: vol/corr/tail三个输出共享特征提取，合理
4. **Newton风险平价**: 数学正确，工程扎实
5. **Deep OTM 对冲**: 用优化而非枚举的方法选择期权合约，思路新颖
6. **Async streaming**: 模拟实时环境，架构可扩展

### 做得不好/不足的
1. **全合成数据**: 无真实金融序列特征（肥尾、波动率聚集、相关崩塌）
2. **无宏观 regime 识别**: 模型直接从收益序列预测统计矩，不区分"当前处于什么宏观环境"
3. **固定风险预算**: [0.4,0.3,0.15,0.15] 不随环境变化，这是"全天候"和"固定风险平价"的本质区别
4. **无置信度/不确定性**: CNN输出确定性预测，无法表达"当前预测有多可靠"
5. **股票对冲 vs 宏观对冲的混淆**: pipeline对每只股票做独立期权对冲，但全天候应该做宏观层面的尾部保护
6. **无回测/验证**: 只有live demo输出，没有回测框架、参数敏感性分析、样本外验证
7. **预训练数据泄露风险**: 预训练用的合成regime switch和推理用的不同的合成数据生成器，但两者分布假设相同，无法证明泛化能力
