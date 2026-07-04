# model.py & stable_prefix_tuning.py — 深度解读

Status: **complete**

## 概述

两个独立 Python 脚本，共享 ASCII art banner，同一作者。分别探索**行为经济学逆向交易**和**Prefix Tuning 稳定性**。

---

## model.py — 行为经济学逆向交易回测 (268行)

### 做什么

下载 DOGE 加密货币 + Gold/NQ/VIX 宏观数据 → 滚动随机森林预测次日涨跌方向 → **逆向操作**（预测涨→做空，预测跌→做多）→ 不确定时转黄金避险。

### 核心逻辑

```python
# 行为经济学假设：市场参与者存在 confirmation bias（确认偏误）
# 随机森林学会的是"大众预测"，所以取反就是"反大众"
if prob_up >= 0.55:   → SHORT（做空，大众看涨时反着来）
elif prob_up <= 0.45: → LONG （做多，大众看跌时反着来）
else:                 → GOLD （不确定，避险）
```

### 特征工程（8个）

| 特征 | 含义 |
|------|------|
| Dist_SMA20 | 现价偏离20日均线 |
| Mom_5 | 5日动量 |
| RSI | 14日相对强弱 |
| ATR | 14日平均真实波幅 |
| Intraday_Return | 日内收益 close/open-1 |
| NQ_Ret_Prev | 纳斯达克前一日收益率 |
| VIX_Ret_Prev | VIX 前一日变化率 |
| Gap | 开盘跳空 open/prev_close-1 |

### 策略细节

- **训练**: 180天滚动窗口，每20天重新训练
- **模型**: RandomForestClassifier (100棵树, max_depth=2, min_samples_leaf=20) — 非常浅，故意欠拟合
- **仓位**: 基于 ATR 的杠杆上限 `min(3.0, 0.10/ATR)`，波动大→降低杠杆
- **止损**: -15% (日内 MAE 检查)
- **成本**: 0.04% per trade + 0.01% per position
- **指标**: Sharpe / Sortino / Calmar / MDD / Win Rate

### 评价

**亮点**:
- 逆向思维的量化实现：预测 herd behavior 然后取反
- 使用黄金作为避险备选（不确定时转避险资产）
- 基于 ATR 的动态仓位管理
- 浅层随机森林 + 小训练窗口 = 防止过拟合
- 完整的回测指标和可视化

**问题**:
- 仅测试 DOGE（一种 meme coin），无普适性
- 行为经济学假设（confirmation bias = 预测取反）缺乏验证
- 注释中提到"未来改动：直接预测 confirmation bias"说明这是中间版本
- 黄金避险阈值 (0.45~0.55) 是任意的

### 可迁移模式

**"预测偏差→逆向交易"框架** — 不直接预测价格，而是预测市场情绪偏差然后反向操作。这是行为金融学的一个工程落地模板。适用于任何有明确"羊群效应"的市场（加密货币、meme股）。

---

## stable_prefix_tuning.py — Prefix Tuning 稳定性实验 (214行)

### 做什么

对比 4 种技术方案，看哪种能让 Qwen3-0.6B 的 Prefix Tuning 训练更稳定（loss 收敛更好）。任务：GLUE SST-2 情感分类。

### 架构

```python
CustomPromptTuningModel:
  - 10个虚拟token → Embedding → (可选MLP) → 拼接在输入序列前
  - base_model 全部冻结，只训练虚拟 token embeddings
  - labels: 只计算"Sentiment:"之后的token的loss（prefix部分mask为-100）
```

### 4 种对比方案

| 方案 | 技术 | 原理 |
|------|------|------|
| 1. Raw Prefix | 直接优化 embedding | baseline |
| 2. MLP_Prefix | MLP(64→128→64) 重参数化 | 间接参数化稳定优化 |
| 3. Raw_SAM | SAM 优化器 | 寻找平坦最小值→更好泛化 |
| 4. Raw_HessianTrace | Hessian 迹惩罚 | 强制 loss 曲率平坦 |

### SAM 实现（从零手写, 98-131行）

```python
# Step 1: 梯度上升找到最差扰动方向
e_w = grad * (rho / ||grad||)
p += e_w  # climb to local maximum

# Step 2: 在扰动位置重新计算梯度并下降
p -= e_w  # back to original
optimizer.step()  # 用扰动位置的梯度更新
```

这是一个正确的最小 SAM 实现：`first_step` 在参数空间沿着梯度方向走一步（找最差点），`second_step` 在原位置用扰动点的梯度做更新。不需要维护两份参数副本，通过 `state[p]["e_w"]` 保存扰动向量。

### Hessian 迹估计（Hutchinson's estimator, 133-144行）

```python
v = [randn_like(p) for p in params]  # Rademacher 随机向量
hvp = grad(grad(loss), params, v)     # Hessian-vector product
trace ≈ Σ(h_i · v_i)                  # Hutchinson 估计
```

这是 Hessian 迹的无偏估计 — 不需要计算完整的 Hessian 矩阵。用于惩罚 loss landscape 的曲率，鼓励更平坦的极小值。

### 评价

**亮点**:
- SAM 从零手写实现，不是调库
- Hutchinson's Hessian trace 是正确的数学实现
- 只训练 10 个 token × 64 维 = 640 个参数，极致 parameter-efficient
- MPS 支持 (Apple Silicon)

**问题**:
- 只比较训练 loss，没有验证集准确率 — 无法判断哪种方案真正更好
- 3 epochs 太少，4种方案可能都没收敛
- Hessian trace 的 alpha=1e-4 是 magic number
- SAM 的 rho=0.05 也是 magic number
- Qwen3-0.6B 做 SST-2 是大材小用

### 可迁移模式

1. **SAM 优化器最小实现** — 22行手写，可直接嵌入任何 PyTorch 训练管线。金融模型对过拟合敏感的场景尤其适用。
2. **Hessian Trace Penalty** — 不需要完整 Hessian 的曲率正则化。对小样本金融模型的泛化提升有理论优势。
3. **Prefix Tuning 模板** — 冻结 base model + 只训练虚拟 token 的 paradigm，适合将大模型适配到金融 NLP 任务（情绪分析、新闻分类）。

---

## 总结

| 维度 | model.py | stable_prefix_tuning.py |
|------|----------|------------------------|
| 类型 | 量化回测策略 | ML 实验脚本 |
| 核心思想 | 预测偏差→逆向交易 | 训练稳定性对比 |
| 数据 | Yahoo Finance (DOGE/Gold/VIX) | GLUE SST-2 |
| 模型 | RandomForest (50棵树) | Qwen3-0.6B + Prefix |
| 复杂度 | 简单 | 中等（手写 SAM + HVP） |
| 可复用性 | 逆向交易框架 | SAM 优化器 + Hessian 正则化 |
| 行数 | 268 | 214 |
