# TradePilot 商品期货阶段 1：M 单合约与一次换月审计

Generated at: `2026-07-23T09:47:37.685616+00:00`
Code version: `3c473d984451768c151b2ef9e1e76c4644a11fff-dirty`
Snapshot id: `a2c7f920e10a5ec1`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`

## Scope

本报告只覆盖 Stage 1 的最小样本：豆粕 `M.DCE` 的单合约计算样例，以及一次主力切换窗口审计；不构建连续合约，不进入篮子研究。

## Roll Selection

| Root | Roll date | Roll from | Roll to | Window |
| --- | --- | --- | --- | --- |
| M.DCE | 2025-04-07 | M2505.DCE | M2509.DCE | 5 trading days before/after |

## Single Contract Calculation

| Contract | Trade date | Close | Multiplier | Trade unit | Quote unit | One-lot notional | P/L for 1% move |
| --- | --- | --- | --- | --- | --- | --- | --- |
| M2509.DCE | 2025-04-07 | 3056 | 10 | 吨 | 人民币元/吨 | 30560 | 305.6 |

## Roll Window Audit

| Trade date | Mapped active | Contract | Close | Settle | Volume | OI | Mapped? |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2025-03-28 | M2505.DCE | M2505.DCE | 2813 | 2826 | 1054314 | 1486764 | yes |
| 2025-03-28 | M2505.DCE | M2509.DCE | 2942 | 2952 | 547756 | 1795382 | no |
| 2025-03-31 | M2505.DCE | M2505.DCE | 2851 | 2830 | 1193856 | 1358857 | yes |
| 2025-03-31 | M2505.DCE | M2509.DCE | 2989 | 2966 | 682366 | 1833643 | no |
| 2025-04-01 | M2505.DCE | M2505.DCE | 2804 | 2816 | 1250988 | 1297118 | yes |
| 2025-04-01 | M2505.DCE | M2509.DCE | 2951 | 2961 | 865877 | 1843857 | no |
| 2025-04-02 | M2505.DCE | M2505.DCE | 2832 | 2820 | 865772 | 1222762 | yes |
| 2025-04-02 | M2505.DCE | M2509.DCE | 2983 | 2969 | 683460 | 1864781 | no |
| 2025-04-03 | M2505.DCE | M2505.DCE | 2865 | 2843 | 1248774 | 1108276 | yes |
| 2025-04-03 | M2505.DCE | M2509.DCE | 3039 | 3005 | 1442436 | 1960957 | no |
| 2025-04-07 | M2509.DCE | M2505.DCE | 2885 | 2903 | 1049621 | 965560 | no |
| 2025-04-07 | M2509.DCE | M2509.DCE | 3056 | 3079 | 2068098 | 2069991 | yes |
| 2025-04-08 | M2509.DCE | M2505.DCE | 2973 | 2922 | 986295 | 854483 | no |
| 2025-04-08 | M2509.DCE | M2509.DCE | 3164 | 3102 | 2551277 | 2349506 | yes |
| 2025-04-09 | M2509.DCE | M2505.DCE | 2946 | 2957 | 745874 | 783576 | no |
| 2025-04-09 | M2509.DCE | M2509.DCE | 3119 | 3133 | 2980757 | 2416074 | yes |
| 2025-04-10 | M2509.DCE | M2505.DCE | 2896 | 2919 | 604887 | 708455 | no |
| 2025-04-10 | M2509.DCE | M2509.DCE | 3074 | 3093 | 2599623 | 2396848 | yes |
| 2025-04-11 | M2509.DCE | M2505.DCE | 2912 | 2907 | 456346 | 674464 | no |
| 2025-04-11 | M2509.DCE | M2509.DCE | 3082 | 3074 | 1948453 | 2368722 | yes |
| 2025-04-14 | M2509.DCE | M2505.DCE | 2958 | 2947 | 527661 | 657057 | no |
| 2025-04-14 | M2509.DCE | M2509.DCE | 3104 | 3097 | 2164045 | 2426884 | yes |

## Roll Gap Audit

| Date | Previous date | Old close | New close | Same-day spread | Same-day spread % | Naive series gap | Naive series gap % | Old settle | New settle | Same-day settle spread | Naive settle series gap |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2025-04-07 | 2025-04-03 | 2885 | 3056 | 171 | 5.9272% | 191 | 6.6667% | 2903 | 3079 | 176 | 236 |

## Stage 1 Decision

结论：`pass`。本窗口所有新旧合约审计行均包含 `close/settle/volume/oi`，单合约名义价值和 1% 价格变动盈亏可由原始 `close` 与 `multiplier` 复算。

同日换月价差为 `M2509.DCE` close `3056` 减 `M2505.DCE` close `2885`，即 `171` 点、`5.9272%`。真实天真主力序列跳空则是从 2025-04-03 `M2505.DCE` close `2865` 到 2025-04-07 `M2509.DCE` close `3056`，形成 `191` 点、`6.6667%`；该跳空混合了市场单日变化和合约切换价差，不能解释为可交易的单日市场收益，也不是实际移仓成本。

Stage 2 收益口径决策：连续合约同时保留 `close` 与 `settle` 两套复权序列；默认绩效、波动、回撤和后续篮子研究冻结使用 `adjusted_close` 计算的 `continuous_return`。`settle` 口径作为 `adjusted_settle` / `settle_return_audit` 保留，用于审计、结算语义对照和敏感性说明，不能在看到绩效后替换主口径。复权公式冻结为比值法后向复权，绝对 `roll_gap` 仍保留用于解释换月价差。

后续 Stage 2 若构建连续合约，应继续保留新旧合约价格、换月调整量和来源批次，并禁止用天真拼接序列计算绩效。
