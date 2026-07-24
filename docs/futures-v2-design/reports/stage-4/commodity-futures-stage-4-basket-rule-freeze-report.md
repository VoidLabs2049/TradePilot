# TradePilot 商品期货阶段 4：商品篮子规则冻结报告

Generated at: `2026-07-24T08:20:43.469779+00:00`
Code version: `9bec58bc509e96f91ccf4aa82c530c77d4d1f798`
Snapshot id: `35535fa46a9465b8`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
Output path: `/home/nixos/workspace/TradePilot/data/lakehouse/derived/derived.futures_commodity_basket/part-00000.parquet`

## Scope

本报告只覆盖 Stage 4 商品篮子定义、权重规则冻结和风险贡献检查；不运行 ETF 基线增量回测，不形成商品 sleeve 接受结论。

## Frozen Rules

| Item | Value |
| --- | --- |
| Candidates | AL.SHF, CU.SHF, RB.SHF, I.DCE, M.DCE, P.DCE, SC.INE, TA.ZCE |
| Control | AU.SHF |
| Rebalance frequency | month_end |
| Volatility window | 252 |
| Minimum volatility observations | 126 |
| Weight cap | 25.0000% |
| Missing data rule | complete_case_across_stage4_roots |
| Performance field | continuous_return from Stage 2 adjusted_close |

## Candidate Decisions

| Root | Stage 3 decision |
| --- | --- |
| AL.SHF | observe |
| CU.SHF | observe |
| RB.SHF | accept |
| I.DCE | observe |
| M.DCE | accept |
| P.DCE | observe |
| SC.INE | observe |
| TA.ZCE | observe |

## Basket Metrics

| Rule | Rows | Window | Ann return | Ann volatility | Max drawdown |
| --- | --- | --- | --- | --- | --- |
| equal_risk | 2017 | 2018-03-27 .. 2026-07-20 | 11.6432% | 14.2037% | -23.0458% |
| equal_weight | 2017 | 2018-03-27 .. 2026-07-20 | 11.6949% | 15.7109% | -23.9937% |

## Latest Equal-Risk Weights

| Root | Sector | Weight |
| --- | --- | --- |
| AL.SHF | metals | 13.1006% |
| CU.SHF | metals | 11.3932% |
| I.DCE | ferrous | 13.8766% |
| M.DCE | agri | 17.8394% |
| P.DCE | agri | 12.5167% |
| RB.SHF | ferrous | 18.8042% |
| SC.INE | energy | 4.6399% |
| TA.ZCE | energy | 7.8293% |

## Latest Equal-Risk Contribution

| Root | Sector | Weight | Vol contribution | Risk contribution |
| --- | --- | --- | --- | --- |
| AL.SHF | metals | 13.1006% | 12.4562% | 13.1630% |
| CU.SHF | metals | 11.3932% | 12.4278% | 9.8925% |
| I.DCE | ferrous | 13.8766% | 12.4520% | 12.8882% |
| M.DCE | agri | 17.8394% | 12.6177% | 11.3509% |
| P.DCE | agri | 12.5167% | 12.4941% | 13.8377% |
| RB.SHF | ferrous | 18.8042% | 12.4547% | 12.6149% |
| SC.INE | energy | 4.6399% | 12.5764% | 12.6007% |
| TA.ZCE | energy | 7.8293% | 12.5211% | 13.6520% |

## Sector Risk Contribution

| Sector | Risk contribution |
| --- | --- |
| agri | 25.1886% |
| energy | 26.2528% |
| ferrous | 25.5031% |
| metals | 23.0555% |

## Required Pair Correlations

| Pair | Correlation |
| --- | --- |
| AL.SHF/CU.SHF | 0.6018 |
| RB.SHF/I.DCE | 0.7049 |
| SC.INE/TA.ZCE | 0.6317 |

## Sensitivity Checks

### Leave One Out

| Scenario | Rows | Ann return | Ann volatility | Max drawdown |
| --- | --- | --- | --- | --- |
| exclude AL.SHF | 2017 | 12.0250% | 16.6490% | -24.9660% |
| exclude CU.SHF | 2017 | 11.7069% | 16.5905% | -23.7000% |
| exclude RB.SHF | 2017 | 12.4398% | 16.1473% | -25.3988% |
| exclude I.DCE | 2017 | 8.9932% | 15.1293% | -29.1495% |
| exclude M.DCE | 2017 | 11.9425% | 17.1136% | -26.2282% |
| exclude P.DCE | 2017 | 10.7678% | 15.8909% | -21.9159% |
| exclude SC.INE | 2017 | 12.4409% | 14.7626% | -25.3070% |
| exclude TA.ZCE | 2017 | 12.9013% | 15.4699% | -23.0216% |

### Leave Sector Out

| Scenario | Rows | Ann return | Ann volatility | Max drawdown |
| --- | --- | --- | --- | --- |
| exclude agri | 2017 | 10.8788% | 17.6364% | -25.3961% |
| exclude energy | 2017 | 13.9022% | 14.7970% | -24.4153% |
| exclude ferrous | 2017 | 9.3152% | 16.0187% | -35.2870% |
| exclude metals | 2017 | 12.0513% | 17.9732% | -25.3499% |

### AU Control

| Scenario | Rows | Ann return | Ann volatility | Max drawdown |
| --- | --- | --- | --- | --- |
| exclude AU control | 2017 | 11.6949% | 15.7109% | -23.9937% |
| include AU.SHF | 2017 | 12.1386% | 14.2801% | -22.1961% |

## Stage 4 Decision

结论：`stage4_rule_frozen`。等权与等风险商品篮子定义、参数和缺失数据规则已冻结，可进入 Stage 5 的 ETF 基线增量回测。

限制：Stage 4 只证明篮子构造可复算；商品 sleeve 是否保留仍取决于 Stage 5/6 的基线增量回测、成本和稳健性评估。
