# TradePilot 商品期货阶段 0 接入验收与数据冻结报告

Generated at: `2026-07-22T08:34:54.167813+00:00`
Code version: `5ed49bb32c94631e2441077b8b7e4061739528d3`
Snapshot id: `b4d737274d432780`
Lakehouse root: `/home/nixos/workspace/TradePilot/data/lakehouse`
DB path: `/home/nixos/workspace/TradePilot/data/tradepilot.duckdb`

## Scope

本报告覆盖接入验收、字段/单位清单、交易日历交叉校验、主力映射可追溯性、连续合约前置缺口与数据冻结标识。

## Stage 0 Decision

结论：`pass_with_caveats`。当前快照满足阶段 0 进入阶段 1 的最小门槛：交易日历按业务键冻结后无重复，主力映射全部能关联到实际单合约行情，映射主力行的 `close/settle/volume/oi` 无缺失。

限制：非主力/远月单合约仍存在早期 OHLC、`settle`、`volume`、`oi` 缺口，以及少量到期附近零 OHLC 但有结算价的记录；这些记录不得在阶段 1 之后静默用于收益、换月或流动性判断，必须在单合约审计时逐条复核或排除。

## Inputs

- root codes: `AU.SHF, AL.SHF, CU.SHF, RB.SHF, I.DCE, M.DCE, P.DCE, SC.INE, TA.ZCE`
- trading calendar audit rows after business-key freeze: `22621`
- normalized trading calendar rows: `0`
- reference.trading_calendar: `149` parquet files
- reference.futures_instruments: `376` parquet files
- market.futures_mapping: `268` parquet files
- market.futures_contract_daily: `278` parquet files

## Field And Unit Manifest

| Dataset | Field | Stage 0 unit / meaning |
| --- | --- | --- |
| reference.futures_instruments | multiplier | contract size multiplier; programmatic sizing input |
| reference.futures_instruments | per_unit | secondary sizing field from Tushare; kept for audit |
| reference.futures_instruments | trade_unit | physical trade unit label from source |
| reference.futures_instruments | quote_unit | quotation unit label from source |
| market.futures_contract_daily | open/high/low/close/settle/pre_close/pre_settle/change1/change2 | raw quoted price fields in contract quotation units |
| market.futures_contract_daily | volume | daily traded volume / hands as delivered by source |
| market.futures_contract_daily | oi | daily open interest as delivered by source |
| market.futures_contract_daily | oi_chg | open-interest delta as delivered by source |
| market.futures_mapping | active_contract | point-in-time dominant concrete contract code |
| reference.trading_calendar | trade_date | exchange trading date, not natural calendar date |

## Validation Summary

| Dataset | Check | Status | Count | Sample keys |
| --- | --- | --- | --- | --- |
| reference.trading_calendar | calendar.date_continuity | warning | 1 | - |
| reference.trading_calendar | calendar.duplicate_key | pass | 1 | - |
| reference.trading_calendar | calendar.exchange_supported | pass | 1 | - |
| reference.trading_calendar | calendar.is_open_boolean | pass | 1 | - |
| reference.trading_calendar | calendar.open_day_pretrade_sequence | pass | 1 | - |
| reference.trading_calendar | calendar.pretrade_before_trade_date | pass | 1 | - |
| reference.trading_calendar | calendar.trade_date_required | pass | 1 | - |
| reference.futures_instruments | futures_instruments.contract_code_required | pass | 1 | - |
| reference.futures_instruments | futures_instruments.duplicate_contract_code | pass | 1 | - |
| reference.futures_instruments | futures_instruments.list_delist_order | pass | 1 | - |
| reference.futures_instruments | futures_instruments.multiplier_available | pass | 1 | - |
| reference.futures_instruments | futures_instruments.multiplier_positive | pass | 1 | - |
| market.futures_mapping | futures_mapping.active_contract_required | pass | 1 | - |
| market.futures_mapping | futures_mapping.duplicate_business_key | pass | 1 | - |
| market.futures_mapping | futures_mapping.exchange_suffix_match | pass | 1 | - |
| market.futures_mapping | futures_mapping.root_code_required | pass | 1 | - |
| market.futures_mapping | futures_mapping.trade_date_required | pass | 1 | - |
| market.futures_contract_daily | futures_daily.contract_code_required | pass | 1 | - |
| market.futures_contract_daily | futures_daily.duplicate_business_key | pass | 1 | - |
| market.futures_contract_daily | futures_daily.ohlc_non_negative | pass | 1 | - |
| market.futures_contract_daily | futures_daily.oi_non_negative | pass | 1 | - |
| market.futures_contract_daily | futures_daily.settle_availability | warning | 1 | - |
| market.futures_contract_daily | futures_daily.trade_date_open | fail | 51 | AL0603.SHF|2005-03-16, AL0603.SHF|2005-03-17 |
| market.futures_contract_daily | futures_daily.trade_date_required | pass | 1 | - |
| market.futures_contract_daily | futures_daily.volume_non_negative | pass | 1 | - |

## Root Coverage

| Root | Mapping rows | Mapping window | Distinct active contracts | Matched daily rows | Unmatched mapping rows | Coverage | Daily rows | Missing core fields | OHLC order violations | Mapped missing prices | Mapped missing core |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| AU.SHF | 4501 | 2008-01-09 .. 2026-07-20 | 55 | 4501 | 0 | 100.0% | 13928 | 4 | 0 | 0 | 0 |
| AL.SHF | 5000 | 2005-12-20 .. 2026-07-20 | 245 | 5000 | 0 | 100.0% | 59480 | 542 | 0 | 1 | 0 |
| CU.SHF | 5000 | 2005-12-20 .. 2026-07-20 | 248 | 5000 | 0 | 100.0% | 60212 | 403 | 0 | 3 | 0 |
| RB.SHF | 4206 | 2009-03-27 .. 2026-07-20 | 56 | 4206 | 0 | 100.0% | 13089 | 0 | 0 | 0 | 0 |
| I.DCE | 3102 | 2013-10-18 .. 2026-07-20 | 38 | 3102 | 0 | 100.0% | 9080 | 0 | 2 | 0 | 0 |
| M.DCE | 5000 | 2005-12-20 .. 2026-07-20 | 63 | 5000 | 0 | 100.0% | 15076 | 0 | 2 | 0 | 0 |
| P.DCE | 4551 | 2007-10-29 .. 2026-07-20 | 57 | 4551 | 0 | 100.0% | 13682 | 0 | 1 | 0 | 0 |
| SC.INE | 2018 | 2018-03-26 .. 2026-07-20 | 94 | 2018 | 0 | 100.0% | 35220 | 0 | 0 | 0 | 0 |
| TA.ZCE | 4758 | 2006-12-18 .. 2026-07-20 | 70 | 4758 | 0 | 100.0% | 16396 | 674 | 0 | 0 | 0 |

## Snapshot Freeze

| Dataset | Latest fetched date | Latest successful run id |
| --- | --- | --- |
| market.futures_contract_daily | 2026-07-20 00:00:00 | 447 |
| market.futures_mapping | 2026-07-20 00:00:00 | 446 |
| reference.futures_instruments | NaT | 459 |
| reference.trading_calendar | 2026-07-22 00:00:00 | 460 |

Recent ingestion runs:
- reference.trading_calendar: run_id=460, status=success, finished_at=2026-07-22 06:10:36.579516
- reference.futures_instruments: run_id=459, status=success, finished_at=2026-07-21 08:31:49.937438
- reference.futures_instruments: run_id=458, status=success, finished_at=2026-07-21 08:29:27.812646
- reference.trading_calendar: run_id=457, status=success, finished_at=2026-07-21 08:29:24.883055
- reference.futures_instruments: run_id=456, status=failed, finished_at=2026-07-21 08:27:02.375674
- reference.trading_calendar: run_id=455, status=failed, finished_at=2026-07-21 08:27:01.022835
- reference.trading_calendar: run_id=448, status=success, finished_at=2026-07-21 04:06:38.622649
- market.futures_contract_daily: run_id=447, status=success, finished_at=2026-07-20 09:18:19.078292
- market.futures_mapping: run_id=446, status=success, finished_at=2026-07-20 09:17:46.495335
- market.futures_contract_daily: run_id=445, status=success, finished_at=2026-07-20 09:17:44.550326
- market.futures_mapping: run_id=444, status=success, finished_at=2026-07-20 09:17:16.865694
- market.futures_contract_daily: run_id=443, status=success, finished_at=2026-07-20 09:15:38.727966

## Missing Field Counts

| Field | Missing rows |
| --- | --- |
| open | 21049 |
| high | 21049 |
| low | 21049 |
| close | 674 |
| settle | 200 |
| volume | 735 |
| oi | 14 |

Mapped active-contract missing counts:
| Field | Missing rows |
| --- | --- |
| open | 4 |
| high | 4 |
| low | 4 |
| close | 0 |
| settle | 0 |
| volume | 0 |
| oi | 0 |

## Accepted Anomaly Records

| Category | Root | Contract | Trade date | Fields | Disposition |
| --- | --- | --- | --- | --- | --- |
| single_contract_missing_core | CU.SHF | CU0602.SHF | 2005-02-17 | volume | accepted for stage 0; recheck before stage 1 roll audit if selected |
| single_contract_missing_core | CU.SHF | CU0602.SHF | 2005-02-21 | volume | accepted for stage 0; recheck before stage 1 roll audit if selected |
| single_contract_missing_core | CU.SHF | CU0602.SHF | 2005-02-23 | volume | accepted for stage 0; recheck before stage 1 roll audit if selected |
| single_contract_missing_core | CU.SHF | CU0602.SHF | 2005-02-24 | volume | accepted for stage 0; recheck before stage 1 roll audit if selected |
| single_contract_missing_core | CU.SHF | CU0602.SHF | 2005-02-25 | volume | accepted for stage 0; recheck before stage 1 roll audit if selected |
| single_contract_missing_core | CU.SHF | CU0602.SHF | 2005-02-28 | volume | accepted for stage 0; recheck before stage 1 roll audit if selected |
| single_contract_ohlc_order | I.DCE | I2605.DCE | 2026-05-11 | open,high,low,close | accepted for stage 0; exclude from return construction unless manually justified |
| single_contract_ohlc_order | I.DCE | I2605.DCE | 2026-05-19 | open,high,low,close | accepted for stage 0; exclude from return construction unless manually justified |
| single_contract_ohlc_order | M.DCE | M2605.DCE | 2026-05-15 | open,high,low,close | accepted for stage 0; exclude from return construction unless manually justified |
| single_contract_ohlc_order | M.DCE | M2605.DCE | 2026-05-18 | open,high,low,close | accepted for stage 0; exclude from return construction unless manually justified |
| single_contract_ohlc_order | P.DCE | P2605.DCE | 2026-05-14 | open,high,low,close | accepted for stage 0; exclude from return construction unless manually justified |
| mapped_active_missing_ohlc | CU.SHF | CU0812.SHF | 2008-10-09 | open,high,low | non-blocking for settle/close returns; audit before using intraday OHLC logic |
| mapped_active_missing_ohlc | CU.SHF | CU0901.SHF | 2008-10-24 | open,high,low | non-blocking for settle/close returns; audit before using intraday OHLC logic |
| mapped_active_missing_ohlc | AL.SHF | AL0902.SHF | 2008-12-08 | open,high,low | non-blocking for settle/close returns; audit before using intraday OHLC logic |
| mapped_active_missing_ohlc | CU.SHF | CU0903.SHF | 2009-01-07 | open,high,low | non-blocking for settle/close returns; audit before using intraday OHLC logic |

## Notes

- normalized trading calendar is absent; stage 0 uses raw trading-calendar batches deduplicated by exchange/trade_date for the open-day audit
- core daily fields still have missing rows: close=674, settle=200, volume=735, oi=14; current mapped active-contract rows have no close/settle/volume/oi gaps
