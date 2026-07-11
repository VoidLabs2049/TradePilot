from __future__ import annotations

import json
import time
from datetime import date, datetime

from pydantic import BaseModel, Field

from tradepilot.analysis.fund_flow import (
    analyze_etf_flow,
    analyze_margin,
    analyze_northbound,
    compute_market_sentiment,
)
from tradepilot.analysis.risk import evaluate_stop_loss, evaluate_take_profit
from tradepilot.analysis.sector_rotation import analyze_sectors
from tradepilot.analysis.signal import compute_composite_score
from tradepilot.analysis.technical import analyze_stock
from tradepilot.analysis.valuation import analyze_valuation
from tradepilot.config import DATA_ROOT
from tradepilot.data import get_provider
from tradepilot.db import get_conn

CORE_INSTRUMENTS: list[dict[str, str]] = [
    {"code": "000001", "name": "上证指数", "kind": "index"},
    {"code": "399001", "name": "深证成指", "kind": "index"},
    {"code": "399006", "name": "创业板指", "kind": "index"},
    {"code": "000688", "name": "科创50", "kind": "index"},
    {"code": "510300", "name": "沪深300ETF", "kind": "stock"},
    {"code": "510500", "name": "中证500ETF", "kind": "stock"},
]


class StockAdvice(BaseModel):
    stock_code: str
    stock_name: str
    action: str
    urgency: str
    score: float
    reasons: list[str] = Field(default_factory=list)
    risk_alerts: list[str] = Field(default_factory=list)
    suggested_price: float | None = None
    suggested_stop_loss: float | None = None
    suggested_take_profit: list[float] | None = None


class ScanAlert(BaseModel):
    alert_type: str
    urgency: str
    title: str
    message: str
    stock_code: str | None = None
    sector: str | None = None


class DailyScanResult(BaseModel):
    scan_date: str
    market_overview: dict
    watchlist_advice: list[StockAdvice]
    position_advice: list[StockAdvice]
    core_instrument_advice: list[StockAdvice]
    rotation_suggestions: list[dict]
    alerts: list[ScanAlert]


class DailyScanner:
    def __init__(self) -> None:
        self._provider = get_provider()

    def run(self, scan_date: str | None = None) -> DailyScanResult:
        resolved_scan_date = scan_date or datetime.now().strftime("%Y-%m-%d")
        sentiment = self._compute_market_sentiment()
        sector_result = analyze_sectors(
            self._provider.get_sector_data("2024-01-01", "2025-12-31")
        )
        watchlist = self._load_watchlist()
        positions = self._load_positions()
        position_codes = {position["stock_code"] for position in positions}
        core_codes = {item["code"] for item in CORE_INSTRUMENTS}

        watchlist_advice = [
            self._scan_watch_stock(stock, sentiment, sector_result)
            for stock in watchlist
            if stock["code"] not in position_codes and stock["code"] not in core_codes
        ]
        position_advice = [
            self._scan_position(position, sentiment, sector_result)
            for position in positions
        ]
        core_instrument_advice = [
            self._scan_core_instrument(item, sentiment, sector_result)
            for item in CORE_INSTRUMENTS
        ]
        rotation_suggestions = sector_result.get("switch_suggestions", [])[:5]
        alerts = self._build_alerts(
            watchlist_advice=watchlist_advice,
            position_advice=position_advice,
            core_instrument_advice=core_instrument_advice,
            rotation_suggestions=rotation_suggestions,
        )

        result = DailyScanResult(
            scan_date=resolved_scan_date,
            market_overview={
                "sentiment": sentiment,
                "high_positions": sector_result.get("high_positions", []),
                "low_opportunities": sector_result.get("low_opportunities", []),
                "market_stats": self._get_latest_market_stats(),
            },
            watchlist_advice=watchlist_advice,
            position_advice=position_advice,
            core_instrument_advice=core_instrument_advice,
            rotation_suggestions=rotation_suggestions,
            alerts=alerts,
        )

        self._persist_scan_results(result)
        self._persist_alerts(resolved_scan_date, alerts)
        return result

    def get_latest_scan(self) -> dict | None:
        conn = get_conn()
        latest = conn.execute(
            "SELECT scan_date FROM daily_scan_results ORDER BY scan_date DESC LIMIT 1"
        ).fetchone()
        if latest is None:
            return None

        scan_date = str(latest[0])
        rows = conn.execute(
            """
            SELECT stock_code, stock_name, action, urgency, score, reasons, risk_alerts,
                   suggested_price, suggested_stop_loss, suggested_take_profit
            FROM daily_scan_results
            WHERE scan_date = ?
            ORDER BY urgency DESC, score DESC, stock_code ASC
            """,
            [scan_date],
        ).fetchdf()
        records = rows.to_dict(orient="records")
        for record in records:
            record["reasons"] = self._loads_json_list(record.get("reasons"))
            record["risk_alerts"] = self._loads_json_list(record.get("risk_alerts"))
            record["suggested_take_profit"] = self._loads_json_list(
                record.get("suggested_take_profit")
            )
        return {"scan_date": scan_date, "advice": records}

    def list_alerts(self, unread_only: bool = False) -> list[dict]:
        conn = get_conn()
        query = (
            "SELECT * FROM alerts WHERE read_at IS NULL ORDER BY created_at DESC, id DESC"
            if unread_only
            else "SELECT * FROM alerts ORDER BY created_at DESC, id DESC"
        )
        rows = conn.execute(query).fetchdf()
        return rows.to_dict(orient="records")

    def create_system_alert(
        self, title: str, message: str, urgency: str = "high"
    ) -> None:
        conn = get_conn()
        conn.execute(
            """
            INSERT INTO alerts (id, alert_type, urgency, title, message)
            VALUES (?, 'system', ?, ?, ?)
            """,
            [
                self._next_id(1, namespace="system_alerts"),
                urgency,
                title,
                message,
            ],
        )

    def mark_alert_read(self, alert_id: int) -> None:
        conn = get_conn()
        conn.execute(
            "UPDATE alerts SET read_at = CURRENT_TIMESTAMP WHERE id = ?",
            [alert_id],
        )

    def _load_watchlist(self) -> list[dict[str, str]]:
        path = DATA_ROOT / "watchlist.json"
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []

        watch_stocks = payload.get("watch_stocks", [])
        results: list[dict[str, str]] = []
        for item in watch_stocks:
            code = str(item.get("code", "")).strip()
            if not code:
                continue
            results.append(
                {
                    "code": code,
                    "name": str(item.get("name", "")).strip(),
                }
            )
        return results

    def _load_positions(self) -> list[dict]:
        conn = get_conn()
        rows = conn.execute(
            "SELECT * FROM portfolio WHERE status = 'open' ORDER BY buy_date DESC"
        ).fetchdf()
        return rows.to_dict(orient="records")

    def _scan_watch_stock(
        self,
        stock: dict[str, str],
        sentiment: dict,
        sector_result: dict,
    ) -> StockAdvice:
        evaluation = self._evaluate_stock(
            stock["code"], stock.get("name", ""), sentiment, sector_result
        )
        score = evaluation["score"]
        action = "观望"
        urgency = "无需操作"
        if score >= 80:
            action = "建仓"
            urgency = "立即"
        elif score >= 65:
            action = "关注"
            urgency = "关注"

        return StockAdvice(
            stock_code=stock["code"],
            stock_name=evaluation["stock_name"],
            action=action,
            urgency=urgency,
            score=score,
            reasons=evaluation["reasons"],
            risk_alerts=evaluation["risk_alerts"],
            suggested_price=evaluation["suggested_price"],
            suggested_stop_loss=evaluation["suggested_stop_loss"],
            suggested_take_profit=evaluation["suggested_take_profit"],
        )

    def _scan_position(
        self,
        position: dict,
        market_sentiment: dict,
        sector_result: dict,
    ) -> StockAdvice:
        stock_code = str(position["stock_code"])
        stock_name = str(position.get("stock_name", "")).strip() or stock_code
        kline = self._provider.get_stock_daily(stock_code, "2024-01-01", "2025-12-31")
        current_price = float(kline["close"].iloc[-1])
        sector_position = self._resolve_sector_position(stock_code, sector_result)

        stop_loss = evaluate_stop_loss(
            entry_price=float(position["buy_price"]),
            current_price=current_price,
            stop_loss_pct=-10,
            kline_df=kline,
        )
        take_profit = evaluate_take_profit(
            entry_price=float(position["buy_price"]),
            current_price=current_price,
            take_profit_pct=30,
            kline_df=kline,
            market_sentiment=market_sentiment,
            sector_position=sector_position,
        )
        evaluation = self._evaluate_stock(
            stock_code, stock_name, market_sentiment, sector_result
        )

        action = "持有"
        urgency = "无需操作"
        risk_alerts = [item["name"] for item in stop_loss["conditions"]]
        if stop_loss["triggered"]:
            action = "清仓"
            urgency = "立即"
        elif take_profit["triggered"]:
            action = "减仓"
            urgency = "立即"
            risk_alerts.extend(item["name"] for item in take_profit["conditions"])
        elif evaluation["score"] < 45:
            action = "减仓"
            urgency = "关注"

        return StockAdvice(
            stock_code=stock_code,
            stock_name=stock_name,
            action=action,
            urgency=urgency,
            score=evaluation["score"],
            reasons=evaluation["reasons"],
            risk_alerts=risk_alerts,
            suggested_price=round(current_price, 2),
            suggested_stop_loss=round(float(position["buy_price"]) * 0.9, 2),
            suggested_take_profit=[
                round(float(position["buy_price"]) * 1.15, 2),
                round(float(position["buy_price"]) * 1.30, 2),
            ],
        )

    def _scan_core_instrument(
        self,
        instrument: dict[str, str],
        market_sentiment: dict,
        sector_result: dict,
    ) -> StockAdvice:
        if instrument["kind"] == "index":
            current_price = self._provider.get_index_daily(
                instrument["code"], "2024-01-01", "2025-12-31"
            )["close"].iloc[-1]
            score = market_sentiment.get("score", 50)
            action = "关注" if score >= 60 else "观望"
            urgency = "关注" if score >= 60 else "无需操作"
            reasons = [f"市场情绪{market_sentiment.get('label', '中性')}({score:.0f})"]
            return StockAdvice(
                stock_code=instrument["code"],
                stock_name=instrument["name"],
                action=action,
                urgency=urgency,
                score=float(score),
                reasons=reasons,
                risk_alerts=[],
                suggested_price=round(float(current_price), 2),
                suggested_stop_loss=None,
                suggested_take_profit=None,
            )
        return self._scan_watch_stock(
            {"code": instrument["code"], "name": instrument["name"]},
            market_sentiment,
            sector_result,
        )

    def _evaluate_stock(
        self,
        stock_code: str,
        stock_name: str,
        sentiment: dict,
        sector_result: dict,
    ) -> dict:
        kline = self._provider.get_stock_daily(stock_code, "2024-01-01", "2025-12-31")
        valuation_df = self._provider.get_stock_valuation(
            stock_code, "2024-01-01", "2025-12-31"
        )
        tech = analyze_stock(kline)
        valuation = analyze_valuation(valuation_df, kline)
        sector_position = self._resolve_sector_position(stock_code, sector_result)
        technical_signals = (
            tech["cross_signals"] + tech["divergence_signals"] + tech["volume_signals"]
        )[-12:]
        composite = compute_composite_score(
            technical_signals,
            valuation,
            sentiment,
            sector_position,
        )
        current_price = float(kline["close"].iloc[-1])
        support_price = (
            float(kline["low"].tail(20).min()) if len(kline) >= 20 else current_price
        )

        risk_alerts: list[str] = []
        if sector_position == "high":
            risk_alerts.append("所在板块处于高位")
        if valuation.get("pb_percentile") and valuation["pb_percentile"] >= 80:
            risk_alerts.append(f"PB高分位({valuation['pb_percentile']:.0f}%)")
        if valuation.get("pe_percentile") and valuation["pe_percentile"] >= 80:
            risk_alerts.append(f"PE高分位({valuation['pe_percentile']:.0f}%)")

        return {
            "stock_name": stock_name or stock_code,
            "score": composite["score"],
            "reasons": self._compact_reasons(composite["reasons"]),
            "risk_alerts": risk_alerts,
            "suggested_price": round(current_price, 2),
            "suggested_stop_loss": round(support_price * 0.97, 2),
            "suggested_take_profit": [
                round(current_price * 1.15, 2),
                round(current_price * 1.30, 2),
            ],
        }

    def _compute_market_sentiment(self) -> dict:
        northbound_df = self._provider.get_northbound_flow("2024-01-01", "2025-12-31")
        margin_df = self._provider.get_margin_data("2024-01-01", "2025-12-31")
        etf_result: dict = {}
        for code in ["510050", "510300", "510500", "512100"]:
            etf_df = self._provider.get_etf_flow(code, "2024-01-01", "2025-12-31")
            etf_result.update(analyze_etf_flow(etf_df))
        northbound = analyze_northbound(northbound_df)
        margin = analyze_margin(margin_df)
        return compute_market_sentiment(etf_result, northbound, margin)

    def _resolve_sector_position(
        self, stock_code: str, sector_result: dict
    ) -> str | None:
        mapping = self._provider.get_stock_sector(stock_code)
        if mapping.empty:
            return None
        sectors = {str(row["sector"]) for _, row in mapping.iterrows()}
        high_sectors = {
            item["sector"] for item in sector_result.get("high_positions", [])
        }
        low_sectors = {
            item["sector"] for item in sector_result.get("low_opportunities", [])
        }
        if sectors & high_sectors:
            return "high"
        if sectors & low_sectors:
            return "low"
        return None

    def _build_alerts(
        self,
        watchlist_advice: list[StockAdvice],
        position_advice: list[StockAdvice],
        core_instrument_advice: list[StockAdvice],
        rotation_suggestions: list[dict],
    ) -> list[ScanAlert]:
        alerts: list[ScanAlert] = []
        for advice in position_advice:
            if advice.action == "清仓":
                alerts.append(
                    ScanAlert(
                        alert_type="stop_loss",
                        urgency="high",
                        stock_code=advice.stock_code,
                        title=f"{advice.stock_name} 触发风控信号",
                        message="；".join(advice.risk_alerts[:3])
                        or "触发止损/风控条件",
                    )
                )
            elif advice.action == "减仓":
                alerts.append(
                    ScanAlert(
                        alert_type="take_profit",
                        urgency="high",
                        stock_code=advice.stock_code,
                        title=f"{advice.stock_name} 建议减仓",
                        message="；".join((advice.risk_alerts or advice.reasons)[:3]),
                    )
                )

        for advice in watchlist_advice:
            if advice.action == "建仓":
                alerts.append(
                    ScanAlert(
                        alert_type="watchlist_opportunity",
                        urgency="medium",
                        stock_code=advice.stock_code,
                        title=f"{advice.stock_name} 出现建仓机会",
                        message="；".join(advice.reasons[:3]),
                    )
                )

        for advice in core_instrument_advice:
            if advice.urgency == "关注":
                alerts.append(
                    ScanAlert(
                        alert_type="core_signal",
                        urgency="low",
                        stock_code=advice.stock_code,
                        title=f"核心标的关注：{advice.stock_name}",
                        message="；".join(advice.reasons[:2]),
                    )
                )

        for suggestion in rotation_suggestions[:3]:
            alerts.append(
                ScanAlert(
                    alert_type="rotation",
                    urgency="low",
                    sector=suggestion.get("to_sector"),
                    title=f"高切低建议：{suggestion.get('from_sector')} → {suggestion.get('to_sector')}",
                    message=suggestion.get("reason", ""),
                )
            )
        return alerts

    def _persist_scan_results(self, result: DailyScanResult) -> None:
        conn = get_conn()
        conn.execute(
            "DELETE FROM daily_scan_results WHERE scan_date = ?", [result.scan_date]
        )
        rows = (
            result.watchlist_advice
            + result.position_advice
            + result.core_instrument_advice
        )
        for index, advice in enumerate(rows, 1):
            conn.execute(
                """
                INSERT INTO daily_scan_results (
                    id, scan_date, stock_code, stock_name, action, urgency, score,
                    reasons, risk_alerts, suggested_price, suggested_stop_loss,
                    suggested_take_profit
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    self._next_id(index),
                    result.scan_date,
                    advice.stock_code,
                    advice.stock_name,
                    advice.action,
                    advice.urgency,
                    advice.score,
                    json.dumps(advice.reasons, ensure_ascii=False),
                    json.dumps(advice.risk_alerts, ensure_ascii=False),
                    advice.suggested_price,
                    advice.suggested_stop_loss,
                    json.dumps(advice.suggested_take_profit or [], ensure_ascii=False),
                ],
            )

    def _persist_alerts(self, scan_date: str, alerts: list[ScanAlert]) -> None:
        conn = get_conn()
        conn.execute(
            """
            DELETE FROM alerts
            WHERE DATE(created_at) = ?
              AND (
                    alert_type IN ('stop_loss', 'take_profit', 'watchlist_opportunity', 'rotation')
                 OR alert_type = 'core_signal'
              )
            """,
            [scan_date],
        )
        for index, alert in enumerate(alerts, 1):
            conn.execute(
                """
                INSERT INTO alerts (id, alert_type, urgency, stock_code, sector, title, message)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    self._next_id(index, namespace="alerts"),
                    alert.alert_type,
                    alert.urgency,
                    alert.stock_code,
                    alert.sector,
                    alert.title,
                    alert.message,
                ],
            )

    def _next_id(self, offset: int, namespace: str = "scan") -> int:
        namespace_offsets = {
            "scan": 0,
            "alerts": 1_000_000,
            "system_alerts": 2_000_000,
        }
        namespace_offset = namespace_offsets.get(namespace, 3_000_000)
        return time.time_ns() + namespace_offset + offset

    def _loads_json_list(self, value: object) -> list:
        if value in (None, ""):
            return []
        if isinstance(value, list):
            return value
        try:
            decoded = json.loads(str(value))
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []

    def _compact_reasons(self, reasons: list[str]) -> list[str]:
        compacted: list[str] = []
        seen: set[str] = set()
        for reason in reasons:
            if reason in seen:
                continue
            seen.add(reason)
            compacted.append(reason)
            if len(compacted) >= 6:
                break
        return compacted

    def _get_latest_market_stats(self) -> list[dict]:
        conn = get_conn()
        rows = conn.execute(
            """
            SELECT trade_date, market_code, market_name, listed_count, total_mv, float_mv, amount, vol, pe, turnover_rate
            FROM market_daily_stats
            WHERE trade_date = (SELECT MAX(trade_date) FROM market_daily_stats)
            ORDER BY market_code ASC
            """
        ).fetchdf()
        return rows.to_dict(orient="records")


def normalize_scan_date(scan_date: str | None) -> str | None:
    if scan_date in (None, ""):
        return None
    return date.fromisoformat(scan_date).isoformat()
