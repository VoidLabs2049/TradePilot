const API_BASE = "/api";

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
}

// Market
export const getStocks = () => fetchJson<{code: string; name: string}[]>("/market/stocks");
export const getIndices = () => fetchJson<{code: string; name: string}[]>("/market/indices");
export const getIndexDaily = (code: string) => fetchJson<any[]>(`/market/index_daily?index_code=${code}`);
export const getStockDaily = (code: string) => fetchJson<any[]>(`/market/stock_daily?stock_code=${code}`);
export const getEtfFlow = (code: string) => fetchJson<any[]>(`/market/etf_flow?etf_code=${code}`);
export const getNorthbound = () => fetchJson<any[]>("/market/northbound");
export const getMargin = () => fetchJson<any[]>("/market/margin");
export const getSectors = () => fetchJson<any[]>("/market/sectors");
export const getValuationData = (code: string) => fetchJson<any[]>(`/market/valuation?stock_code=${code}`);

// Analysis
export const getTechnical = (code: string) => fetchJson<any>(`/analysis/technical?stock_code=${code}`);
export const getValuation = (code: string) => fetchJson<any>(`/analysis/valuation?stock_code=${code}`);
export const getSectorRotation = () => fetchJson<any>("/analysis/sector_rotation");

// Signal
export const getSignals = (code: string) => fetchJson<any>(`/signal/list?stock_code=${code}`);
export const getScore = (code: string) => fetchJson<any>(`/signal/score?stock_code=${code}`);
export const getMarketSentiment = () => fetchJson<any>("/signal/market_sentiment");

// Portfolio
export const getPositions = () => fetchJson<any[]>("/portfolio/positions");
export const addPosition = (data: any) => fetch(`${API_BASE}/portfolio/positions`, { method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
export const getTrades = () => fetchJson<any[]>("/portfolio/trades");

// Trade Plan
export const evaluateStock = (code: string) => fetchJson<any>(`/trade_plan/evaluate/${code}`);
export const getPlans = (status?: string) => fetchJson<any[]>(`/trade_plan/list${status ? `?status=${status}` : ""}`);
export const createPlan = (data: any) => fetch(`${API_BASE}/trade_plan/create`, { method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
export const updatePlanStatus = (id: number, data: any) => fetch(`${API_BASE}/trade_plan/${id}/status`, { method: "PUT", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
export const monitorPlan = (id: number) => fetchJson<any>(`/trade_plan/${id}/monitor`);
export const deletePlan = (id: number) => fetch(`${API_BASE}/trade_plan/${id}`, { method: "DELETE" }).then(r => r.json());

// Briefing
export const runDailyScan = (scanDate?: string) => fetchJson<any>(`/briefing/scan/run${scanDate ? `?scan_date=${encodeURIComponent(scanDate)}` : ""}`);
export const getLatestScan = () => fetchJson<{scan_date: string | null; advice: any[]}>("/briefing/scan/latest");
export const getAlerts = (unreadOnly = false) => fetchJson<any[]>(`/briefing/alerts${unreadOnly ? "?unread_only=true" : ""}`);
export const markAlertRead = (id: number) => fetch(`${API_BASE}/briefing/alerts/${id}/read`, { method: "POST" }).then(r => r.json());

// Scheduler
export const getSchedulerStatus = () => fetchJson<any>("/scheduler/status");
export const getSchedulerHistory = (limit = 10) => fetchJson<any[]>(`/scheduler/history?limit=${limit}`);

// Summary
export const getDailySummary = (params?: {
  industry_top_n?: number;
  industry_bottom_n?: number;
  concept_top_n?: number;
  concept_bottom_n?: number;
}) => {
  const qs = params ? "?" + new URLSearchParams(
    Object.entries(params)
      .filter(([, v]) => v !== undefined)
      .map(([k, v]) => [k, String(v)])
  ).toString() : "";
  return fetchJson<any>(`/summary/daily${qs}`);
};
export const get5mBrief = (sectors?: string, stocks?: string) => {
  const params = new URLSearchParams();
  if (sectors) params.set("sectors", sectors);
  if (stocks) params.set("stocks", stocks);
  const qs = params.toString();
  return fetchJson<any>(`/summary/5m${qs ? "?" + qs : ""}`);
};
export const getTradingStatus = () => fetchJson<any>("/summary/trading-status");
export const getWatchlist = () => fetchJson<any>("/summary/watchlist");
export const updateWatchlist = (data: any) => fetch(`${API_BASE}/summary/watchlist`, { method: "PUT", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
