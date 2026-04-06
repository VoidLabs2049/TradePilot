const API_BASE = "/api";

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  return res.json();
}

// Portfolio
export const getPositions = () => fetchJson<any[]>("/portfolio/positions");
export const addPosition = (data: any) => fetch(`${API_BASE}/portfolio/positions`, { method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
export const getTrades = () => fetchJson<any[]>("/portfolio/trades");

// Briefing
export const getAlerts = (unreadOnly = false) => fetchJson<any[]>(`/briefing/alerts${unreadOnly ? "?unread_only=true" : ""}`);
export const markAlertRead = (id: number) => fetch(`${API_BASE}/briefing/alerts/${id}/read`, { method: "POST" }).then(r => r.json());

// Scheduler
export const getSchedulerStatus = () => fetchJson<any>("/scheduler/status");
export const getSchedulerHistory = (limit = 10) => fetchJson<any[]>(`/scheduler/history?limit=${limit}`);

// Workflow
export const getWorkflowStatus = () => fetchJson<any>("/workflow/status");
export const getWorkflowHistory = (limit = 10) => fetchJson<any[]>(`/workflow/history?limit=${limit}`);
export const getLatestWorkflow = (phase: "pre_market" | "post_market") =>
  fetchJson<any | null>(`/workflow/latest?phase=${phase}`);
export const runPreMarketWorkflow = (workflowDate?: string) =>
  fetch(`${API_BASE}/workflow/pre/run${workflowDate ? `?workflow_date=${encodeURIComponent(workflowDate)}` : ""}`, { method: "POST" }).then(r => r.json());
export const runPostMarketWorkflow = (workflowDate?: string) =>
  fetch(`${API_BASE}/workflow/post/run${workflowDate ? `?workflow_date=${encodeURIComponent(workflowDate)}` : ""}`, { method: "POST" }).then(r => r.json());

// Summary
export const getTradingStatus = () => fetchJson<any>("/summary/trading-status");
export const getWatchlist = () => fetchJson<any>("/summary/watchlist");
export const updateWatchlist = (data: any) => fetch(`${API_BASE}/summary/watchlist`, { method: "PUT", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
