const API_BASE = "/api";

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}`);
  }
  return res.json();
}

export type WorkflowPhase = "pre_market" | "post_market";
export type InsightState = "not_requested" | "pending" | "completed" | "failed" | "stale";
export type InsightSectionKey =
  | "market_view"
  | "theme_view"
  | "position_view"
  | "tomorrow_view"
  | "action_frame"
  | "risk_notes"
  | "execution_notes"
  | "custom";

export interface WorkflowStepResult {
  name: string;
  status: string;
  records_affected: number;
  error_message?: string | null;
}

export interface WorkflowRunSummary {
  title: string;
  overview: string;
  requested_date?: string | null;
  resolved_date?: string | null;
  date_resolution?: string;
  market_overview?: Record<string, any>;
  sector_positioning?: Record<string, any>;
  position_health?: Record<string, any>;
  next_day_prep?: Record<string, any>;
  yesterday_recap?: Record<string, any>;
  overnight_news?: Record<string, any>;
  today_watchlist?: Record<string, any>;
  action_frame?: Record<string, any>;
  watch_context?: {
    watch_sectors?: string[];
    watch_stocks?: Array<{ code: string; name?: string }>;
    open_positions?: Array<{ code: string; name?: string }>;
  };
  alerts?: any[];
  metadata?: Record<string, any>;
  steps?: WorkflowStepResult[];
  watchlist?: Record<string, any>;
  scan?: Record<string, any>;
  news?: Record<string, any>;
  scheduler?: Record<string, any>;
  carry_over?: Record<string, any>;
}

export interface WorkflowRun {
  id: number;
  workflow_date: string;
  phase: WorkflowPhase;
  triggered_by: string;
  status: string;
  started_at: string;
  finished_at?: string | null;
  summary: WorkflowRunSummary;
  error_message?: string | null;
}

export interface WorkflowRunResponse {
  run: WorkflowRun;
}

export interface WorkflowContextPayload {
  schema_version: string;
  producer: string;
  producer_version: string;
  generated_at: string;
  workflow_run_id: number;
  workflow_date: string;
  phase: WorkflowPhase;
  context: Record<string, any>;
  metadata: Record<string, any>;
}

// ETF all-weather sleeve metrics are adjustment-aware and measured at the
// monthly rebalance snapshot boundary. Return, volatility, and drawdown values
// are decimal ratios, e.g. 0.05 means 5%.
export interface EtfAwSleeveSnapshot {
  // Frozen v1 sleeve instrument code, such as 510300.SH or 511010.SH.
  sleeve_code: string;
  // Canonical role in the all-weather sleeve set: equity_large, bond, gold, etc.
  sleeve_role: string;
  // Raw close is kept for inspection; adjusted close is the canonical return base.
  close?: number | null;
  adj_factor?: number | null;
  adj_close?: number | null;
  // Trailing returns over the named windows, ending at the rebalance date.
  return_1m?: number | null;
  return_3m?: number | null;
  return_6m?: number | null;
  // Trailing risk measures over the named windows, ending at the rebalance date.
  volatility_3m?: number | null;
  max_drawdown_6m?: number | null;
  // complete, partial, stale, or missing. stale takes precedence when inputs
  // have not reached the rebalance date; missing means inputs covered the date
  // but the target row or core fields are unavailable.
  data_status: string;
  // Backend-provided diagnostics such as observation counts and stale source flags.
  quality_notes?: Record<string, any>;
  // Latest source trade date available for this sleeve when the snapshot was built.
  source_max_trade_date?: string | null;
}

// Latest ETF all-weather monthly rebalance context exposed to workflow insight.
export interface EtfAwSnapshotContext {
  schema_version: string;
  contract_version?: string;
  calendar_name: string;
  calendar_month: string;
  rebalance_date: string;
  effective_date?: string | null;
  data_status: string;
  sleeves: EtfAwSleeveSnapshot[];
}

export interface EtfAwRiskBudgetSleeve {
  sleeve_role: string;
  base_budget?: number | null;
  delta_budget?: number | null;
  tilted_budget?: number | null;
  budget_status?: string | null;
  quality_notes?: Record<string, any>;
}

export interface EtfAwRiskBudget {
  schema_version: string;
  contract_version: string;
  calendar_name?: string | null;
  rebalance_date?: string | null;
  strategy_name?: string | null;
  strategy_version?: string | null;
  market_regime_label?: string | null;
  budget_status?: string | null;
  budget_basis?: string | null;
  confidence_score?: number | null;
  effective_confidence_score?: number | null;
  base_budget_sum?: number | null;
  tilted_budget_sum?: number | null;
  budgets: EtfAwRiskBudgetSleeve[];
  quality_notes?: Record<string, any>;
  source_strategy_context_rebalance_date?: string | null;
  source_regime_rebalance_date?: string | null;
}

export interface EtfAwShadowDailyPoint {
  observation_date: string;
  total_asset: number;
  daily_return: number;
  cumulative_return: number;
  baseline_cumulative_return?: number | null;
  relative_cumulative_return?: number | null;
}

export interface EtfAwShadowReport {
  account_id: string;
  start_date: string;
  end_date: string;
  metrics: Record<string, number | null>;
  daily_series: EtfAwShadowDailyPoint[];
  weight_drift: Record<string, any>;
  fill_quality: Array<Record<string, any>>;
  integrity: {
    observation_count: number;
    missing_baseline_dates: string[];
    unattributable_fill_dates: string[];
    warnings: Record<string, number>;
  };
}

export interface EtfAwShadowReportResponse {
  state: "not_initialized" | "awaiting_observation" | "invalid" | "ready";
  accounts: string[];
  report: EtfAwShadowReport | null;
  blocking_reasons?: string[];
}

export interface EtfAwShadowUpdateResponse {
  state: "updated" | "invalid";
  account_id?: string;
  seed_date?: string;
  seed_created?: boolean;
  observations_written?: number;
  seed_artifact?: string;
  blocking_reasons?: string[];
  diagnostics?: Record<string, any>;
}

export interface EtfAwShadowStatus {
  account_id: string;
  latest_sleeve_daily_date?: string | null;
  latest_target_weight_date?: string | null;
  latest_shadow_observation_date?: string | null;
  missing_observation_dates: string[];
  latest_prices: Array<{
    sleeve_code: string;
    sleeve_role: string;
    close: number;
    trade_date: string;
  }>;
  is_stale: boolean;
  next_action: string;
}

export interface EtfAwLocalPerformance {
  source_dataset: string;
  start_date: string;
  end_date: string;
  observation_count: number;
  series: Array<{
    date: string;
    strategy: string;
    strategy_version: string;
    net_value: number;
    period_return: number;
    daily_return: number;
  }>;
  metrics: Array<{ strategy: string; metric: string; value: number }>;
}

export interface EtfAwTargetWeightRow {
  sleeve_code: string;
  sleeve_role: string;
  target_weight: number | null;
  target_weight_status: string;
  turnover_estimate?: number | null;
}

export interface EtfAwPlanOrderRow {
  sleeve_code: string;
  sleeve_role: string;
  target_weight: number | null;
  latest_price: number | null;
  order_side: string;
  order_quantity: number;
  estimated_notional: number | null;
  target_notional: number | null;
}

export interface EtfAwRobustnessScenario {
  cost_scenario: string;
  gross_total_return?: number | null;
  gross_sharpe_ratio?: number | null;
  gross_max_drawdown?: number | null;
  net_total_return?: number | null;
  net_annualized_return?: number | null;
  net_annualized_volatility?: number | null;
  net_sharpe_ratio?: number | null;
  net_max_drawdown?: number | null;
  average_turnover?: number | null;
  estimated_cost_fraction_sum?: number | null;
}

export interface EtfAwRobustnessComparison {
  cost_scenario: string;
  gross_total_return_diff?: number | null;
  net_total_return_diff?: number | null;
  net_sharpe_ratio_diff?: number | null;
  net_max_drawdown_diff?: number | null;
}

export interface EtfAwResearchSummary {
  target_weight: {
    rebalance_date: string | null;
    status_counts: Record<string, number>;
    rows: EtfAwTargetWeightRow[];
  };
  latest_plan: {
    plan_id: string;
    plan_date: string;
    plan_status: string;
    account_id: string;
    estimated_buy_notional: number | null;
    estimated_sell_notional: number | null;
    rows: EtfAwPlanOrderRow[];
  } | null;
  robustness: {
    verdict: "pass" | "fail" | "blocked";
    decision_rule: string;
    report_status: string;
    comparable_range: { start_date: string | null; end_date: string | null };
    coverage: Record<string, any>;
    strategies: Array<{
      label: string;
      strategy_name: string;
      strategy_version: string;
      scenarios: EtfAwRobustnessScenario[];
    }>;
    comparisons: EtfAwRobustnessComparison[];
    diagnostics: Record<string, any>;
  } | null;
  fixed_weight_backtest: {
    weight_rebalance_date: string | null;
    weight_basis: string;
    baseline: string;
    summary: {
      segment_count: number;
      profitable_segments: number;
      beat_equal_weight_segments: number;
      profitable_ratio: number | null;
      beat_equal_weight_ratio: number | null;
      average_total_return_diff?: number | null;
      worst_max_drawdown?: number | null;
    };
    segments: Array<{
      segment_name: string;
      segment_type: string;
      start_date: string;
      end_date: string;
      observation_count: number;
      strategy: {
        total_return: number | null;
        annualized_return: number | null;
        annualized_volatility: number | null;
        sharpe_ratio: number | null;
        max_drawdown: number | null;
      };
      equal_weight_baseline: {
        total_return: number | null;
        annualized_return: number | null;
        annualized_volatility: number | null;
        sharpe_ratio: number | null;
        max_drawdown: number | null;
      };
      comparison: {
        total_return_diff: number | null;
        annualized_return_diff: number | null;
        sharpe_ratio_diff: number | null;
        max_drawdown_diff: number | null;
      };
      profitable: boolean;
      beats_equal_weight: boolean;
    }>;
    optimization: {
      method: string;
      objective: string;
      best_candidate_name: string;
      candidates: Array<{
      candidate_name: string;
        shrinkage_to_equal_weight?: number;
        search_method?: string;
        weights: Record<string, number>;
        score: number;
        summary: {
          segment_count: number;
          profitable_segments: number;
          beat_equal_weight_segments: number;
          profitable_ratio: number | null;
          beat_equal_weight_ratio: number | null;
          average_total_return_diff: number | null;
          worst_max_drawdown: number | null;
        };
      }>;
    };
  } | null;
}

export interface InsightMetric {
  label: string;
  value: string | number | null;
}

export interface InsightListItem {
  title?: string;
  description?: string;
  status?: string;
  tags?: string[];
}

export interface WorkflowInsightSection {
  key: InsightSectionKey | string;
  title: string;
  summary?: string;
  bullets?: string[];
  tags?: string[];
  metrics?: InsightMetric[];
  items?: InsightListItem[];
}

export interface WorkflowInsightPayload {
  summary?: string;
  sections?: WorkflowInsightSection[];
}

export interface WorkflowInsightRecord {
  id: number;
  workflow_run_id: number;
  workflow_date: string;
  phase: WorkflowPhase;
  producer: string;
  status: InsightState;
  schema_version: string;
  producer_version: string;
  generated_at: string;
  source_run_id: number;
  source_context_schema_version: string;
  insight: WorkflowInsightPayload;
  error_message?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface WorkflowInsightResponse {
  insight: WorkflowInsightRecord | null;
  state: InsightState;
  is_stale: boolean;
  latest_run_id: number | null;
}

export interface WorkflowInsightUpsertRequest {
  workflow_date: string;
  phase: WorkflowPhase;
  producer?: string;
  status?: InsightState;
  schema_version?: string;
  producer_version: string;
  generated_at: string;
  source_run_id: number;
  source_context_schema_version?: string;
  insight: WorkflowInsightPayload;
  error_message?: string | null;
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
export const getLatestWorkflow = (phase: WorkflowPhase) =>
  fetchJson<WorkflowRunResponse | null>(`/workflow/latest?phase=${phase}`);
export const getLatestWorkflowContext = (phase: WorkflowPhase) =>
  fetchJson<WorkflowContextPayload | null>(`/workflow/context/latest?phase=${phase}`);
export const getLatestEtfAwContext = (asOfDate?: string) =>
  fetchJson<EtfAwSnapshotContext | null>(`/workflow/etf-aw/latest${asOfDate ? `?as_of_date=${encodeURIComponent(asOfDate)}` : ""}`);
export const getLatestEtfAwRiskBudget = (asOfDate?: string) =>
  fetchJson<EtfAwRiskBudget | null>(`/workflow/etf-aw/risk-budget/latest${asOfDate ? `?as_of_date=${encodeURIComponent(asOfDate)}` : ""}`);
export const getEtfAwShadowReport = (accountId?: string) =>
  fetchJson<EtfAwShadowReportResponse>(`/workflow/etf-aw/shadow-report${accountId ? `?account_id=${encodeURIComponent(accountId)}` : ""}`);
export const getEtfAwShadowStatus = (accountId = "etf-aw-paper") =>
  fetchJson<EtfAwShadowStatus>(`/workflow/etf-aw/shadow/status?account_id=${encodeURIComponent(accountId)}`);
export const updateEtfAwLocalShadow = (accountId = "etf-aw-paper") =>
  fetch(`${API_BASE}/workflow/etf-aw/shadow/update-local?account_id=${encodeURIComponent(accountId)}`, { method: "POST" }).then((res) => {
    if (!res.ok) {
      throw new Error(`${res.status} ${res.statusText}`);
    }
    return res.json() as Promise<EtfAwShadowUpdateResponse>;
  });
export const getEtfAwLocalPerformance = () =>
  fetchJson<EtfAwLocalPerformance | null>("/workflow/etf-aw/performance");
export const getEtfAwResearchSummary = () =>
  fetchJson<EtfAwResearchSummary>("/workflow/etf-aw/research-summary");
export const getLatestWorkflowInsight = (phase: WorkflowPhase, producer = "the_one") =>
  fetchJson<WorkflowInsightResponse>(`/workflow/insight/latest?phase=${phase}&producer=${encodeURIComponent(producer)}`);
export const upsertWorkflowInsight = (data: WorkflowInsightUpsertRequest) =>
  fetch(`${API_BASE}/workflow/insight`, { method: "PUT", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
export const runPreMarketWorkflow = (workflowDate?: string) =>
  fetch(`${API_BASE}/workflow/pre/run${workflowDate ? `?workflow_date=${encodeURIComponent(workflowDate)}` : ""}`, { method: "POST" }).then(r => r.json());
export const runPostMarketWorkflow = (workflowDate?: string) =>
  fetch(`${API_BASE}/workflow/post/run${workflowDate ? `?workflow_date=${encodeURIComponent(workflowDate)}` : ""}`, { method: "POST" }).then(r => r.json());

// Summary
export const getTradingStatus = () => fetchJson<any>("/summary/trading-status");
export const getWatchlist = () => fetchJson<any>("/summary/watchlist");
export const updateWatchlist = (data: any) => fetch(`${API_BASE}/summary/watchlist`, { method: "PUT", headers: {"Content-Type": "application/json"}, body: JSON.stringify(data) }).then(r => r.json());
