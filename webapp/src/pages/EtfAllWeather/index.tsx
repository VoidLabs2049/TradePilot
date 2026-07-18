import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Col,
  Empty,
  InputNumber,
  Progress,
  Row,
  Select,
  Space,
  Statistic,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import {
  CalendarOutlined,
  CheckCircleOutlined,
  DatabaseOutlined,
  ReloadOutlined,
  SyncOutlined,
  WarningOutlined,
} from "@ant-design/icons";
import { Area, Column, Line, Pie } from "@ant-design/charts";
import {
  getEtfAwShadowReport,
  getEtfAwShadowStatus,
  getEtfAwLocalPerformance,
  getEtfAwResearchSummary,
  getLatestEtfAwRiskBudget,
  updateEtfAwLocalShadow,
  type EtfAwPlanOrderRow,
  type EtfAwResearchSummary,
  type EtfAwShadowReportResponse,
  type EtfAwShadowStatus,
  type EtfAwShadowUpdateResponse,
  type EtfAwTargetWeightRow,
  type EtfAwLocalPerformance,
  type EtfAwRiskBudget,
  type EtfAwRiskBudgetSleeve,
  type EtfAwSleeveRole,
} from "../../services/api";
import "./index.css";

const { Text, Title } = Typography;
const DEFAULT_SHADOW_ACCOUNT = "etf-aw-v2-paper";
const BASELINE_SHADOW_ACCOUNT = "etf-aw-baseline-v2-paper";

const ROLE_LABELS: Record<EtfAwSleeveRole, string> = {
  equity_large: "大盘权益",
  equity_small: "小盘权益",
  equity_overseas: "纳指权益",
  bond: "债券",
  gold: "黄金",
  cash: "现金",
};

const ROLE_COLORS: Record<EtfAwSleeveRole, string> = {
  equity_large: "#1677ff",
  equity_small: "#13c2c2",
  equity_overseas: "#722ed1",
  bond: "#52c41a",
  gold: "#faad14",
  cash: "#8c8c8c",
};

const ROLE_ORDER: EtfAwSleeveRole[] = [
  "equity_large",
  "equity_small",
  "equity_overseas",
  "bond",
  "gold",
  "cash",
];

function roleLabel(role: string) {
  return ROLE_LABELS[role as EtfAwSleeveRole] || role;
}

function roleColor(role: string) {
  return ROLE_COLORS[role as EtfAwSleeveRole] || "#1677ff";
}

type PositionInput = Record<string, number>;

function statusColor(status?: string | null) {
  return (
    {
      complete: "green",
      partial: "orange",
      stale: "gold",
      missing: "red",
      unavailable: "red",
    } as Record<string, string>
  )[status || ""] || "default";
}

function formatPercent(value?: number | null) {
  return typeof value === "number" && Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : "-";
}

function formatSignedPercent(value?: number | null) {
  if (typeof value !== "number") {
    return "-";
  }
  const rounded = Math.round(value * 10000) / 100;
  if (Object.is(rounded, -0) || rounded === 0) {
    return "0.00%";
  }
  const prefix = rounded > 0 ? "+" : "";
  return `${prefix}${rounded.toFixed(2)}%`;
}

function formatCurrency(value?: number | null) {
  return typeof value === "number" && Number.isFinite(value)
    ? value.toLocaleString("zh-CN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : "-";
}

function formatQuantity(value?: number | null) {
  return typeof value === "number" && Number.isFinite(value)
    ? Math.round(value).toLocaleString("zh-CN")
    : "-";
}

function formatDecimal(value?: number | null) {
  return typeof value === "number" && Number.isFinite(value) ? value.toFixed(2) : "-";
}

function buildReturnDistribution(values: number[], binCount = 24) {
  if (values.length === 0) {
    return [];
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  const width = max === min ? 1 : (max - min) / binCount;
  const counts = Array.from({ length: binCount }, () => 0);
  values.forEach((value) => {
    const index = Math.min(Math.floor((value - min) / width), binCount - 1);
    counts[index] += 1;
  });
  return counts.map((count, index) => ({
    range: formatPercent(min + width * (index + 0.5)),
    count,
  }));
}

function qualityReasons(notes?: Record<string, any>): string[] {
  const reasons = notes?.reasons;
  return Array.isArray(reasons) ? reasons.map(String) : [];
}

function BudgetBar({ row }: { row: EtfAwRiskBudgetSleeve }) {
  const value = typeof row.tilted_budget === "number" ? row.tilted_budget : 0;
  return (
    <Space size={8} style={{ width: "100%" }}>
      <div
        style={{
          width: 92,
          height: 8,
          borderRadius: 4,
          background: "#f0f0f0",
          overflow: "hidden",
          flexShrink: 0,
        }}
      >
        <div
          style={{
            width: `${Math.max(0, Math.min(value * 100, 100))}%`,
            height: "100%",
            background: roleColor(row.sleeve_role),
          }}
        />
      </div>
      <Text>{formatPercent(row.tilted_budget)}</Text>
    </Space>
  );
}

export default function EtfAllWeather() {
  const [riskBudget, setRiskBudget] = useState<EtfAwRiskBudget | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [shadow, setShadow] = useState<EtfAwShadowReportResponse | null>(null);
  const [dynamicShadow, setDynamicShadow] = useState<EtfAwShadowReportResponse | null>(null);
  const [baselineShadow, setBaselineShadow] = useState<EtfAwShadowReportResponse | null>(null);
  const [accountId, setAccountId] = useState<string>();
  const [performance, setPerformance] = useState<EtfAwLocalPerformance | null>(null);
  const [researchSummary, setResearchSummary] = useState<EtfAwResearchSummary | null>(null);
  const [shadowStatus, setShadowStatus] = useState<EtfAwShadowStatus | null>(null);
  const [updatingShadow, setUpdatingShadow] = useState(false);
  const [shadowUpdate, setShadowUpdate] = useState<EtfAwShadowUpdateResponse | null>(null);
  const [totalAssetInput, setTotalAssetInput] = useState(200_000);
  const [cashInput, setCashInput] = useState(200_000);
  const [positionsInput, setPositionsInput] = useState<PositionInput>({});

  const refresh = async (selectedAccountId?: string) => {
    setLoading(true);
    setError(null);
    const requestedAccountId = selectedAccountId || accountId || DEFAULT_SHADOW_ACCOUNT;
    const failures: string[] = [];
    try {
      const [budgetResult, shadowResult, dynamicShadowResult, baselineShadowResult, performanceResult, statusResult, researchResult] = await Promise.allSettled([
        getLatestEtfAwRiskBudget(),
        getEtfAwShadowReport(requestedAccountId),
        getEtfAwShadowReport(DEFAULT_SHADOW_ACCOUNT),
        getEtfAwShadowReport(BASELINE_SHADOW_ACCOUNT),
        getEtfAwLocalPerformance(),
        getEtfAwShadowStatus(requestedAccountId),
        getEtfAwResearchSummary(),
      ]);

      if (budgetResult.status === "fulfilled") {
        setRiskBudget(budgetResult.value);
      } else {
        setRiskBudget(null);
        failures.push(`风险预算：${budgetResult.reason instanceof Error ? budgetResult.reason.message : "读取失败"}`);
      }
      if (shadowResult.status === "fulfilled") {
        setShadow(shadowResult.value);
        if (!accountId && shadowResult.value.accounts.length > 0) {
          setAccountId(
            shadowResult.value.accounts.includes(requestedAccountId)
              ? requestedAccountId
              : shadowResult.value.accounts[0],
          );
        }
      } else {
        setShadow(null);
        failures.push(`模拟盘：${shadowResult.reason instanceof Error ? shadowResult.reason.message : "读取失败"}`);
      }
      if (dynamicShadowResult.status === "fulfilled") {
        setDynamicShadow(dynamicShadowResult.value);
      } else {
        setDynamicShadow(null);
        failures.push(`动态模拟：${dynamicShadowResult.reason instanceof Error ? dynamicShadowResult.reason.message : "读取失败"}`);
      }
      if (baselineShadowResult.status === "fulfilled") {
        setBaselineShadow(baselineShadowResult.value);
      } else {
        setBaselineShadow(null);
        failures.push(`基线模拟：${baselineShadowResult.reason instanceof Error ? baselineShadowResult.reason.message : "读取失败"}`);
      }
      if (performanceResult.status === "fulfilled") {
        setPerformance(performanceResult.value);
      } else {
        setPerformance(null);
        failures.push(`本地绩效：${performanceResult.reason instanceof Error ? performanceResult.reason.message : "读取失败"}`);
      }
      if (statusResult.status === "fulfilled") {
        setShadowStatus(statusResult.value);
      } else {
        setShadowStatus(null);
        failures.push(`模拟盘状态：${statusResult.reason instanceof Error ? statusResult.reason.message : "读取失败"}`);
      }
      if (researchResult.status === "fulfilled") {
        setResearchSummary(researchResult.value);
      } else {
        setResearchSummary(null);
        failures.push(`方案结果：${researchResult.reason instanceof Error ? researchResult.reason.message : "读取失败"}`);
      }
      if (failures.length > 0) {
        setError(failures.join(" / "));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "ETF 全天候风险预算读取失败");
      setRiskBudget(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const updateLocalShadow = async () => {
    const selectedAccountId = accountId || shadow?.accounts[0] || DEFAULT_SHADOW_ACCOUNT;
    setUpdatingShadow(true);
    setError(null);
    try {
      const result = await updateEtfAwLocalShadow(
        selectedAccountId,
        selectedAccountId === BASELINE_SHADOW_ACCOUNT ? "baseline" : "target-weight",
      );
      setShadowUpdate(result);
      if (result.state === "invalid") {
        setError((result.blocking_reasons || ["本地模拟盘更新失败"]).join(" / "));
        return;
      }
      await refresh(selectedAccountId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "本地模拟盘更新失败");
    } finally {
      setUpdatingShadow(false);
    }
  };

  const budgets = riskBudget?.budgets || [];
  const reasons = qualityReasons(riskBudget?.quality_notes);
  const tiltedSum = riskBudget?.tilted_budget_sum;
  const sumOk = typeof tiltedSum === "number" && Math.abs(tiltedSum - 1) <= 0.000001;
  const effectiveConfidence = riskBudget?.effective_confidence_score;
  const activeRisk = useMemo(
    () =>
      budgets
        .filter((row) => typeof row.delta_budget === "number" && row.delta_budget !== 0)
        .map((row) => `${roleLabel(row.sleeve_role)} ${formatSignedPercent(row.delta_budget)}`),
    [budgets],
  );
  const allocationData = budgets
    .filter((row) => typeof row.tilted_budget === "number")
    .map((row) => ({
      role: roleLabel(row.sleeve_role),
      value: row.tilted_budget as number,
      sleeveRole: row.sleeve_role,
    }));
  const allocationSummary = budgets
    .filter((row) => typeof row.tilted_budget === "number")
    .sort((a, b) => (b.tilted_budget || 0) - (a.tilted_budget || 0));
  const shadowSeries = (shadow?.report?.daily_series || []).flatMap((row) => [
    { date: row.observation_date, series: "模拟盘", value: row.cumulative_return },
    ...(typeof row.baseline_cumulative_return === "number"
      ? [{ date: row.observation_date, series: "Baseline", value: row.baseline_cumulative_return }]
      : []),
    ...(typeof row.relative_cumulative_return === "number"
      ? [{ date: row.observation_date, series: "相对收益", value: row.relative_cumulative_return }]
      : []),
  ]);
  const shadowComparisonSeries = [
    ...(dynamicShadow?.report?.daily_series || []).map((row) => ({
      date: row.observation_date,
      series: "动态风险预算",
      value: row.cumulative_return,
    })),
    ...(baselineShadow?.report?.daily_series || []).map((row) => ({
      date: row.observation_date,
      series: "静态逆波动率",
      value: row.cumulative_return,
    })),
  ];
  const shadowComparisonRows = [
    { key: "dynamic", strategy: "动态风险预算", report: dynamicShadow?.report },
    { key: "baseline", strategy: "静态逆波动率", report: baselineShadow?.report },
  ].filter((row) => row.report);
  const dynamicReturn = dynamicShadow?.report?.metrics.period_return;
  const baselineReturn = baselineShadow?.report?.metrics.period_return;
  const returnLead =
    typeof dynamicReturn === "number" && typeof baselineReturn === "number"
      ? dynamicReturn - baselineReturn
      : null;
  const primaryStrategy =
    (performance?.metrics || []).find((row) => row.strategy !== "static_inverse_vol")?.strategy ||
    performance?.metrics?.[0]?.strategy;
  const performanceSeries = (performance?.series || []).map((row) => ({
    date: row.date,
    series: row.strategy === primaryStrategy ? "ETF 全天候" : "静态逆波动率基准",
    value: row.period_return,
  }));
  const primaryDailySeries = (performance?.series || [])
    .filter((row) => row.strategy === primaryStrategy)
    .sort((a, b) => a.date.localeCompare(b.date));
  let runningPeak = 0;
  const drawdownSeries = primaryDailySeries.map((row) => {
    runningPeak = Math.max(runningPeak, row.net_value);
    return {
      date: row.date,
      value: runningPeak > 0 ? row.net_value / runningPeak - 1 : 0,
    };
  });
  let currentUnderwaterDays = 0;
  let maxUnderwaterDays = 0;
  let currentDrawdownPeakIndex = 0;
  let maxDrawdownPeakIndex = 0;
  let maxDrawdownIndex = 0;
  let maxDrawdownValue = 0;
  let maxDrawdownRecoveryDays: number | null = null;
  let peakValue = 0;
  primaryDailySeries.forEach((row, index) => {
    if (row.net_value >= peakValue) {
      if (maxDrawdownRecoveryDays === null && index > maxDrawdownIndex && maxDrawdownValue < 0) {
        maxDrawdownRecoveryDays = index - maxDrawdownPeakIndex;
      }
      peakValue = row.net_value;
      currentDrawdownPeakIndex = index;
      currentUnderwaterDays = 0;
      return;
    }
    currentUnderwaterDays += 1;
    maxUnderwaterDays = Math.max(maxUnderwaterDays, currentUnderwaterDays);
    const drawdown = peakValue > 0 ? row.net_value / peakValue - 1 : 0;
    if (drawdown < maxDrawdownValue) {
      maxDrawdownValue = drawdown;
      maxDrawdownPeakIndex = currentDrawdownPeakIndex;
      maxDrawdownIndex = index;
      maxDrawdownRecoveryDays = null;
    }
  });
  const rollingVolatilitySeries = primaryDailySeries.flatMap((row, index, rows) => {
    if (index < 59) {
      return [];
    }
    const window = rows.slice(index - 59, index + 1).map((item) => item.daily_return);
    const mean = window.reduce((sum, value) => sum + value, 0) / window.length;
    const variance = window.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (window.length - 1);
    return [{ date: row.date, value: Math.sqrt(variance) * Math.sqrt(252) }];
  });
  const returnDistribution = buildReturnDistribution(primaryDailySeries.map((row) => row.daily_return));
  const strategyMetrics = Object.fromEntries(
    (performance?.metrics || [])
      .filter((row) => row.strategy === primaryStrategy)
      .map((row) => [row.metric, row.value]),
  );
  const calmar =
    typeof strategyMetrics.annualized_return === "number" && strategyMetrics.max_drawdown < 0
      ? strategyMetrics.annualized_return / Math.abs(strategyMetrics.max_drawdown)
      : null;
  const cost10Comparison = researchSummary?.robustness?.comparisons.find((row) => row.cost_scenario === "cost_10bps");
  const strategyRobustness = researchSummary?.robustness?.strategies.find((row) => row.label === "strategy");
  const baselineRobustness = researchSummary?.robustness?.strategies.find((row) => row.label === "baseline");
  const strategyCost10 = strategyRobustness?.scenarios.find((row) => row.cost_scenario === "cost_10bps");
  const baselineCost10 = baselineRobustness?.scenarios.find((row) => row.cost_scenario === "cost_10bps");
  const verdict = researchSummary?.robustness?.verdict;
  const verdictColor = verdict === "pass" ? "green" : verdict === "fail" ? "red" : "orange";
  const verdictText = verdict === "pass" ? "通过" : verdict === "fail" ? "失败" : "阻断";
  const fixedBacktest = researchSummary?.fixed_weight_backtest;
  const optimizedCandidate = fixedBacktest?.optimization.candidates.find(
    (row) => row.candidate_name === fixedBacktest.optimization.best_candidate_name,
  );
  const currentCandidate = fixedBacktest?.optimization.candidates.find((row) => row.candidate_name === "当前权重");
  const equalCandidate = fixedBacktest?.optimization.candidates.find((row) => row.candidate_name === "等权");
  const recentFrontier = fixedBacktest?.optimization.recent_return_frontier;
  const balancedRecentSolution = recentFrontier?.solutions.find(
    (row) => row.max_drawdown_limit === 0.07,
  )?.solution;
  const comparisonCards = [
    { name: "当前权重", candidate: currentCandidate, color: "orange" },
    { name: "优化候选", candidate: optimizedCandidate, color: "green" },
    { name: "等权基准", candidate: equalCandidate, color: "blue" },
  ].filter((item) => item.candidate);
  const priceByRole = Object.fromEntries(
    (shadowStatus?.latest_prices || []).map((row) => [row.sleeve_role, row.close]),
  );
  const codeByRole = Object.fromEntries(
    [
      ...(shadowStatus?.latest_prices || []).map((row) => [row.sleeve_role, row.sleeve_code]),
      ...(researchSummary?.target_weight.rows || []).map((row) => [row.sleeve_role, row.sleeve_code]),
    ],
  );
  const targetByRole = Object.fromEntries(
    optimizedCandidate
      ? ROLE_ORDER.map((role) => {
          const code = codeByRole[role];
          return [role, code ? optimizedCandidate.weights[code] || 0 : 0];
        })
      : researchSummary?.target_weight.rows.length
      ? researchSummary.target_weight.rows.map((row) => [row.sleeve_role, row.target_weight || 0])
      : budgets.map((row) => [row.sleeve_role, row.tilted_budget || 0]),
  );
  const positionMarketValue = ROLE_ORDER.reduce((sum, role) => {
    const quantity = positionsInput[role] || 0;
    const price = priceByRole[role] || 0;
    return sum + quantity * price;
  }, 0);
  const accountAsset = Math.max(totalAssetInput || 0, cashInput + positionMarketValue);
  const rebalanceRows = ROLE_ORDER.map((role) => {
    const price = priceByRole[role] || 0;
    const quantity = positionsInput[role] || 0;
    const currentMarketValue = quantity * price;
    const targetWeight = targetByRole[role] || 0;
    const targetMarketValue = accountAsset * targetWeight;
    const deltaNotional = targetMarketValue - currentMarketValue;
    const rawQuantity = price > 0 ? deltaNotional / price : 0;
    const orderQuantity = Math.floor(Math.abs(rawQuantity) / 100) * 100;
    const side = orderQuantity === 0 ? "HOLD" : deltaNotional > 0 ? "BUY" : "SELL";
    const signedQuantity = side === "SELL" ? -orderQuantity : side === "BUY" ? orderQuantity : 0;
    const estimatedNotional = Math.abs(signedQuantity) * price;
    const postMarketValue = currentMarketValue + signedQuantity * price;
    return {
      sleeve_role: role,
      symbol: codeByRole[role] || "-",
      price,
      quantity,
      current_market_value: currentMarketValue,
      current_weight: accountAsset > 0 ? currentMarketValue / accountAsset : 0,
      target_weight: targetWeight,
      target_market_value: targetMarketValue,
      drift: accountAsset > 0 ? currentMarketValue / accountAsset - targetWeight : 0,
      order_side: side,
      order_quantity: orderQuantity,
      estimated_notional: estimatedNotional,
      post_weight: accountAsset > 0 ? postMarketValue / accountAsset : 0,
      post_drift: accountAsset > 0 ? postMarketValue / accountAsset - targetWeight : 0,
    };
  });
  const estimatedBuyNotional = rebalanceRows
    .filter((row) => row.order_side === "BUY")
    .reduce((sum, row) => sum + row.estimated_notional, 0);
  const estimatedSellNotional = rebalanceRows
    .filter((row) => row.order_side === "SELL")
    .reduce((sum, row) => sum + row.estimated_notional, 0);
  const estimatedCashAfter = cashInput - estimatedBuyNotional + estimatedSellNotional;

  const budgetContent = riskBudget ? (
    <>
      {fixedBacktest ? (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 12 }}
          message="风险预算需要继续优化"
          description={`当前固定权重在 ${fixedBacktest.summary.profitable_segments}/${fixedBacktest.summary.segment_count} 个分段盈利，但只在 ${fixedBacktest.summary.beat_equal_weight_segments}/${fixedBacktest.summary.segment_count} 个分段跑赢等权。风险预算调参应优先提高相对基准稳定性。`}
        />
      ) : null}
      <Row gutter={[12, 12]}>
            <Col xs={24} md={6}>
              <Card size="small">
                <Statistic title="预算状态" value={riskBudget.budget_status || "-"} />
                <Tag color={statusColor(riskBudget.budget_status)} style={{ marginTop: 8 }}>
                  {riskBudget.budget_status || "unknown"}
                </Tag>
              </Card>
            </Col>
            <Col xs={24} md={6}>
              <Card size="small">
                <Statistic title="市场 Regime" value={riskBudget.market_regime_label || "-"} />
                <Text type="secondary">{riskBudget.budget_basis || "-"}</Text>
              </Card>
            </Col>
            <Col xs={24} md={6}>
              <Card size="small">
                <Statistic title="生效置信度" value={formatPercent(riskBudget.effective_confidence_score)} />
                <Text type="secondary">原始：{formatPercent(riskBudget.confidence_score)}</Text>
              </Card>
            </Col>
            <Col xs={24} md={6}>
              <Card size="small">
                <Statistic title="预算合计" value={formatPercent(tiltedSum)} />
                <Tag color={sumOk ? "green" : "red"} style={{ marginTop: 8 }}>{sumOk ? "sum ok" : "check sum"}</Tag>
              </Card>
            </Col>
          </Row>

          <Row gutter={[12, 12]} align="stretch">
            <Col xs={24} xl={8}>
              <Card title="当前资产配置" style={{ height: "100%" }}>
                <Pie
                  data={allocationData}
                  angleField="value"
                  colorField="role"
                  innerRadius={0.62}
                  height={280}
                  scale={{
                    color: { range: allocationData.map((item) => roleColor(item.sleeveRole)) },
                  }}
                  label={{ text: (item: { value: number }) => formatPercent(item.value), position: "outside" }}
                  legend={{ color: { position: "bottom", layout: { justifyContent: "center" } } }}
                  tooltip={{ items: [{ channel: "y", valueFormatter: (value: number) => formatPercent(value) }] }}
                  annotations={[{
                    type: "text",
                    style: {
                      text: formatPercent(tiltedSum),
                      x: "50%",
                      y: "50%",
                      textAlign: "center",
                      fontSize: 22,
                      fontWeight: 600,
                    },
                  }]}
                />
              </Card>
            </Col>
            <Col xs={24} xl={16}>
              <Card
                title="风险预算分配"
                extra={<Text type="secondary">调仓日：{riskBudget.rebalance_date || "-"}</Text>}
                style={{ height: "100%" }}
              >
            <Table
              size="small"
              pagination={false}
              scroll={{ x: 720 }}
              rowKey="sleeve_role"
              dataSource={budgets}
              columns={[
                {
                  title: "Sleeve",
                  dataIndex: "sleeve_role",
                  key: "sleeve_role",
                  render: (value: string) => (
                    <Space>
                      <span
                        style={{
                          display: "inline-block",
                          width: 10,
                          height: 10,
                          borderRadius: 2,
                          background: roleColor(value),
                        }}
                      />
                      <Text>{roleLabel(value)}</Text>
                      <Text type="secondary">{value}</Text>
                    </Space>
                  ),
                },
                { title: "中性预算", dataIndex: "base_budget", key: "base_budget", render: formatPercent },
                { title: "Regime 偏移", dataIndex: "delta_budget", key: "delta_budget", render: formatSignedPercent },
                {
                  title: "最终预算",
                  dataIndex: "tilted_budget",
                  key: "tilted_budget",
                  render: (_: unknown, row: EtfAwRiskBudgetSleeve) => <BudgetBar row={row} />,
                },
                {
                  title: "状态",
                  dataIndex: "budget_status",
                  key: "budget_status",
                  render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
                },
                {
                  title: "诊断",
                  key: "quality_notes",
                  render: (_: unknown, row: EtfAwRiskBudgetSleeve) => {
                    const rowReasons = qualityReasons(row.quality_notes);
                    return rowReasons.length > 0 ? (
                      <Tooltip title={rowReasons.join(" / ")}>
                        <Tag color="orange">{rowReasons.length} 条</Tag>
                      </Tooltip>
                    ) : (
                      <Text type="secondary">-</Text>
                    );
                  },
                },
              ]}
            />
              </Card>
            </Col>
          </Row>

          <Row gutter={[12, 12]}>
            <Col xs={24} lg={12}>
              <Card size="small" title="公式检查">
                <Space size={8} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
                  <Alert
                    type={sumOk ? "success" : "error"}
                    showIcon
                    message={`tilted_budget 合计 ${formatPercent(tiltedSum)}`}
                  />
                  <Text type="secondary">公式：tilted = normalize(base + effective_confidence * delta)</Text>
                  {typeof effectiveConfidence === "number" ? (
                    <Progress percent={Math.round(effectiveConfidence * 100)} />
                  ) : (
                    <Text type="secondary">生效置信度：-</Text>
                  )}
                </Space>
              </Card>
            </Col>
            <Col xs={24} lg={12}>
              <Card size="small" title="数据诊断">
                {reasons.length > 0 ? (
                  <Space wrap>
                    {reasons.map((reason) => <Tag color="orange" key={reason}>{reason}</Tag>)}
                  </Space>
                ) : (
                  <Alert type="success" showIcon message="暂无降级原因" />
                )}
                {activeRisk.length > 0 ? (
                  <div style={{ marginTop: 12 }}>
                    <Text type="secondary">当前偏移：</Text>
                    <div style={{ marginTop: 6 }}>
                      {activeRisk.map((item) => <Tag key={item}>{item}</Tag>)}
                    </div>
                  </div>
                ) : null}
              </Card>
            </Col>
          </Row>
        </>
  ) : null;

  const statusContent = (
    <Row gutter={[12, 12]}>
      <Col xs={24} lg={8}>
        <Card size="small">
          <Statistic title="本地行情最新日" value={shadowStatus?.latest_sleeve_daily_date || "-"} />
          <Text type="secondary">目标权重：{shadowStatus?.latest_target_weight_date || "-"}</Text>
        </Card>
      </Col>
      <Col xs={24} lg={8}>
        <Card size="small">
          <Statistic title="模拟盘观察最新日" value={shadowStatus?.latest_shadow_observation_date || "-"} />
          <Tag color={shadowStatus?.is_stale ? "orange" : "green"} style={{ marginTop: 8 }}>
            {shadowStatus?.is_stale ? "可补数据" : "已到最新"}
          </Tag>
        </Card>
      </Col>
      <Col xs={24} lg={8}>
        <Card size="small">
          <Statistic title="缺失观察日" value={shadowStatus?.missing_observation_dates.length || 0} />
          <Text type="secondary">{shadowStatus?.next_action || "-"}</Text>
        </Card>
      </Col>
    </Row>
  );

  const allocationContent = riskBudget ? (
    <Card
      title="当前 ETF 全天候指标分配"
      extra={<Text type="secondary">调仓日：{riskBudget.rebalance_date || "-"}</Text>}
    >
      <Row gutter={[12, 12]} align="middle">
        <Col xs={24} xl={8}>
          <Pie
            data={allocationData}
            angleField="value"
            colorField="role"
            innerRadius={0.62}
            height={260}
            scale={{
              color: { range: allocationData.map((item) => roleColor(item.sleeveRole)) },
            }}
            label={{ text: (item: { value: number }) => formatPercent(item.value), position: "outside" }}
            legend={{ color: { position: "bottom", layout: { justifyContent: "center" } } }}
            tooltip={{ items: [{ channel: "y", valueFormatter: (value: number) => formatPercent(value) }] }}
            annotations={[{
              type: "text",
              style: {
                text: formatPercent(tiltedSum),
                x: "50%",
                y: "50%",
                textAlign: "center",
                fontSize: 20,
                fontWeight: 600,
              },
            }]}
          />
        </Col>
        <Col xs={24} xl={16}>
          <Table
            size="small"
            pagination={false}
            rowKey="sleeve_role"
            dataSource={allocationSummary}
            columns={[
              {
                title: "资产",
                dataIndex: "sleeve_role",
                render: (value: string) => (
                  <Space>
                    <span
                      style={{
                        display: "inline-block",
                        width: 10,
                        height: 10,
                        borderRadius: 2,
                        background: roleColor(value),
                      }}
                    />
                    <Text>{roleLabel(value)}</Text>
                  </Space>
                ),
              },
              { title: "目标分配", dataIndex: "tilted_budget", render: formatPercent },
              { title: "中性预算", dataIndex: "base_budget", render: formatPercent },
              { title: "Regime 偏移", dataIndex: "delta_budget", render: formatSignedPercent },
              {
                title: "状态",
                dataIndex: "budget_status",
                render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
              },
            ]}
          />
        </Col>
      </Row>
    </Card>
  ) : null;

  const rebalanceContent = (
    <Card
      title="调仓试算"
      extra={<Text type="secondary">使用：{optimizedCandidate?.candidate_name || "当前目标权重"} · research-only</Text>}
    >
      <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
        <Alert
          type="warning"
          showIcon
          message="辅助决策试算"
          description="按最新本地收盘价和优化候选/当前目标分配计算，数量按 100 份取整，未计费用、滑点、涨跌停和成交约束。"
        />
        <Row gutter={[12, 12]}>
          <Col xs={24} md={8}>
            <Card size="small">
              <Text type="secondary">总资产</Text>
              <InputNumber
                min={0}
                step={10000}
                value={totalAssetInput}
                onChange={(value) => setTotalAssetInput(Number(value || 0))}
                style={{ width: "100%", marginTop: 8 }}
              />
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small">
              <Text type="secondary">当前现金</Text>
              <InputNumber
                min={0}
                step={1000}
                value={cashInput}
                onChange={(value) => setCashInput(Number(value || 0))}
                style={{ width: "100%", marginTop: 8 }}
              />
            </Card>
          </Col>
          <Col xs={24} md={8}>
            <Card size="small">
              <Statistic title="预计调仓后现金" value={formatCurrency(estimatedCashAfter)} />
              <Text type={estimatedCashAfter < 0 ? "danger" : "secondary"}>
                买入 {formatCurrency(estimatedBuyNotional)} · 卖出 {formatCurrency(estimatedSellNotional)}
              </Text>
            </Card>
          </Col>
        </Row>
        <Row gutter={[12, 12]}>
          {ROLE_ORDER.map((role) => (
            <Col xs={24} sm={12} lg={8} xl={4} key={role}>
              <Card size="small">
                <Space direction="vertical" size={6} style={{ width: "100%" }}>
                  <Text>{roleLabel(role)}</Text>
                  <Text type="secondary">{codeByRole[role] || "-"}</Text>
                  <InputNumber
                    min={0}
                    step={100}
                    value={positionsInput[role] || 0}
                    onChange={(value) => setPositionsInput({ ...positionsInput, [role]: Number(value || 0) })}
                    style={{ width: "100%" }}
                  />
                  <Text type="secondary">参考价 {formatCurrency(priceByRole[role])}</Text>
                </Space>
              </Card>
            </Col>
          ))}
        </Row>
        <Table
          size="small"
          pagination={false}
          scroll={{ x: 1120 }}
          rowKey="sleeve_role"
          dataSource={rebalanceRows}
          columns={[
            {
              title: "ETF",
              dataIndex: "sleeve_role",
              fixed: "left",
              render: (value: string, row) => (
                <Space direction="vertical" size={0}>
                  <Text>{roleLabel(value)}</Text>
                  <Text type="secondary">{row.symbol}</Text>
                </Space>
              ),
            },
            { title: "当前份额", dataIndex: "quantity", render: formatQuantity },
            { title: "当前权重", dataIndex: "current_weight", render: formatPercent },
            { title: "目标权重", dataIndex: "target_weight", render: formatPercent },
            { title: "偏离", dataIndex: "drift", render: formatSignedPercent },
            {
              title: "建议动作",
              dataIndex: "order_side",
              render: (value: string) => <Tag color={value === "BUY" ? "green" : value === "SELL" ? "red" : "default"}>{value}</Tag>,
            },
            { title: "建议份额", dataIndex: "order_quantity", render: formatQuantity },
            { title: "参考价格", dataIndex: "price", render: formatCurrency },
            { title: "预计成交额", dataIndex: "estimated_notional", render: formatCurrency },
            { title: "调仓后偏离", dataIndex: "post_drift", render: formatSignedPercent },
          ]}
        />
      </Space>
    </Card>
  );

  const researchContent = researchSummary ? (
    <Card
      title="方案效果与订单"
      extra={
        <Space>
          <Tag color={verdictColor}>{verdictText}</Tag>
          <Text type="secondary">
            {researchSummary.robustness?.comparable_range.start_date || "-"} 至 {researchSummary.robustness?.comparable_range.end_date || "-"}
          </Text>
        </Space>
      }
    >
      <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
        <Alert
          type={verdict === "pass" ? "success" : verdict === "fail" ? "error" : "warning"}
          showIcon
          message={`判定：${verdictText}`}
          description={`规则：${researchSummary.robustness?.decision_rule || "-"}。当前用于研究展示，不代表未来收益保证。`}
        />
        {recentFrontier ? (
          <Card
            size="small"
            title="近期收益 / 回撤约束前沿"
            extra={<Tag color="blue">近 6 个月优化</Tag>}
          >
            <Alert
              type="info"
              showIcon
              style={{ marginBottom: 12 }}
              message="在最大回撤阈值内最大化近期收益"
              description="5% / 7% / 10% 三档使用同一 long-only 搜索空间；近 12 个月、样本外和全区间仅用于复核，不参与近期目标排序。"
            />
            <Table
              size="small"
              pagination={false}
              rowKey="max_drawdown_limit"
              scroll={{ x: 760 }}
              dataSource={recentFrontier.solutions}
              columns={[
                { title: "回撤上限", dataIndex: "max_drawdown_limit", render: formatPercent },
                { title: "可行组合", dataIndex: "feasible_candidate_count" },
                { title: "近 6 月收益", render: (_, row) => formatPercent(row.solution?.recent_6m.total_return) },
                { title: "实际回撤", render: (_, row) => formatPercent(row.solution?.recent_6m.max_drawdown) },
                { title: "近 6 月 Sharpe", render: (_, row) => formatDecimal(row.solution?.recent_6m.sharpe_ratio) },
                { title: "近 12 月收益", render: (_, row) => formatPercent(row.solution?.validation.recent_12m?.total_return) },
                { title: "样本外收益", render: (_, row) => formatPercent(row.solution?.validation.out_of_sample?.total_return) },
              ]}
            />
            {balancedRecentSolution ? (
              <>
                <Alert
                  type="success"
                  showIcon
                  style={{ marginTop: 12, marginBottom: 12 }}
                  message={`7% 平衡档：近 6 个月收益 ${formatPercent(balancedRecentSolution.recent_6m.total_return)}，回撤 ${formatPercent(balancedRecentSolution.recent_6m.max_drawdown)}`}
                  description="研究候选，不会自动覆盖当前目标权重或模拟账户。"
                />
                <Table
                  size="small"
                  pagination={false}
                  rowKey="role"
                  dataSource={ROLE_ORDER.map((role) => {
                    const code = codeByRole[role];
                    const currentWeight = researchSummary.target_weight.rows.find(
                      (row) => row.sleeve_role === role,
                    )?.target_weight || 0;
                    const candidateWeight = code ? balancedRecentSolution.weights[code] || 0 : 0;
                    return {
                      role,
                      code,
                      currentWeight,
                      candidateWeight,
                      delta: candidateWeight - currentWeight,
                    };
                  })}
                  columns={[
                    {
                      title: "资产",
                      dataIndex: "role",
                      render: (value: string, row) => (
                        <Space direction="vertical" size={0}>
                          <Text>{roleLabel(value)}</Text>
                          <Text type="secondary">{row.code}</Text>
                        </Space>
                      ),
                    },
                    { title: "当前权重", dataIndex: "currentWeight", render: formatPercent },
                    { title: "7% 平衡档", dataIndex: "candidateWeight", render: formatPercent },
                    { title: "变化", dataIndex: "delta", render: formatSignedPercent },
                  ]}
                />
              </>
            ) : null}
          </Card>
        ) : null}
        <Row gutter={[12, 12]}>
          {comparisonCards.map((item) => (
            <Col xs={24} md={8} key={item.name}>
              <Card
                size="small"
                title={
                  <Space>
                    <Tag color={item.color}>{item.name}</Tag>
                    {item.name === fixedBacktest?.optimization.best_candidate_name ? <Tag color="green">推荐候选</Tag> : null}
                  </Space>
                }
              >
                <Row gutter={[8, 8]}>
                  <Col span={12}>
                    <Statistic
                      title="盈利分段"
                      value={`${item.candidate!.summary.profitable_segments}/${item.candidate!.summary.segment_count}`}
                    />
                  </Col>
                  <Col span={12}>
                    <Statistic
                      title="跑赢等权"
                      value={`${item.candidate!.summary.beat_equal_weight_segments}/${item.candidate!.summary.segment_count}`}
                    />
                  </Col>
                  <Col span={12}>
                    <Statistic
                      title="平均相对收益"
                      value={formatSignedPercent(item.candidate!.summary.average_total_return_diff)}
                    />
                  </Col>
                  <Col span={12}>
                    <Statistic
                      title="最差回撤"
                      value={formatPercent(item.candidate!.summary.worst_max_drawdown)}
                    />
                  </Col>
                </Row>
              </Card>
            </Col>
          ))}
        </Row>
        {optimizedCandidate ? (
          <Card
            size="small"
            title="推荐候选权重"
            extra={<Text type="secondary">{optimizedCandidate.candidate_name}</Text>}
          >
            <Table
              size="small"
              pagination={false}
              rowKey="role"
              dataSource={ROLE_ORDER.map((role) => {
                const code = codeByRole[role];
                const currentWeight = researchSummary.target_weight.rows.find((row) => row.sleeve_role === role)?.target_weight || 0;
                const optimizedWeight = code ? optimizedCandidate.weights[code] || 0 : 0;
                return {
                  role,
                  code,
                  currentWeight,
                  optimizedWeight,
                  delta: optimizedWeight - currentWeight,
                  equalWeight: 0.2,
                };
              })}
              columns={[
                {
                  title: "资产",
                  dataIndex: "role",
                  render: (value: string, row) => (
                    <Space direction="vertical" size={0}>
                      <Text>{roleLabel(value)}</Text>
                      <Text type="secondary">{row.code}</Text>
                    </Space>
                  ),
                },
                { title: "当前权重", dataIndex: "currentWeight", render: formatPercent },
                { title: "优化权重", dataIndex: "optimizedWeight", render: formatPercent },
                { title: "变化", dataIndex: "delta", render: formatSignedPercent },
                { title: "等权", dataIndex: "equalWeight", render: formatPercent },
              ]}
            />
          </Card>
        ) : null}
        <Row gutter={[12, 12]}>
          <Col xs={12} lg={6}>
            <Card size="small">
              <Statistic title="策略 10bps 后收益" value={formatPercent(strategyCost10?.net_total_return)} />
              <Text type="secondary">年化 {formatPercent(strategyCost10?.net_annualized_return)}</Text>
            </Card>
          </Col>
          <Col xs={12} lg={6}>
            <Card size="small">
              <Statistic title="基准 10bps 后收益" value={formatPercent(baselineCost10?.net_total_return)} />
              <Text type="secondary">年化 {formatPercent(baselineCost10?.net_annualized_return)}</Text>
            </Card>
          </Col>
          <Col xs={12} lg={6}>
            <Card size="small">
              <Statistic title="相对基准收益差" value={formatSignedPercent(cost10Comparison?.net_total_return_diff)} />
              <Text type="secondary">夏普差 {formatSignedPercent(cost10Comparison?.net_sharpe_ratio_diff)}</Text>
            </Card>
          </Col>
          <Col xs={12} lg={6}>
            <Card size="small">
              <Statistic title="策略最大回撤" value={formatPercent(strategyCost10?.net_max_drawdown)} />
              <Text type="secondary">平均换手 {formatPercent(strategyCost10?.average_turnover)}</Text>
            </Card>
          </Col>
        </Row>
        {fixedBacktest ? (
          <Card
            size="small"
            title="当前权重固定组合 · 多段历史模拟"
            extra={<Text type="secondary">权重日：{fixedBacktest.weight_rebalance_date || "-"}</Text>}
          >
            <Alert
              type="info"
              showIcon
              style={{ marginBottom: 12 }}
              message={`盈利 ${fixedBacktest.summary.profitable_segments}/${fixedBacktest.summary.segment_count} 段，跑赢等权 ${fixedBacktest.summary.beat_equal_weight_segments}/${fixedBacktest.summary.segment_count} 段`}
              description="这里把最新目标权重作为固定组合放入历史数据中分段模拟，并与 20% 等权基准比较；用于判断当前权重结构是否稳健。"
            />
            <Table
              size="small"
              pagination={false}
              scroll={{ x: 1180 }}
              rowKey={(row) => `${row.segment_type}-${row.start_date}-${row.end_date}`}
              dataSource={fixedBacktest.segments}
              columns={[
                {
                  title: "区间",
                  fixed: "left",
                  render: (_: unknown, row) => (
                    <Space direction="vertical" size={0}>
                      <Text>{row.segment_name}</Text>
                      <Text type="secondary">{row.start_date} 至 {row.end_date}</Text>
                    </Space>
                  ),
                },
                { title: "交易日", dataIndex: "observation_count" },
                {
                  title: "策略收益",
                  render: (_: unknown, row) => formatPercent(row.strategy.total_return),
                },
                {
                  title: "策略年化",
                  render: (_: unknown, row) => formatPercent(row.strategy.annualized_return),
                },
                {
                  title: "最大回撤",
                  render: (_: unknown, row) => formatPercent(row.strategy.max_drawdown),
                },
                {
                  title: "夏普",
                  render: (_: unknown, row) => formatDecimal(row.strategy.sharpe_ratio),
                },
                {
                  title: "等权收益",
                  render: (_: unknown, row) => formatPercent(row.equal_weight_baseline.total_return),
                },
                {
                  title: "相对等权",
                  render: (_: unknown, row) => formatSignedPercent(row.comparison.total_return_diff),
                },
                {
                  title: "盈利",
                  dataIndex: "profitable",
                  render: (value: boolean) => <Tag color={value ? "green" : "red"}>{value ? "盈利" : "亏损"}</Tag>,
                },
                {
                  title: "跑赢等权",
                  dataIndex: "beats_equal_weight",
                  render: (value: boolean) => <Tag color={value ? "green" : "orange"}>{value ? "是" : "否"}</Tag>,
                },
              ]}
            />
            {optimizedCandidate ? (
              <Alert
                type={
                  optimizedCandidate.summary.beat_equal_weight_segments === optimizedCandidate.summary.segment_count
                    ? "success"
                    : "warning"
                }
                showIcon
                style={{ marginTop: 12 }}
                message={`优化探索建议：${optimizedCandidate.candidate_name}`}
                description={
                  optimizedCandidate.summary.beat_equal_weight_segments === optimizedCandidate.summary.segment_count
                    ? "该候选在当前多段历史测试中全部跑赢等权；仍需继续做滚动样本外和交易成本敏感性验证。"
                    : "该候选仍未完全解决跑赢等权分段数不足的问题；下一轮优化应继续提高跨区间相对基准稳定性。"
                }
              />
            ) : null}
            <Table
              size="small"
              pagination={false}
              scroll={{ x: 1120 }}
              style={{ marginTop: 12 }}
              rowKey="candidate_name"
              dataSource={fixedBacktest.optimization.candidates}
              columns={[
                {
                  title: "候选权重",
                  dataIndex: "candidate_name",
                  fixed: "left",
                  render: (value: string, row) => (
                    <Space direction="vertical" size={0}>
                      <Text>{value}</Text>
                      <Text type="secondary">
                        {typeof row.shrinkage_to_equal_weight === "number"
                          ? `收缩到等权 ${formatPercent(row.shrinkage_to_equal_weight)}`
                          : row.search_method || "候选搜索"}
                      </Text>
                    </Space>
                  ),
                },
                {
                  title: "盈利分段",
                  render: (_: unknown, row) => `${row.summary.profitable_segments}/${row.summary.segment_count}`,
                },
                {
                  title: "跑赢等权",
                  render: (_: unknown, row) => `${row.summary.beat_equal_weight_segments}/${row.summary.segment_count}`,
                },
                {
                  title: "平均相对收益",
                  render: (_: unknown, row) => formatSignedPercent(row.summary.average_total_return_diff),
                },
                {
                  title: "最差回撤",
                  render: (_: unknown, row) => formatPercent(row.summary.worst_max_drawdown),
                },
                {
                  title: "权重",
                  render: (_: unknown, row) => (
                    <Space wrap size={[4, 4]}>
                      {ROLE_ORDER.map((role) => {
                        const code = codeByRole[role];
                        const value = code ? row.weights[code] : undefined;
                        return <Tag key={role}>{roleLabel(role)} {formatPercent(value)}</Tag>;
                      })}
                    </Space>
                  ),
                },
              ]}
            />
          </Card>
        ) : null}
        <Row gutter={[12, 12]}>
          <Col xs={24} xl={10}>
            <Card
              size="small"
              title="当前目标权重"
              extra={<Text type="secondary">调仓日：{researchSummary.target_weight.rebalance_date || "-"}</Text>}
            >
              <Table<EtfAwTargetWeightRow>
                size="small"
                pagination={false}
                rowKey="sleeve_role"
                dataSource={[...researchSummary.target_weight.rows].sort(
                  (a, b) => ROLE_ORDER.indexOf(a.sleeve_role) - ROLE_ORDER.indexOf(b.sleeve_role),
                )}
                columns={[
                  {
                    title: "ETF",
                    dataIndex: "sleeve_role",
                    render: (value: string, row) => (
                      <Space direction="vertical" size={0}>
                        <Text>{roleLabel(value)}</Text>
                        <Text type="secondary">{row.sleeve_code}</Text>
                      </Space>
                    ),
                  },
                  { title: "目标权重", dataIndex: "target_weight", render: formatPercent },
                  { title: "换手估计", dataIndex: "turnover_estimate", render: formatPercent },
                  {
                    title: "状态",
                    dataIndex: "target_weight_status",
                    render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
                  },
                ]}
              />
            </Card>
          </Col>
          <Col xs={24} xl={14}>
            <Card
              size="small"
              title="已生成模型订单（当前目标权重）"
              extra={<Text type="secondary">{researchSummary.latest_plan?.plan_id || "暂无订单"}</Text>}
            >
              {researchSummary.latest_plan ? (
                <Table<EtfAwPlanOrderRow>
                  size="small"
                  pagination={false}
                  scroll={{ x: 840 }}
                  rowKey="sleeve_role"
                  dataSource={[...researchSummary.latest_plan.rows].sort(
                    (a, b) => ROLE_ORDER.indexOf(a.sleeve_role) - ROLE_ORDER.indexOf(b.sleeve_role),
                  )}
                  columns={[
                    {
                      title: "ETF",
                      dataIndex: "sleeve_role",
                      fixed: "left",
                      render: (value: string, row) => (
                        <Space direction="vertical" size={0}>
                          <Text>{roleLabel(value)}</Text>
                          <Text type="secondary">{row.sleeve_code}</Text>
                        </Space>
                      ),
                    },
                    {
                      title: "方向",
                      dataIndex: "order_side",
                      render: (value: string) => <Tag color={value === "BUY" ? "green" : value === "SELL" ? "red" : "default"}>{value}</Tag>,
                    },
                    { title: "数量", dataIndex: "order_quantity", render: formatQuantity },
                    { title: "目标权重", dataIndex: "target_weight", render: formatPercent },
                    { title: "参考价格", dataIndex: "latest_price", render: formatCurrency },
                    { title: "预计金额", dataIndex: "estimated_notional", render: formatCurrency },
                    { title: "目标金额", dataIndex: "target_notional", render: formatCurrency },
                  ]}
                />
              ) : (
                <Empty description="暂无模型订单" />
              )}
            </Card>
          </Col>
        </Row>
      </Space>
    </Card>
  ) : (
    <Card><Empty description="暂无方案结果数据" /></Card>
  );

  const shadowContent = shadow?.state === "ready" && shadow.report ? (
    <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
      <Alert
        type="warning"
        showIcon
        message="模拟盘 · 零费用假设 · research-only · 未连接券商"
        description="这里用于观察前向净值和成交归因；策略是否值得继续优化，以多段历史模拟和扣费后基准对比为主。"
      />
      {shadowUpdate?.state === "updated" ? (
        <Alert
          type={shadowUpdate.observations_written ? "success" : "info"}
          showIcon
          message={`本地观察已更新：新增 ${shadowUpdate.observations_written || 0} 个交易日`}
          description={`账户 ${shadowUpdate.account_id || "-"} · seed ${shadowUpdate.seed_date || "-"}`}
        />
      ) : null}
      {shadowComparisonRows.length === 2 ? (
        <>
          <Alert
            type={typeof returnLead === "number" && returnLead >= 0 ? "success" : "info"}
            showIcon
            message={
              typeof returnLead === "number"
                ? `动态策略区间收益${returnLead >= 0 ? "领先" : "落后"} ${formatPercent(Math.abs(returnLead))}`
                : "动态策略与静态基线并行观察"
            }
            description="两组账户使用相同初始资产和观察区间；短样本只用于前向跟踪，不作为单独调参依据。"
          />
          <Card title="策略模拟对比" extra={<Tag color="blue">同区间</Tag>}>
            <Table
              size="small"
              pagination={false}
              rowKey="key"
              dataSource={shadowComparisonRows}
              columns={[
                { title: "策略", dataIndex: "strategy" },
                { title: "期末资产", render: (_, row) => formatCurrency(row.report?.metrics.ending_asset) },
                { title: "区间收益", render: (_, row) => formatSignedPercent(row.report?.metrics.period_return) },
                { title: "年化波动", render: (_, row) => formatPercent(row.report?.metrics.annualized_volatility) },
                { title: "最大回撤", render: (_, row) => formatPercent(row.report?.metrics.max_drawdown) },
                { title: "交易日", render: (_, row) => row.report?.integrity.observation_count || 0 },
              ]}
              scroll={{ x: 720 }}
            />
          </Card>
          <Card
            title="动态策略 vs 静态基线"
            extra={<Text type="secondary">{dynamicShadow?.report?.start_date} 至 {dynamicShadow?.report?.end_date}</Text>}
          >
            <Line
              data={shadowComparisonSeries}
              xField="date"
              yField="value"
              colorField="series"
              height={320}
              scale={{ color: { range: ["#1677ff", "#595959"] } }}
              axis={{ y: { labelFormatter: (value: number) => formatPercent(value) } }}
              tooltip={{ items: [{ channel: "y", valueFormatter: (value: number) => formatPercent(value) }] }}
            />
          </Card>
        </>
      ) : (
        <Alert type="info" showIcon message="对比账户数据尚未就绪" />
      )}
      <Row gutter={[12, 12]}>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="区间收益" value={formatPercent(shadow.report.metrics.period_return)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="最大回撤" value={formatPercent(shadow.report.metrics.max_drawdown)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="年化波动" value={formatPercent(shadow.report.metrics.annualized_volatility)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="观察交易日" value={shadow.report.integrity.observation_count} /></Card></Col>
      </Row>
      <Card title={`账户明细 · ${shadow.report.account_id}`} extra={<Text type="secondary">{shadow.report.start_date} 至 {shadow.report.end_date}</Text>}>
        <Line
          data={shadowSeries}
          xField="date"
          yField="value"
          colorField="series"
          height={320}
          axis={{ y: { labelFormatter: (value: number) => formatPercent(value) } }}
          tooltip={{ items: [{ channel: "y", valueFormatter: (value: number) => formatPercent(value) }] }}
        />
      </Card>
      <Row gutter={[12, 12]}>
        <Col xs={24} lg={12}>
          <Card title="模拟成交质量" size="small">
            <Table
              size="small"
              pagination={false}
              scroll={{ x: 560 }}
              rowKey={(row) => `${row.symbol}-${row.order_side}`}
              dataSource={shadow.report.fill_quality}
              columns={[
                { title: "标的", dataIndex: "symbol" },
                { title: "方向", dataIndex: "order_side" },
                { title: "成交率", dataIndex: "fill_ratio", render: formatPercent },
                { title: "成交均价", dataIndex: "volume_weighted_fill_price" },
                { title: "价格偏差", dataIndex: "price_deviation", render: formatPercent },
              ]}
            />
          </Card>
        </Col>
        <Col xs={24} lg={12}>
          <Card title="观察完整性" size="small">
            <Space direction="vertical">
              <Text>缺失 Baseline：{shadow.report.integrity.missing_baseline_dates.length} 天</Text>
              <Text>成交不可归因：{shadow.report.integrity.unattributable_fill_dates.length} 天</Text>
              <Space wrap>{Object.entries(shadow.report.integrity.warnings).map(([key, count]) => <Tag color="orange" key={key}>{key} · {count}</Tag>)}</Space>
            </Space>
          </Card>
        </Col>
      </Row>
    </Space>
  ) : (
    <Card>
      <Empty
        description={shadow?.state === "awaiting_observation" ? "账户已初始化，尚无每日观察数据" : "尚未初始化模拟盘观察账户"}
      />
      {shadow?.blocking_reasons?.length ? <Alert type="error" message={shadow.blocking_reasons.join(" / ")} /> : null}
    </Card>
  );

  const performanceContent = performance ? (
    <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
      <Alert
        type="info"
        showIcon
        message={`本地 lakehouse · ${performance.source_dataset}`}
        description="历史效果展示动态月度权重净值；顶部多段模拟展示的是“当前最新权重固定不变”的稳健性测试，两者口径不同。"
      />
      <Row gutter={[12, 12]}>
        <Col xs={12} md={8} xl={4}><Card size="small"><Statistic title="累计收益" value={formatPercent(strategyMetrics.total_return)} /></Card></Col>
        <Col xs={12} md={8} xl={4}><Card size="small"><Statistic title="年化收益" value={formatPercent(strategyMetrics.annualized_return)} /></Card></Col>
        <Col xs={12} md={8} xl={4}><Card size="small"><Statistic title="年化波动" value={formatPercent(strategyMetrics.annualized_volatility)} /></Card></Col>
        <Col xs={12} md={8} xl={4}><Card size="small"><Statistic title="最大回撤" value={formatPercent(strategyMetrics.max_drawdown)} /></Card></Col>
        <Col xs={12} md={8} xl={4}><Card size="small"><Statistic title="Sharpe" value={formatDecimal(strategyMetrics.sharpe_ratio)} /></Card></Col>
        <Col xs={12} md={8} xl={4}><Card size="small"><Statistic title="Calmar" value={formatDecimal(calmar)} /></Card></Col>
        <Col xs={12} md={8} xl={4}><Card size="small"><Statistic title="最长水下" value={maxUnderwaterDays} suffix="交易日" /></Card></Col>
        <Col xs={12} md={8} xl={4}><Card size="small"><Statistic title="最大回撤恢复" value={maxDrawdownRecoveryDays ?? "未恢复"} suffix={maxDrawdownRecoveryDays === null ? undefined : "交易日"} /></Card></Col>
      </Row>
      <Card
        title="累计收益与基准"
        extra={<Text type="secondary">{performance.start_date} 至 {performance.end_date} · {performance.observation_count} 个交易日</Text>}
      >
        <Line
          data={performanceSeries}
          xField="date"
          yField="value"
          colorField="series"
          height={360}
          axis={{ y: { labelFormatter: (value: number) => formatPercent(value) } }}
          tooltip={{ items: [{ channel: "y", valueFormatter: (value: number) => formatPercent(value) }] }}
        />
      </Card>
      <div className="etf-aw-chart-grid">
        <Card title="回撤曲线" size="small">
          <Area
            data={drawdownSeries}
            xField="date"
            yField="value"
            height={280}
            style={{ fill: "#ffccc7", fillOpacity: 0.65, stroke: "#cf1322", lineWidth: 1.5 }}
            axis={{ y: { labelFormatter: (value: number) => formatPercent(value) } }}
            tooltip={{ items: [{ channel: "y", valueFormatter: (value: number) => formatPercent(value) }] }}
          />
        </Card>
        <Card title="60 日滚动年化波动率" size="small">
          {rollingVolatilitySeries.length > 0 ? (
            <Line
              data={rollingVolatilitySeries}
              xField="date"
              yField="value"
              height={280}
              style={{ stroke: "#d97706", lineWidth: 2 }}
              axis={{ y: { labelFormatter: (value: number) => formatPercent(value) } }}
              tooltip={{ items: [{ channel: "y", valueFormatter: (value: number) => formatPercent(value) }] }}
            />
          ) : <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="至少需要 60 个交易日" />}
        </Card>
        <Card title="日收益率分布" size="small" className="etf-aw-chart-grid-wide">
          <Column
            data={returnDistribution}
            xField="range"
            yField="count"
            height={280}
            style={{ fill: "#69b1ff", stroke: "#1677ff" }}
            axis={{ x: { title: "日收益率", labelAutoRotate: false }, y: { title: "交易日数" } }}
            tooltip={{ items: [{ channel: "y", name: "交易日数" }] }}
          />
        </Card>
      </div>
    </Space>
  ) : <Card><Empty description="本地 lakehouse 暂无历史绩效数据" /></Card>;

  const overviewItems = [
    {
      key: "verdict",
      icon: verdict === "pass" ? <CheckCircleOutlined /> : <WarningOutlined />,
      label: "研究判定",
      value: verdictText,
      detail: researchSummary?.robustness?.decision_rule || "等待稳健性结果",
      tone: verdict === "pass" ? "positive" : verdict === "fail" ? "negative" : "warning",
    },
    {
      key: "weight",
      icon: <CalendarOutlined />,
      label: "目标权重日期",
      value: researchSummary?.target_weight.rebalance_date || "-",
      detail: `${researchSummary?.target_weight.rows.length || 0} 个资产袖套`,
      tone: "neutral",
    },
    {
      key: "market",
      icon: <DatabaseOutlined />,
      label: "行情最新日",
      value: shadowStatus?.latest_sleeve_daily_date || "-",
      detail: shadowStatus?.is_stale ? "观察数据待补齐" : "本地数据已同步",
      tone: shadowStatus?.is_stale ? "warning" : "positive",
    },
    {
      key: "observation",
      icon: <SyncOutlined />,
      label: "缺失观察日",
      value: String(shadowStatus?.missing_observation_dates.length || 0),
      detail: shadowStatus?.next_action || "无需操作",
      tone: shadowStatus?.missing_observation_dates.length ? "warning" : "neutral",
    },
  ];

  return (
    <div className="etf-aw-page">
      <header className="etf-aw-header">
        <div className="etf-aw-heading">
          <Text className="etf-aw-eyebrow">RESEARCH WORKSPACE</Text>
          <Title level={2}>ETF 全天候</Title>
          <Text type="secondary">风险预算、组合验证与模拟盘观察</Text>
        </div>
        <div className="etf-aw-actions">
          {shadow && shadow.accounts.length > 1 ? (
            <Select
              aria-label="模拟盘账户"
              value={accountId}
              options={shadow.accounts.map((value) => ({ value }))}
              onChange={(value) => { setAccountId(value); refresh(value); }}
            />
          ) : null}
          <Button icon={<SyncOutlined />} loading={updatingShadow} onClick={updateLocalShadow}>
            更新观察
          </Button>
          <Tooltip title="刷新全部研究数据">
            <Button aria-label="刷新全部研究数据" icon={<ReloadOutlined />} loading={loading} onClick={() => refresh()} />
          </Tooltip>
        </div>
      </header>

      {error ? <Alert type="error" showIcon message="ETF 全天候数据读取失败" description={error} /> : null}

      <section className="etf-aw-overview" aria-label="研究状态总览">
        {overviewItems.map((item) => (
          <div className={`etf-aw-overview-item etf-aw-overview-item--${item.tone}`} key={item.key}>
            <span className="etf-aw-overview-icon">{item.icon}</span>
            <div className="etf-aw-overview-copy">
              <Text type="secondary">{item.label}</Text>
              <strong>{item.value}</strong>
              <Text type="secondary" ellipsis={{ tooltip: item.detail }}>{item.detail}</Text>
            </div>
          </div>
        ))}
      </section>

      <Tabs
        className="etf-aw-workspace-tabs"
        defaultActiveKey="overview"
        items={[
          {
            key: "overview",
            label: "决策总览",
            children: <div className="etf-aw-tab-stack">{researchContent}{allocationContent}</div>,
          },
          {
            key: "rebalance",
            label: "调仓工作台",
            children: <div className="etf-aw-tab-stack">{statusContent}{rebalanceContent}</div>,
          },
          { key: "performance", label: "历史证据", children: performanceContent },
          { key: "shadow", label: "模拟盘", children: shadowContent },
          { key: "budget", label: "风险预算", children: budgetContent },
        ]}
      />
    </div>
  );
}
