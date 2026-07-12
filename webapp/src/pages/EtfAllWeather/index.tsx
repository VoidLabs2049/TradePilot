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
import { ReloadOutlined, SyncOutlined } from "@ant-design/icons";
import { Line, Pie } from "@ant-design/charts";
import {
  getEtfAwShadowReport,
  getEtfAwShadowStatus,
  getEtfAwLocalPerformance,
  getLatestEtfAwRiskBudget,
  updateEtfAwLocalShadow,
  type EtfAwShadowReportResponse,
  type EtfAwShadowStatus,
  type EtfAwShadowUpdateResponse,
  type EtfAwLocalPerformance,
  type EtfAwRiskBudget,
  type EtfAwRiskBudgetSleeve,
} from "../../services/api";

const { Text, Title } = Typography;

const ROLE_LABELS: Record<string, string> = {
  equity_large: "大盘权益",
  equity_small: "小盘权益",
  bond: "债券",
  gold: "黄金",
  cash: "现金",
};

const ROLE_COLORS: Record<string, string> = {
  equity_large: "#1677ff",
  equity_small: "#13c2c2",
  bond: "#52c41a",
  gold: "#faad14",
  cash: "#8c8c8c",
};

const ROLE_ORDER = ["equity_large", "equity_small", "bond", "gold", "cash"];

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
  return typeof value === "number" ? `${(value * 100).toFixed(2)}%` : "-";
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
            background: ROLE_COLORS[row.sleeve_role] || "#1677ff",
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
  const [accountId, setAccountId] = useState<string>();
  const [performance, setPerformance] = useState<EtfAwLocalPerformance | null>(null);
  const [shadowStatus, setShadowStatus] = useState<EtfAwShadowStatus | null>(null);
  const [updatingShadow, setUpdatingShadow] = useState(false);
  const [shadowUpdate, setShadowUpdate] = useState<EtfAwShadowUpdateResponse | null>(null);
  const [totalAssetInput, setTotalAssetInput] = useState(1_000_000);
  const [cashInput, setCashInput] = useState(0);
  const [positionsInput, setPositionsInput] = useState<PositionInput>({});

  const refresh = async (selectedAccountId?: string) => {
    setLoading(true);
    setError(null);
    const requestedAccountId = selectedAccountId || accountId || "etf-aw-paper";
    try {
      const [budgetData, shadowData, performanceData, statusData] = await Promise.all([
        getLatestEtfAwRiskBudget(),
        getEtfAwShadowReport(requestedAccountId),
        getEtfAwLocalPerformance(),
        getEtfAwShadowStatus(requestedAccountId),
      ]);
      setRiskBudget(budgetData);
      setShadow(shadowData);
      setPerformance(performanceData);
      setShadowStatus(statusData);
      if (!accountId && shadowData.accounts.length > 0) {
        setAccountId(shadowData.accounts[0]);
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
    const selectedAccountId = accountId || shadow?.accounts[0] || "etf-aw-paper";
    setUpdatingShadow(true);
    setError(null);
    try {
      const result = await updateEtfAwLocalShadow(selectedAccountId);
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
        .map((row) => `${ROLE_LABELS[row.sleeve_role] || row.sleeve_role} ${formatSignedPercent(row.delta_budget)}`),
    [budgets],
  );
  const allocationData = budgets
    .filter((row) => typeof row.tilted_budget === "number")
    .map((row) => ({
      role: ROLE_LABELS[row.sleeve_role] || row.sleeve_role,
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
  const performanceSeries = (performance?.series || []).map((row) => ({
    date: row.date,
    series: row.strategy === "etf_aw_v1" ? "ETF 全天候" : "静态风险平价 Baseline",
    value: row.period_return,
  }));
  const strategyMetrics = Object.fromEntries(
    (performance?.metrics || [])
      .filter((row) => row.strategy === "etf_aw_v1")
      .map((row) => [row.metric, row.value]),
  );
  const priceByRole = Object.fromEntries(
    (shadowStatus?.latest_prices || []).map((row) => [row.sleeve_role, row.close]),
  );
  const codeByRole = Object.fromEntries(
    (shadowStatus?.latest_prices || []).map((row) => [row.sleeve_role, row.sleeve_code]),
  );
  const targetByRole = Object.fromEntries(
    budgets.map((row) => [row.sleeve_role, row.tilted_budget || 0]),
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
                    color: { range: allocationData.map((item) => ROLE_COLORS[item.sleeveRole] || "#1677ff") },
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
                          background: ROLE_COLORS[value] || "#1677ff",
                        }}
                      />
                      <Text>{ROLE_LABELS[value] || value}</Text>
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
              color: { range: allocationData.map((item) => ROLE_COLORS[item.sleeveRole] || "#1677ff") },
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
                        background: ROLE_COLORS[value] || "#1677ff",
                      }}
                    />
                    <Text>{ROLE_LABELS[value] || value}</Text>
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
      extra={<Text type="secondary">research-only · 不连接券商 · 不自动下单</Text>}
    >
      <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
        <Alert
          type="warning"
          showIcon
          message="辅助决策试算"
          description="按最新本地收盘价和当前目标分配计算，数量按 100 份取整，未计费用、滑点、涨跌停和成交约束。"
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
                  <Text>{ROLE_LABELS[role] || role}</Text>
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
                  <Text>{ROLE_LABELS[value] || value}</Text>
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

  const shadowContent = shadow?.state === "ready" && shadow.report ? (
    <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
      <Alert
        type="warning"
        showIcon
        message="模拟盘 · 零费用假设 · research-only · 未连接券商"
      />
      {shadowUpdate?.state === "updated" ? (
        <Alert
          type={shadowUpdate.observations_written ? "success" : "info"}
          showIcon
          message={`本地观察已更新：新增 ${shadowUpdate.observations_written || 0} 个交易日`}
          description={`账户 ${shadowUpdate.account_id || "-"} · seed ${shadowUpdate.seed_date || "-"}`}
        />
      ) : null}
      <Row gutter={[12, 12]}>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="区间收益" value={formatPercent(shadow.report.metrics.period_return)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="最大回撤" value={formatPercent(shadow.report.metrics.max_drawdown)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="年化波动" value={formatPercent(shadow.report.metrics.annualized_volatility)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="观察交易日" value={shadow.report.integrity.observation_count} /></Card></Col>
      </Row>
      <Card title="Forward 净值表现" extra={<Text type="secondary">{shadow.report.start_date} 至 {shadow.report.end_date}</Text>}>
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
      <Alert type="info" showIcon message={`本地 lakehouse · ${performance.source_dataset}`} />
      <Row gutter={[12, 12]}>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="累计收益" value={formatPercent(strategyMetrics.total_return)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="年化收益" value={formatPercent(strategyMetrics.annualized_return)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="最大回撤" value={formatPercent(strategyMetrics.max_drawdown)} /></Card></Col>
        <Col xs={12} lg={6}><Card size="small"><Statistic title="交易日" value={performance.observation_count} /></Card></Col>
      </Row>
      <Card title="策略历史效果" extra={<Text type="secondary">{performance.start_date} 至 {performance.end_date}</Text>}>
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
    </Space>
  ) : <Card><Empty description="本地 lakehouse 暂无历史绩效数据" /></Card>;

  return (
    <Space size={16} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
        <div>
          <Title level={3} style={{ marginBottom: 4 }}>ETF 全天候</Title>
          <Text type="secondary">风险预算与 Stage O forward observation</Text>
        </div>
        <Space>
          {shadow && shadow.accounts.length > 1 ? <Select value={accountId} options={shadow.accounts.map((value) => ({ value }))} onChange={(value) => { setAccountId(value); refresh(value); }} /> : null}
          <Button icon={<SyncOutlined />} loading={updatingShadow} onClick={updateLocalShadow}>更新本地观察</Button>
          <Button icon={<ReloadOutlined />} loading={loading} onClick={() => refresh()}>刷新</Button>
        </Space>
      </div>
      {error ? <Alert type="error" showIcon message="ETF 全天候数据读取失败" description={error} /> : null}
      {statusContent}
      {allocationContent}
      {rebalanceContent}
      <Tabs items={[
        { key: "performance", label: "历史效果", children: performanceContent },
        { key: "shadow", label: "模拟盘观察", children: shadowContent },
        { key: "budget", label: "风险预算", children: budgetContent },
      ]} />
    </Space>
  );
}
