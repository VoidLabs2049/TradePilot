import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Col,
  Empty,
  Progress,
  Row,
  Space,
  Statistic,
  Table,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import {
  getLatestEtfAwRiskBudget,
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
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${(value * 100).toFixed(2)}%`;
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

  const refresh = async () => {
    setLoading(true);
    try {
      setRiskBudget(await getLatestEtfAwRiskBudget());
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  const budgets = riskBudget?.budgets || [];
  const reasons = qualityReasons(riskBudget?.quality_notes);
  const tiltedSum = riskBudget?.tilted_budget_sum;
  const sumOk = typeof tiltedSum === "number" && Math.abs(tiltedSum - 1) <= 0.000001;
  const activeRisk = useMemo(
    () =>
      budgets
        .filter((row) => typeof row.delta_budget === "number" && row.delta_budget !== 0)
        .map((row) => `${ROLE_LABELS[row.sleeve_role] || row.sleeve_role} ${formatSignedPercent(row.delta_budget)}`),
    [budgets],
  );

  return (
    <Space size={16} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
        <div>
          <Title level={3} style={{ marginBottom: 4 }}>ETF 全天候</Title>
          <Text type="secondary">风险预算用于后续目标权重生成；当前页面只展示已冻结 artifact。</Text>
        </div>
        <Button icon={<ReloadOutlined />} loading={loading} onClick={refresh}>刷新</Button>
      </div>

      {!riskBudget && !loading ? (
        <Card>
          <Empty description="暂无 ETF 全天候风险预算数据" />
        </Card>
      ) : null}

      {riskBudget ? (
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

          <Card
            title="风险预算分配"
            extra={<Text type="secondary">调仓日：{riskBudget.rebalance_date || "-"}</Text>}
          >
            <Table
              size="small"
              pagination={false}
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
                  <Progress percent={Math.round((riskBudget.effective_confidence_score || 0) * 100)} />
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
      ) : null}
    </Space>
  );
}
