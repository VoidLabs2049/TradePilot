import { useEffect, useMemo, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Drawer,
  List,
  Row,
  Space,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from "antd";
import OvernightNewsTabs, { newsDirectionColor, newsDirectionLabel } from "./OvernightNewsTabs";
import { HistoryOutlined, ReloadOutlined } from "@ant-design/icons";
import {
  getLatestEtfAwContext,
  getLatestWorkflow,
  getLatestWorkflowContext,
  getLatestWorkflowInsight,
  getSchedulerHistory,
  getSchedulerStatus,
  getWorkflowHistory,
  getWorkflowStatus,
  runPostMarketWorkflow,
  runPreMarketWorkflow,
  type EtfAwSleeveSnapshot,
  type EtfAwSnapshotContext,
  type WorkflowContextPayload,
  type WorkflowInsightResponse,
  type WorkflowInsightSection,
  type WorkflowPhase,
  type WorkflowRunResponse,
  type WorkflowStepResult,
} from "../../services/api";

const { Paragraph, Text, Title } = Typography;

function statusColor(status?: string) {
  return (
    {
      success: "green",
      partial: "orange",
      failed: "red",
      skipped: "default",
      completed: "green",
      stale: "orange",
      pending: "blue",
      not_requested: "default",
      complete: "green",
      missing: "red",
    } as Record<string, string>
  )[status || ""] || "default";
}

function stepStatusColor(status?: string) {
  return (
    {
      success: "green",
      failed: "red",
      skipped: "default",
      partial: "orange",
    } as Record<string, string>
  )[status || ""] || "default";
}

function formatPercent(value?: number | null) {
  return typeof value === "number" ? `${(value * 100).toFixed(2)}%` : "-";
}

function formatQualityValue(value: unknown): string {
  if (Array.isArray(value)) {
    return value.join(" / ");
  }
  if (value && typeof value === "object") {
    return Object.entries(value as Record<string, unknown>)
      .map(([key, entry]) => `${key}: ${formatQualityValue(entry)}`)
      .join("；");
  }
  return value == null || value === "" ? "-" : String(value);
}

function etfAwDiagnostics(row: EtfAwSleeveSnapshot): string[] {
  const notes = row.quality_notes || {};
  const diagnostics: string[] = [];
  if (notes.stale_sources) {
    diagnostics.push(`stale: ${formatQualityValue(notes.stale_sources)}`);
  }
  if (notes.source_lag) {
    diagnostics.push(`lag: ${formatQualityValue(notes.source_lag)}`);
  }
  if (notes.partial_features) {
    diagnostics.push(`partial: ${formatQualityValue(notes.partial_features)}`);
  }
  if (notes.window_observations) {
    diagnostics.push(`obs: ${formatQualityValue(notes.window_observations)}`);
  }
  if (notes.missing_reason) {
    diagnostics.push(`missing: ${formatQualityValue(notes.missing_reason)}`);
  }
  return diagnostics;
}

function getOverviewText(context: WorkflowContextPayload | null, workflow?: WorkflowRunResponse | null) {
  return context?.metadata?.overview || workflow?.run.summary.overview || workflow?.run.error_message || "暂无摘要";
}

function getTitleText(context: WorkflowContextPayload | null, workflow?: WorkflowRunResponse | null) {
  return context?.metadata?.title || workflow?.run.summary.title || "工作流";
}

function getInsightIntro(state?: string, schemaVersion?: string) {
  if (state === "stale") {
    return "当前 The-One insight 基于旧 context 生成，TradePilot briefing 仍以最新 workflow 为准。";
  }
  if (state === "failed") {
    return "The-One insight 生成失败，当前继续展示 TradePilot briefing。";
  }
  if (state === "completed") {
    return `The-One 补充分析已就绪 · ${schemaVersion || "unknown schema"}`;
  }
  if (state === "pending") {
    return "The-One insight 生成中，当前先查看 TradePilot briefing。";
  }
  return "尚未生成 The-One insight，当前展示 TradePilot briefing。";
}

function renderMetricDescriptions(section: WorkflowInsightSection) {
  const metrics = Array.isArray(section.metrics) ? section.metrics : [];
  if (metrics.length === 0) {
    return null;
  }
  return (
    <Descriptions
      column={1}
      size="small"
      items={metrics.map((item, metricIndex) => ({
        key: `${section.key}-metric-${metricIndex}`,
        label: item.label || `指标${metricIndex + 1}`,
        children: item.value ?? "-",
      }))}
    />
  );
}

function renderListItems(section: WorkflowInsightSection) {
  const items = Array.isArray(section.items) ? section.items : [];
  if (items.length === 0) {
    return null;
  }
  return (
    <List
      size="small"
      dataSource={items}
      renderItem={(item) => (
        <List.Item>
          <div style={{ width: "100%" }}>
            <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
              {item.title ? <Text strong>{item.title}</Text> : null}
              {item.status ? <Tag color={statusColor(item.status)}>{item.status}</Tag> : null}
              {(item.tags || []).map((tag) => <Tag key={`${item.title || item.description}-${tag}`}>{tag}</Tag>)}
            </div>
            {item.description ? <div style={{ marginTop: 4, color: "#666" }}>{item.description}</div> : null}
          </div>
        </List.Item>
      )}
    />
  );
}

function renderBulletList(section: WorkflowInsightSection) {
  const bullets = Array.isArray(section.bullets) ? section.bullets : [];
  if (bullets.length === 0) {
    return null;
  }
  return (
    <List
      size="small"
      dataSource={bullets}
      renderItem={(item: string) => <List.Item>{item}</List.Item>}
    />
  );
}

function renderTags(section: WorkflowInsightSection) {
  const tags = Array.isArray(section.tags) ? section.tags : [];
  if (tags.length === 0) {
    return null;
  }
  return <div>{tags.map((tag) => <Tag key={`${section.key}-${tag}`}>{tag}</Tag>)}</div>;
}

function renderStandardInsightSection(section: WorkflowInsightSection, index: number) {
  const baseContent = (
    <Space size={10} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
      {section.summary ? <Paragraph style={{ marginBottom: 0 }}>{section.summary}</Paragraph> : null}
      {renderTags(section)}
      {renderMetricDescriptions(section)}
      {renderListItems(section)}
      {renderBulletList(section)}
    </Space>
  );

  const key = section.key || `section-${index}`;

  switch (section.key) {
    case "market_view":
      return (
        <Col xs={24} lg={12} key={key}>
          <Card size="small" title={section.title || "市场视角"} extra={<Tag color="blue">market</Tag>}>
            {baseContent}
          </Card>
        </Col>
      );
    case "theme_view":
      return (
        <Col xs={24} lg={12} key={key}>
          <Card size="small" title={section.title || "主题 / 板块视角"} extra={<Tag color="purple">theme</Tag>}>
            {baseContent}
          </Card>
        </Col>
      );
    case "position_view":
      return (
        <Col xs={24} lg={12} key={key}>
          <Card size="small" title={section.title || "持仓视角"} extra={<Tag color="gold">position</Tag>}>
            {baseContent}
          </Card>
        </Col>
      );
    case "tomorrow_view":
      return (
        <Col xs={24} lg={12} key={key}>
          <Card size="small" title={section.title || "明日准备"} extra={<Tag color="cyan">tomorrow</Tag>}>
            {baseContent}
          </Card>
        </Col>
      );
    case "action_frame":
      return (
        <Col xs={24} lg={12} key={key}>
          <Card size="small" title={section.title || "操作框架"} extra={<Tag color="green">action</Tag>}>
            {baseContent}
          </Card>
        </Col>
      );
    case "risk_notes":
      return (
        <Col xs={24} lg={12} key={key}>
          <Card size="small" title={section.title || "风险提示"} extra={<Tag color="red">risk</Tag>}>
            {baseContent}
          </Card>
        </Col>
      );
    case "execution_notes":
      return (
        <Col xs={24} key={key}>
          <Card size="small" title={section.title || "执行备注"} extra={<Tag>notes</Tag>}>
            {baseContent}
          </Card>
        </Col>
      );
    default:
      return (
        <Col xs={24} lg={12} key={key}>
          <Card size="small" title={section.title || section.key || `Section ${index + 1}`} extra={<Tag>custom</Tag>}>
            {baseContent}
          </Card>
        </Col>
      );
  }
}

export default function Dashboard() {
  const [activePhase, setActivePhase] = useState<WorkflowPhase>("pre_market");
  const [preWorkflow, setPreWorkflow] = useState<WorkflowRunResponse | null>(null);
  const [postWorkflow, setPostWorkflow] = useState<WorkflowRunResponse | null>(null);
  const [preContext, setPreContext] = useState<WorkflowContextPayload | null>(null);
  const [postContext, setPostContext] = useState<WorkflowContextPayload | null>(null);
  const [etfAwContext, setEtfAwContext] = useState<EtfAwSnapshotContext | null>(null);
  const [preInsight, setPreInsight] = useState<WorkflowInsightResponse | null>(null);
  const [postInsight, setPostInsight] = useState<WorkflowInsightResponse | null>(null);
  const [workflowStatus, setWorkflowStatus] = useState<any>(null);
  const [workflowHistory, setWorkflowHistory] = useState<any[]>([]);
  const [schedulerStatus, setSchedulerStatus] = useState<any>(null);
  const [schedulerHistory, setSchedulerHistory] = useState<any[]>([]);
  const [runningPhase, setRunningPhase] = useState<WorkflowPhase | null>(null);
  const [loading, setLoading] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);

  const refreshData = async () => {
    setLoading(true);
    try {
      const [
        pre,
        post,
        preContextData,
        postContextData,
        etfAwContextData,
        preInsightData,
        postInsightData,
        status,
        history,
        scheduler,
        schedulerRuns,
      ] = await Promise.all([
        getLatestWorkflow("pre_market"),
        getLatestWorkflow("post_market"),
        getLatestWorkflowContext("pre_market"),
        getLatestWorkflowContext("post_market"),
        getLatestEtfAwContext(),
        getLatestWorkflowInsight("pre_market"),
        getLatestWorkflowInsight("post_market"),
        getWorkflowStatus(),
        getWorkflowHistory(10),
        getSchedulerStatus(),
        getSchedulerHistory(10),
      ]);
      setPreWorkflow(pre);
      setPostWorkflow(post);
      setPreContext(preContextData);
      setPostContext(postContextData);
      setEtfAwContext(etfAwContextData);
      setPreInsight(preInsightData);
      setPostInsight(postInsightData);
      setWorkflowStatus(status);
      setWorkflowHistory(history);
      setSchedulerStatus(scheduler);
      setSchedulerHistory(schedulerRuns);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refreshData();
  }, []);

  const handleRunWorkflow = async (phase: WorkflowPhase) => {
    setRunningPhase(phase);
    try {
      if (phase === "pre_market") {
        await runPreMarketWorkflow();
      } else {
        await runPostMarketWorkflow();
      }
      await refreshData();
    } finally {
      setRunningPhase(null);
    }
  };

  const currentWorkflow = useMemo(() => {
    return activePhase === "pre_market" ? preWorkflow : postWorkflow;
  }, [activePhase, postWorkflow, preWorkflow]);

  const currentContext = useMemo(() => {
    return activePhase === "pre_market" ? preContext : postContext;
  }, [activePhase, postContext, preContext]);

  const currentInsight = useMemo(() => {
    return activePhase === "pre_market" ? preInsight : postInsight;
  }, [activePhase, postInsight, preInsight]);

  const summary = currentWorkflow?.run.summary;
  const steps: WorkflowStepResult[] = summary?.steps || [];
  const context = currentContext?.context || {};
  const alerts = context?.alerts || summary?.alerts || [];
  const watchContext = context?.watch_context || summary?.watch_context || {};
  const watchSectors = watchContext?.watch_sectors || [];
  const watchStocks = watchContext?.watch_stocks || [];
  const overnightNews = context?.overnight_news || summary?.overnight_news || {};
  const yesterdayRecap = context?.yesterday_recap || {};
  const todayWatchlist = context?.today_watchlist || {};
  const actionFrame = context?.action_frame || {};
  const marketOverview = context?.market_overview || {};
  const sectorPositioning = context?.sector_positioning || {};
  const positionHealth = context?.position_health || {};
  const nextDayPrep = context?.next_day_prep || {};
  const etfAwSnapshot: EtfAwSnapshotContext | null = context?.etf_aw_context || etfAwContext;
  const newsItems = overnightNews?.highlights || [];
  const categorizedNews = Object.entries(overnightNews?.categorized || {}).filter(([, items]) => Array.isArray(items) && items.length > 0);
  const newsSectorMappings = overnightNews?.sector_mappings || [];
  const newsFocusSectors = todayWatchlist?.news_focus_sectors || actionFrame?.news_focus_sectors || [];
  const positiveNewsSectors = actionFrame?.positive_news_sectors || [];
  const riskNewsSectors = actionFrame?.risk_news_sectors || [];
  const trackedItems = positionHealth?.tracked_items || [];
  const watchSectorRecords = sectorPositioning?.watch_sectors || [];
  const etfAwRows = etfAwSnapshot?.sleeves || [];

  const insightPayload = currentInsight?.insight?.insight || {};
  const insightSections: WorkflowInsightSection[] = Array.isArray(insightPayload?.sections) ? insightPayload.sections : [];
  const hasInsight = currentInsight?.state === "completed" || currentInsight?.state === "stale";
  const insightSummary = insightPayload?.summary;
  const briefingHighlights = activePhase === "pre_market"
    ? [
        actionFrame?.posture ? `姿态：${actionFrame.posture}` : null,
        (actionFrame?.focus_directions || []).length > 0 ? `关注方向：${(actionFrame.focus_directions || []).slice(0, 3).join(" / ")}` : null,
        (todayWatchlist?.focus_sectors || []).length > 0
          ? `重点板块：${(todayWatchlist.focus_sectors || []).slice(0, 3).map((item: any) => item.sector_name).filter(Boolean).join(" / ")}`
          : null,
        newsItems.length > 0 ? `隔夜信息：${newsItems.length} 条` : null,
      ].filter(Boolean)
    : [
        marketOverview?.regime ? `市场状态：${marketOverview.regime}` : null,
        (sectorPositioning?.market_leaders || []).length > 0
          ? `主线方向：${(sectorPositioning.market_leaders || []).slice(0, 3).map((item: any) => item.sector_name).filter(Boolean).join(" / ")}`
          : null,
        nextDayPrep?.market_bias ? `明日偏向：${nextDayPrep.market_bias}` : null,
        trackedItems.length > 0 ? `跟踪对象：${trackedItems.length} 个` : null,
      ].filter(Boolean);

  const etfAwPanel = {
    key: "etf-aw",
    label: "ETF 全天候上下文",
    children: (
      <Space size={10} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
        <Space wrap>
          <Tag color={statusColor(etfAwSnapshot?.data_status)}>{etfAwSnapshot?.data_status || "none"}</Tag>
          <Text type="secondary">调仓日：{etfAwSnapshot?.rebalance_date || "-"}</Text>
        </Space>
        <Table
          size="small"
          pagination={false}
          rowKey="sleeve_code"
          dataSource={etfAwRows}
          columns={[
            { title: "Sleeve", dataIndex: "sleeve_role", key: "sleeve_role" },
            { title: "代码", dataIndex: "sleeve_code", key: "sleeve_code" },
            { title: "1M", dataIndex: "return_1m", key: "return_1m", render: formatPercent },
            { title: "3M", dataIndex: "return_3m", key: "return_3m", render: formatPercent },
            { title: "6M", dataIndex: "return_6m", key: "return_6m", render: formatPercent },
            { title: "3M 波动", dataIndex: "volatility_3m", key: "volatility_3m", render: formatPercent },
            { title: "6M 回撤", dataIndex: "max_drawdown_6m", key: "max_drawdown_6m", render: formatPercent },
            { title: "状态", dataIndex: "data_status", key: "data_status", render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag> },
            {
              title: "诊断",
              key: "quality_notes",
              render: (_: unknown, row: EtfAwSleeveSnapshot) => {
                const diagnostics = etfAwDiagnostics(row);
                if (diagnostics.length === 0) {
                  return <Text type="secondary">-</Text>;
                }
                return (
                  <Tooltip title={diagnostics.join("；")}>
                    <Text type="secondary">{diagnostics[0]}</Text>
                  </Tooltip>
                );
              },
            },
          ]}
        />
      </Space>
    ),
  };

  const contextPanels = activePhase === "pre_market"
    ? [
        {
          key: "recap",
          label: "昨日复盘摘要",
          children: (
            <Space size={8} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
              <Paragraph style={{ marginBottom: 0 }}>{yesterdayRecap?.summary || "暂无上一交易日盘后结论。"}</Paragraph>
              <Text type="secondary">市场状态：{yesterdayRecap?.regime || "unknown"}</Text>
            </Space>
          ),
        },
        {
          key: "news",
          label: "隔夜信息",
          children: (
            <OvernightNewsTabs
              highlights={newsItems}
              categorizedNews={categorizedNews}
              sectorMappings={newsSectorMappings}
            />
          ),
        },
        etfAwPanel,
        {
          key: "watchlist",
          label: "今日关注清单",
          children: (
            <Row gutter={[12, 12]}>
              <Col xs={24} lg={12}>
                <Card size="small" title="市场大势观察">
                  <List
                    size="small"
                    dataSource={todayWatchlist?.market_checkpoints || []}
                    locale={{ emptyText: "暂无观察项" }}
                    renderItem={(item: string) => <List.Item>{item}</List.Item>}
                  />
                </Card>
              </Col>
              <Col xs={24} lg={12}>
                <Card size="small" title="重点板块">
                  {(todayWatchlist?.focus_sectors || []).length > 0
                    ? (todayWatchlist.focus_sectors || []).map((item: any) => (
                        <Tag key={item.sector_name} color={item.news_matched ? newsDirectionColor(item.news_direction) || "gold" : undefined}>
                          {item.sector_name}
                          {item.news_matched ? ` · ${newsDirectionLabel(item.news_direction)}` : ""}
                        </Tag>
                      ))
                    : <Text type="secondary">暂无</Text>}
                  {newsFocusSectors.length > 0 ? (
                    <div style={{ marginTop: 8 }}>
                      <Text type="secondary">隔夜新闻重点映射：</Text>
                      <div style={{ marginTop: 6 }}>
                        {newsFocusSectors.map((sector: string) => {
                          const direction = positiveNewsSectors.includes(sector) ? "positive" : riskNewsSectors.includes(sector) ? "negative" : "mixed";
                          return <Tag key={`news-${sector}`} color={newsDirectionColor(direction)}>{sector} · {newsDirectionLabel(direction)}</Tag>;
                        })}
                      </div>
                    </div>
                  ) : null}
                </Card>
              </Col>
              <Col xs={24}>
                <Card size="small" title="持仓观察">
                  {(todayWatchlist?.position_watch || []).length > 0 ? (todayWatchlist.position_watch || []).map((item: any) => (
                    <div key={item.code} style={{ fontSize: 12, marginBottom: 6 }}>
                      <Text strong>{item.name}</Text>
                      <Text type="secondary"> · {item.today_observation}</Text>
                    </div>
                  )) : <Text type="secondary">暂无</Text>}
                </Card>
              </Col>
            </Row>
          ),
        },
        {
          key: "action-frame",
          label: "操作框架",
          children: (
            <Descriptions
              column={1}
              size="small"
              items={[
                { key: "posture", label: "姿态", children: actionFrame?.posture || "observe" },
                {
                  key: "focus",
                  label: "关注方向",
                  children: (actionFrame?.focus_directions || []).length > 0
                    ? (actionFrame.focus_directions || []).map((item: string) => (
                        <Tag
                          key={item}
                          color={
                            positiveNewsSectors.includes(item)
                              ? newsDirectionColor("positive")
                              : riskNewsSectors.includes(item)
                                ? newsDirectionColor("negative")
                                : newsFocusSectors.includes(item)
                                  ? newsDirectionColor("mixed")
                                  : undefined
                          }
                        >
                          {item}
                          {positiveNewsSectors.includes(item)
                            ? " · 偏利多"
                            : riskNewsSectors.includes(item)
                              ? " · 偏风险"
                              : newsFocusSectors.includes(item)
                                ? " · 待观察"
                                : ""}
                        </Tag>
                      ))
                    : "暂无",
                },
                {
                  key: "risk",
                  label: "风险提示",
                  children: (actionFrame?.risk_warnings || []).length > 0
                    ? <List size="small" dataSource={actionFrame.risk_warnings || []} renderItem={(item: string) => <List.Item>{item}</List.Item>} />
                    : "暂无",
                },
                {
                  key: "news-focus",
                  label: "新闻驱动方向",
                  children: newsFocusSectors.length > 0
                    ? newsFocusSectors.map((item: string) => {
                        const direction = positiveNewsSectors.includes(item) ? "positive" : riskNewsSectors.includes(item) ? "negative" : "mixed";
                        return <Tag key={`action-news-${item}`} color={newsDirectionColor(direction)}>{item} · {newsDirectionLabel(direction)}</Tag>;
                      })
                    : "暂无",
                },
              ]}
            />
          ),
        },
      ]
    : [
        {
          key: "market-overview",
          label: "市场大势",
          children: (
            <Space size={8} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
              <Paragraph style={{ marginBottom: 0 }}>{marketOverview?.summary || "暂无市场大势结论"}</Paragraph>
              <Text type="secondary">市场状态：{marketOverview?.regime || "neutral"}</Text>
              {(marketOverview?.key_takeaways || []).length > 0 ? (
                <List size="small" dataSource={marketOverview.key_takeaways || []} renderItem={(item: string) => <List.Item>{item}</List.Item>} />
              ) : null}
            </Space>
          ),
        },
        {
          key: "sector-positioning",
          label: "板块定位",
          children: (
            <Descriptions
              column={1}
              size="small"
              items={[
                {
                  key: "leaders",
                  label: "当日主线",
                  children: (sectorPositioning?.market_leaders || []).length > 0
                    ? (sectorPositioning.market_leaders || []).map((item: any) => <Tag key={item.sector_name}>{item.sector_name}</Tag>)
                    : "暂无",
                },
                {
                  key: "watch-sectors",
                  label: "固定观察池",
                  children: watchSectorRecords.length > 0
                    ? watchSectorRecords.map((item: any) => <Tag key={item.sector_name}>{item.sector_name}</Tag>)
                    : "暂无",
                },
              ]}
            />
          ),
        },
        etfAwPanel,
        {
          key: "position-health",
          label: "持仓健康度",
          children: (
            <Table
              dataSource={trackedItems}
              rowKey={(item: any) => `${item.subject_type}-${item.code}`}
              size="small"
              pagination={false}
              locale={{ emptyText: "暂无持仓/观察对象" }}
              columns={[
                { title: "对象", dataIndex: "name", width: 120 },
                { title: "类型", dataIndex: "subject_type", width: 100 },
                {
                  title: "状态",
                  dataIndex: "state",
                  width: 100,
                  render: (value: string) => <Tag color={stepStatusColor(value === "breakdown" ? "failed" : value === "breakout" ? "success" : value === "watch" ? "partial" : "skipped")}>{value}</Tag>,
                },
                { title: "观察要点", dataIndex: "observation_note", render: (value: string) => value || "-" },
              ]}
            />
          ),
        },
        {
          key: "next-day-prep",
          label: "明日准备",
          children: (
            <Descriptions
              column={1}
              size="small"
              items={[
                { key: "bias", label: "市场偏向", children: nextDayPrep?.market_bias || "observe" },
                {
                  key: "focus-sectors",
                  label: "重点方向",
                  children: (nextDayPrep?.focus_sectors || []).length > 0
                    ? (nextDayPrep.focus_sectors || []).map((item: string) => <Tag key={item}>{item}</Tag>)
                    : "暂无",
                },
                {
                  key: "risk-notes",
                  label: "风险提示",
                  children: (nextDayPrep?.risk_notes || []).length > 0
                    ? <List size="small" dataSource={nextDayPrep.risk_notes || []} renderItem={(item: string) => <List.Item>{item}</List.Item>} />
                    : "暂无",
                },
              ]}
            />
          ),
        },
      ];

  function renderExecMetaCard() {
    if (!currentWorkflow) return null;
    return (
      <Card size="small" title="执行元信息">
        <Descriptions
          column={1}
          size="small"
          items={[
            { key: "phase", label: "阶段", children: currentWorkflow.run.phase },
            { key: "triggered_by", label: "触发", children: currentWorkflow.run.triggered_by },
            { key: "context_schema", label: "Context Schema", children: currentContext?.schema_version || "-" },
            { key: "insight_schema", label: "Insight Schema", children: currentInsight?.insight?.schema_version || "-" },
            { key: "generated_at", label: "Context 时间", children: currentContext?.generated_at ? String(currentContext.generated_at).slice(0, 16).replace("T", " ") : "-" },
          ]}
        />
      </Card>
    );
  }

  function renderStepsCard() {
    return (
      <Card size="small" title="执行步骤">
        <Table
          dataSource={steps}
          rowKey="name"
          size="small"
          pagination={false}
          columns={[
            { title: "步骤", dataIndex: "name", width: 130 },
            {
              title: "状态",
              dataIndex: "status",
              width: 90,
              render: (value: string) => <Tag color={stepStatusColor(value)}>{value}</Tag>,
            },
            { title: "错误", dataIndex: "error_message", render: (value: string | null) => value || "-" },
          ]}
        />
      </Card>
    );
  }

  function renderAlertsCard() {
    return (
      <Card size="small" title="最近预警">
        <List
          size="small"
          dataSource={alerts}
          locale={{ emptyText: "暂无预警" }}
          renderItem={(item: any) => (
            <List.Item>
              <div style={{ width: "100%" }}>
                <div>
                  <Tag color={item.read_at ? "default" : "red"}>{item.urgency || "medium"}</Tag>
                  <Text strong>{item.title}</Text>
                </div>
                {item.message ? <Text type="secondary">{item.message}</Text> : null}
              </div>
            </List.Item>
          )}
        />
      </Card>
    );
  }

  function renderWatchlistCard() {
    return (
      <Card size="small" title="关注池">
        <div style={{ marginBottom: 10 }}>
          <Text strong>关注板块</Text>
          <div style={{ marginTop: 6 }}>
            {watchSectors.length > 0 ? watchSectors.map((sector: string) => <Tag key={sector}>{sector}</Tag>) : <Text type="secondary">暂无</Text>}
          </div>
        </div>
        <div>
          <Text strong>关注股票</Text>
          <div style={{ marginTop: 6 }}>
            {watchStocks.length > 0 ? watchStocks.map((stock: any) => (
              <Tag key={stock.code}>{stock.name ? `${stock.name}(${stock.code})` : stock.code}</Tag>
            )) : <Text type="secondary">暂无</Text>}
          </div>
        </div>
      </Card>
    );
  }

  function renderHistoryDrawer() {
    return (
      <Drawer
        title="运行历史"
        placement="right"
        width={680}
        open={historyOpen}
        onClose={() => setHistoryOpen(false)}
      >
        <Space size={16} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
          <Card size="small" title="Workflow 历史">
            <Table
              dataSource={workflowHistory}
              rowKey="id"
              size="small"
              pagination={false}
              columns={[
                { title: "阶段", dataIndex: "phase", width: 120 },
                {
                  title: "状态",
                  dataIndex: "status",
                  width: 90,
                  render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
                },
                { title: "触发", dataIndex: "triggered_by", width: 90 },
                {
                  title: "时间",
                  dataIndex: "started_at",
                  render: (value: string) => String(value).slice(5, 16).replace("T", " "),
                },
              ]}
            />
          </Card>
          <Card size="small" title="Scheduler 历史">
            <Table
              dataSource={schedulerHistory}
              rowKey="id"
              size="small"
              pagination={false}
              columns={[
                { title: "任务", dataIndex: "job_name", width: 150 },
                {
                  title: "状态",
                  dataIndex: "status",
                  width: 90,
                  render: (value: string) => <Tag color={statusColor(value)}>{value}</Tag>,
                },
                { title: "影响", dataIndex: "records_affected", width: 70 },
                {
                  title: "时间",
                  dataIndex: "started_at",
                  render: (value: string) => String(value).slice(5, 16).replace("T", " "),
                },
              ]}
            />
          </Card>
        </Space>
      </Drawer>
    );
  }

  function renderLeftColumn() {
    if (!currentWorkflow) {
      return <Alert type="info" showIcon description="先运行一次盘前或盘后 workflow。" />;
    }
    return (
      <Space size={16} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
        <Card size="small" variant="borderless" style={{ background: "#f6ffed", borderLeft: "3px solid #52c41a", borderRadius: 8 }}>
          <Space size={10} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "flex-start", flexWrap: "wrap" }}>
              <div>
                <Title level={4} style={{ margin: 0 }}>{getTitleText(currentContext, currentWorkflow)}</Title>
                <Text type="secondary">{currentWorkflow.run.workflow_date}</Text>
              </div>
              <Space>
                <Tag color={statusColor(currentWorkflow.run.status)}>{currentWorkflow.run.status}</Tag>
                <Tag color={statusColor(currentInsight?.state)}>{currentInsight?.state || "not_requested"}</Tag>
              </Space>
            </div>

            <Paragraph style={{ marginBottom: 0 }}>
              {getOverviewText(currentContext, currentWorkflow)}
            </Paragraph>

            {briefingHighlights.length > 0 ? (
              <div>
                {briefingHighlights.map((item) => <Tag key={item}>{item}</Tag>)}
              </div>
            ) : null}

            <Alert
              type={currentInsight?.state === "failed" ? "error" : currentInsight?.state === "stale" ? "warning" : hasInsight ? "success" : "info"}
              showIcon
              description={getInsightIntro(currentInsight?.state, currentInsight?.insight?.schema_version)}
            />
          </Space>
        </Card>

        <div style={{ borderTop: "1px solid #e8e8e8", paddingTop: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: "#389e0d", marginBottom: 12, letterSpacing: 1, textTransform: "uppercase" }}>
            {activePhase === "pre_market" ? "盘前简报" : "盘后简报"}
          </div>
          <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
            {contextPanels.map((panel) => (
              <Card key={panel.key} size="small" title={panel.label} style={{ background: "#fafafa" }}>
                {panel.children}
              </Card>
            ))}
          </Space>
        </div>

        {hasInsight && insightSections.length > 0 ? (
          <div style={{ background: "#f0f7ff", borderRadius: 8, padding: "12px 12px 0" }}>
            <div style={{ fontSize: 12, fontWeight: 600, color: "#1677ff", marginBottom: 8, letterSpacing: 1, textTransform: "uppercase" }}>
              The-One 补充分析
            </div>
            {insightSummary ? <Paragraph style={{ marginBottom: 12 }}>{insightSummary}</Paragraph> : null}
            <Row gutter={[12, 12]}>
              {insightSections.map((section, index) => renderStandardInsightSection(section, index))}
            </Row>
          </div>
        ) : null}
      </Space>
    );
  }

  function renderSystemStatusCard() {
    return (
      <Card size="small" title="系统状态">
        <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
          <div>
            <Text type="secondary" style={{ fontSize: 12 }}>盘前状态</Text>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 4 }}>
              <Tag color={statusColor(workflowStatus?.pre_market?.status)}>{workflowStatus?.pre_market?.status || "none"}</Tag>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {workflowStatus?.pre_market?.finished_at
                  ? String(workflowStatus.pre_market.finished_at).slice(0, 16).replace("T", " ")
                  : "尚未执行"}
              </Text>
            </div>
          </div>
          <div>
            <Text type="secondary" style={{ fontSize: 12 }}>盘后状态</Text>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 4 }}>
              <Tag color={statusColor(workflowStatus?.post_market?.status)}>{workflowStatus?.post_market?.status || "none"}</Tag>
              <Text type="secondary" style={{ fontSize: 12 }}>
                {workflowStatus?.post_market?.finished_at
                  ? String(workflowStatus.post_market.finished_at).slice(0, 16).replace("T", " ")
                  : "尚未执行"}
              </Text>
            </div>
          </div>
          <div>
            <Text type="secondary" style={{ fontSize: 12 }}>调度器</Text>
            <div style={{ marginTop: 4 }}>
              <Tag color={schedulerStatus?.running ? "green" : "default"}>
                {schedulerStatus?.running ? "运行中" : "未运行"}
              </Tag>
            </div>
            {(schedulerStatus?.jobs || []).length > 0 ? (
              <div style={{ marginTop: 6, display: "flex", flexDirection: "column", gap: 4 }}>
                {(schedulerStatus.jobs as any[]).map((job: any) => (
                  <div key={job.id} style={{ display: "flex", justifyContent: "space-between", fontSize: 12 }}>
                    <span>{job.id}</span>
                    <span style={{ color: "#666" }}>
                      {job.next_run_time ? String(job.next_run_time).slice(5, 16).replace("T", " ") : "-"}
                    </span>
                  </div>
                ))}
              </div>
            ) : null}
          </div>
        </Space>
      </Card>
    );
  }

  function renderRightSidebar() {
    return (
      <div style={{ background: "#fafbfc", borderRadius: 8, padding: "12px 14px", border: "1px solid #eaecef" }}>
        <Space size={12} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
          {renderWatchlistCard()}
          {renderAlertsCard()}
          {renderSystemStatusCard()}
          {renderExecMetaCard()}
          {renderStepsCard()}
        </Space>
      </div>
    );
  }

  return (
    <div style={{ background: "#f5f6f8", borderRadius: 8, padding: "20px 24px" }}>
      <Space size={16} style={{ width: "100%", display: "flex", flexDirection: "column", alignItems: "stretch" }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
          <div>
            <Title level={3} style={{ marginBottom: 4 }}>Daily Workflow</Title>
            <Text type="secondary">TradePilot briefing 主展示，The-One 作为补充摘要与扩展分析。</Text>
          </div>
          <Space>
            <Button icon={<HistoryOutlined />} onClick={() => setHistoryOpen(true)}>历史记录</Button>
            <Button icon={<ReloadOutlined />} onClick={refreshData} loading={loading}>刷新</Button>
          </Space>
        </div>

        <Card size="small" style={{ boxShadow: "0 1px 4px rgba(0,0,0,0.06)", borderRadius: 8 }}>
          <Tabs
            activeKey={activePhase}
            onChange={(key) => setActivePhase(key as WorkflowPhase)}
            items={[
              { key: "pre_market", label: "盘前准备" },
              { key: "post_market", label: "盘后复盘" },
            ]}
            tabBarExtraContent={
              <Space>
                <Button
                  size="small"
                  type={activePhase === "pre_market" ? "primary" : "default"}
                  loading={runningPhase === "pre_market"}
                  onClick={() => handleRunWorkflow("pre_market")}
                >
                  运行盘前
                </Button>
                <Button
                  size="small"
                  type={activePhase === "post_market" ? "primary" : "default"}
                  loading={runningPhase === "post_market"}
                  onClick={() => handleRunWorkflow("post_market")}
                >
                  运行盘后
                </Button>
              </Space>
            }
          />

          <Row gutter={[16, 16]}>
            <Col xs={24} xl={16}>
              {renderLeftColumn()}
            </Col>
            <Col xs={24} xl={8}>
              {renderRightSidebar()}
            </Col>
          </Row>
        </Card>
      </Space>

      {renderHistoryDrawer()}
    </div>
  );
}
