import { useEffect, useState } from "react";
import { Card, Row, Col, Tabs, Tag, Table, Statistic, Space, Progress, Button } from "antd";
import { ArrowUpOutlined, ArrowDownOutlined } from "@ant-design/icons";
import { Line } from "@ant-design/charts";
import {
  getIndexDaily,
  getMarketSentiment,
  getSectorRotation,
  getPositions,
  getPlans,
  getStockDaily,
  runDailyScan,
  getLatestScan,
  getAlerts,
  markAlertRead,
  getSchedulerStatus,
  getSchedulerHistory,
} from "../../services/api";

const INDICES = [
  { code: "000001", name: "上证指数" },
  { code: "399001", name: "深证成指" },
  { code: "399006", name: "创业板指" },
  { code: "000688", name: "科创50" },
];

export default function Dashboard() {
  const [indexSummary, setIndexSummary] = useState<any[]>([]);
  const [indexData, setIndexData] = useState<any[]>([]);
  const [activeIndex, setActiveIndex] = useState("000001");
  const [sentiment, setSentiment] = useState<any>(null);
  const [sectors, setSectors] = useState<any>(null);
  const [positions, setPositions] = useState<any[]>([]);
  const [positionPrices, setPositionPrices] = useState<Record<string, number>>({});
  const [activePlans, setActivePlans] = useState<any[]>([]);
  const [scanData, setScanData] = useState<{scan_date: string | null; advice: any[]}>({scan_date: null, advice: []});
  const [alerts, setAlerts] = useState<any[]>([]);
  const [schedulerStatus, setSchedulerStatus] = useState<any>(null);
  const [schedulerHistory, setSchedulerHistory] = useState<any[]>([]);
  const [scanLoading, setScanLoading] = useState(false);
  const [readingAlertId, setReadingAlertId] = useState<number | null>(null);

  const refreshScanData = () => {
    getLatestScan().then(setScanData).catch(() => setScanData({ scan_date: null, advice: [] }));
    getAlerts().then(setAlerts).catch(() => setAlerts([]));
  };

  const refreshSchedulerData = () => {
    getSchedulerStatus().then(setSchedulerStatus).catch(() => setSchedulerStatus(null));
    getSchedulerHistory().then(setSchedulerHistory).catch(() => setSchedulerHistory([]));
  };

  // Load index summary cards
  useEffect(() => {
    Promise.all(
      INDICES.map((idx) =>
        getIndexDaily(idx.code).then((data) => {
          if (data.length < 2) return { ...idx, close: 0, change: 0, changePct: 0 };
          const last = data[data.length - 1];
          const prev = data[data.length - 2];
          return {
            ...idx,
            close: last.close,
            change: last.close - prev.close,
            changePct: ((last.close - prev.close) / prev.close) * 100,
            volume: last.volume,
          };
        })
      )
    ).then(setIndexSummary);
  }, []);

  // Load K-line for selected index
  useEffect(() => {
    getIndexDaily(activeIndex).then((d) => setIndexData(d.slice(-120)));
  }, [activeIndex]);

  // Load sentiment, sectors, positions, plans
  useEffect(() => {
    getMarketSentiment().then(setSentiment);
    getSectorRotation().then(setSectors);
    getPositions().then((pos) => {
      setPositions(pos);
      // Fetch current prices for each position
      const codes = [...new Set(pos.map((p: any) => p.stock_code))];
      Promise.all(
        codes.map((code: string) =>
          getStockDaily(code).then((data) => ({
            code,
            price: data.length > 0 ? data[data.length - 1].close : 0,
          }))
        )
      ).then((results) => {
        const prices: Record<string, number> = {};
        results.forEach((r) => (prices[r.code] = r.price));
        setPositionPrices(prices);
      });
    });
    getPlans("active").then(setActivePlans);
    refreshScanData();
    refreshSchedulerData();
  }, []);

  const upColor = "#cf1322";
  const downColor = "#3f8600";
  const pnlColor = (v: number) => (v >= 0 ? upColor : downColor);

  const sentimentColor = (label: string) =>
    ({ "过热": "#ff4d4f", "偏热": "#fa8c16", "中性": "#1890ff", "偏冷": "#52c41a" }[label] || "#1890ff");

  const actionColor = (action: string) =>
    ({ "建仓": "#52c41a", "关注": "#1890ff", "持有": "#8c8c8c", "减仓": "#fa8c16", "清仓": "#ff4d4f", "观望": "#d9d9d9" }[action] || "#8c8c8c");

  const urgencyColor = (urgency: string) =>
    ({ "立即": "#ff4d4f", "关注": "#fa8c16", "无需操作": "#d9d9d9" }[urgency] || "#8c8c8c");

  const alertTypeColor = (t: string) =>
    ({ stop_loss: "#ff4d4f", take_profit: "#fa8c16", watchlist_opportunity: "#1890ff", rotation: "#722ed1" }[t] || "#8c8c8c");

  const handleRunScan = async () => {
    setScanLoading(true);
    try {
      await runDailyScan();
      refreshScanData();
    } finally {
      setScanLoading(false);
    }
  };

  const handleMarkAlertRead = async (id: number) => {
    setReadingAlertId(id);
    try {
      await markAlertRead(id);
      refreshScanData();
    } finally {
      setReadingAlertId(null);
    }
  };

  return (
    <div>
      {/* Row 1: Index Summary Cards + Sentiment */}
      <Row gutter={[12, 12]}>
        {indexSummary.map((idx) => (
          <Col key={idx.code} flex="1">
            <Card size="small" hoverable onClick={() => setActiveIndex(idx.code)}
              style={{ borderLeft: activeIndex === idx.code ? "3px solid #1890ff" : undefined }}>
              <div style={{ fontSize: 12, color: "#666" }}>{idx.name}</div>
              <div style={{ fontSize: 20, fontWeight: "bold", color: pnlColor(idx.change) }}>
                {idx.close?.toFixed(2)}
              </div>
              <div style={{ fontSize: 12, color: pnlColor(idx.change) }}>
                {idx.change >= 0 ? "+" : ""}{idx.change?.toFixed(2)}{" "}
                ({idx.changePct >= 0 ? "+" : ""}{idx.changePct?.toFixed(2)}%)
              </div>
            </Card>
          </Col>
        ))}
        <Col flex="1">
          <Card size="small">
            <div style={{ fontSize: 12, color: "#666" }}>市场情绪</div>
            {sentiment ? (
              <>
                <div style={{ fontSize: 20, fontWeight: "bold", color: sentimentColor(sentiment.sentiment?.label) }}>
                  {sentiment.sentiment?.score?.toFixed(0)}
                </div>
                <Progress
                  percent={sentiment.sentiment?.score || 50}
                  showInfo={false}
                  strokeColor={sentimentColor(sentiment.sentiment?.label)}
                  size="small"
                />
                <div style={{ fontSize: 12 }}>
                  <Tag color={sentimentColor(sentiment.sentiment?.label)} style={{ margin: 0 }}>
                    {sentiment.sentiment?.label}
                  </Tag>
                </div>
              </>
            ) : <div style={{ color: "#999" }}>加载中...</div>}
          </Card>
        </Col>
      </Row>

      {/* Row 2: K-line + Fund Flow */}
      <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
        <Col span={14}>
          <Card title="大盘走势" size="small">
            <Tabs
              activeKey={activeIndex}
              onChange={setActiveIndex}
              items={INDICES.map((i) => ({ key: i.code, label: i.name }))}
              size="small"
            />
            {indexData.length > 0 && (
              <Line
                data={indexData.map((d: any) => ({ date: String(d.date).slice(0, 10), value: d.close }))}
                xField="date"
                yField="value"
                height={240}
                xAxis={{ tickCount: 6 }}
                smooth
              />
            )}
          </Card>
        </Col>
        <Col span={10}>
          <Card title="资金面" size="small">
            {sentiment ? (
              <Space direction="vertical" style={{ width: "100%" }} size="small">
                <Statistic
                  title="北向资金(近5日)"
                  value={sentiment.northbound?.net_5d ? (sentiment.northbound.net_5d / 1e8).toFixed(1) : 0}
                  suffix="亿"
                  valueStyle={{ color: pnlColor(sentiment.northbound?.net_5d || 0), fontSize: 16 }}
                  prefix={(sentiment.northbound?.net_5d || 0) >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
                />
                <Statistic
                  title="融资余额日变化"
                  value={sentiment.margin?.daily_change ? (sentiment.margin.daily_change / 1e8).toFixed(1) : 0}
                  suffix="亿"
                  valueStyle={{ color: pnlColor(sentiment.margin?.daily_change || 0), fontSize: 16 }}
                />
                <div>
                  <div style={{ color: "#666", fontSize: 12, marginBottom: 4 }}>ETF资金流(近5日)</div>
                  {sentiment.etf && Object.entries(sentiment.etf).map(([code, v]: any) => (
                    <div key={code} style={{ display: "flex", justifyContent: "space-between", fontSize: 13 }}>
                      <span>{code}</span>
                      <span style={{ color: pnlColor(v.net_5d) }}>{(v.net_5d / 1e8).toFixed(1)}亿</span>
                    </div>
                  ))}
                </div>
              </Space>
            ) : "加载中..."}
          </Card>
        </Col>
      </Row>

      {/* Row 2.5: Daily Scan + Alerts */}
      <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
        <Col span={14}>
          <Card
            title={scanData.scan_date ? `今日扫描建议 (${scanData.scan_date})` : "今日扫描建议"}
            size="small"
            extra={<Button size="small" type="primary" loading={scanLoading} onClick={handleRunScan}>运行扫描</Button>}
          >
            {scanData.advice.length > 0 ? (
              <Table
                dataSource={scanData.advice}
                rowKey="stock_code"
                size="small"
                pagination={false}
                columns={[
                  { title: "股票", dataIndex: "stock_name", width: 70 },
                  {
                    title: "操作",
                    dataIndex: "action",
                    width: 60,
                    render: (v: string) => <Tag color={actionColor(v)}>{v}</Tag>,
                  },
                  {
                    title: "紧迫",
                    dataIndex: "urgency",
                    width: 70,
                    render: (v: string) => <Tag color={urgencyColor(v)}>{v}</Tag>,
                  },
                  {
                    title: "评分",
                    dataIndex: "score",
                    width: 50,
                    render: (v: number) => v?.toFixed(0),
                    sorter: (a: any, b: any) => a.score - b.score,
                  },
                  {
                    title: "理由",
                    dataIndex: "reasons",
                    render: (v: string[] | string) => (
                      <span style={{ fontSize: 12, color: "#666" }}>{Array.isArray(v) ? v.slice(0, 2).join("；") : (v || "")}</span>
                    ),
                  },
                ]}
              />
            ) : <div style={{ color: "#999" }}>{scanData.scan_date ? "暂无扫描结果" : "尚未运行今日扫描"}</div>}
          </Card>
        </Col>
        <Col span={10}>
          <Card title="最新预警" size="small" extra={<Button size="small" onClick={refreshScanData}>刷新</Button>}>
            {alerts.length > 0 ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {alerts.slice(0, 8).map((alert: any) => (
                  <div key={alert.id} style={{
                    padding: "6px 8px",
                    background: alert.read_at ? "#fafafa" : "#fff",
                    borderRadius: 4,
                    borderLeft: `3px solid ${alertTypeColor(alert.alert_type)}`,
                    opacity: alert.read_at ? 0.6 : 1,
                  }}>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                      <span style={{ fontWeight: "bold", fontSize: 13 }}>
                        <Tag color={alertTypeColor(alert.alert_type)} style={{ marginRight: 4, fontSize: 11 }}>
                          {({ stop_loss: "风控", take_profit: "止盈", watchlist_opportunity: "机会", rotation: "轮动" } as any)[alert.alert_type] || alert.alert_type}
                        </Tag>
                        {alert.title}
                      </span>
                      {!alert.read_at && (
                        <Button
                          type="link"
                          size="small"
                          loading={readingAlertId === alert.id}
                          onClick={() => handleMarkAlertRead(alert.id)}
                        >
                          已读
                        </Button>
                      )}
                    </div>
                    {alert.message && (
                      <div style={{ fontSize: 12, color: "#666", marginTop: 2 }}>{alert.message}</div>
                    )}
                  </div>
                ))}
              </div>
            ) : <div style={{ color: "#999" }}>暂无预警</div>}
          </Card>
        </Col>
      </Row>

      {/* Row 3: Sectors + Portfolio & Plans */}
      <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
        <Col span={14}>
          <Card title="行业板块" size="small">
            {sectors?.sectors ? (
              <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
                {sectors.sectors
                  .slice()
                  .sort((a: any, b: any) => (b.change_1d || 0) - (a.change_1d || 0))
                  .map((s: any) => (
                    <Card
                      key={s.sector}
                      size="small"
                      style={{
                        width: "calc(20% - 8px)",
                        background: s.change_1d >= 0 ? "#fff1f0" : "#f6ffed",
                        borderColor: s.change_1d >= 0 ? "#ffa39e" : "#b7eb8f",
                      }}
                    >
                      <div style={{ fontWeight: "bold", fontSize: 12 }}>{s.sector}</div>
                      <div style={{ color: pnlColor(s.change_1d), fontSize: 14 }}>
                        {s.change_1d >= 0 ? "+" : ""}{s.change_1d?.toFixed(2)}%
                      </div>
                      <div style={{ fontSize: 10, color: "#999" }}>60日: {s.change_60d?.toFixed(1)}%</div>
                    </Card>
                  ))}
              </div>
            ) : "加载中..."}
            {sectors?.switch_suggestions?.length > 0 && (
              <div style={{ marginTop: 12 }}>
                <strong>高切低建议:</strong>
                {sectors.switch_suggestions.map((s: any, i: number) => (
                  <Tag key={i} color="blue" style={{ margin: 4 }}>{s.from_sector} → {s.to_sector}</Tag>
                ))}
              </div>
            )}
          </Card>
        </Col>
        <Col span={10}>
          <Card title="调度器状态" size="small" style={{ marginBottom: 12 }} extra={<Button size="small" onClick={refreshSchedulerData}>刷新</Button>}>
            <div style={{ marginBottom: 8 }}>
              <Tag color={schedulerStatus?.running ? "green" : "default"}>
                {schedulerStatus?.running ? "运行中" : "未运行"}
              </Tag>
            </div>
            {schedulerStatus?.jobs?.length ? (
              <div style={{ display: "flex", flexDirection: "column", gap: 6, fontSize: 12 }}>
                {schedulerStatus.jobs.map((job: any) => (
                  <div key={job.id} style={{ display: "flex", justifyContent: "space-between", gap: 8 }}>
                    <span>{job.id}</span>
                    <span style={{ color: "#666" }}>{job.next_run_time ? String(job.next_run_time).slice(5, 16).replace("T", " ") : "-"}</span>
                  </div>
                ))}
              </div>
            ) : <div style={{ color: "#999" }}>暂无调度任务</div>}
          </Card>

          {/* Portfolio P&L */}
          <Card title="持仓盈亏" size="small" style={{ marginBottom: 12 }}>
            {positions.length > 0 ? (
              <Table
                dataSource={positions}
                rowKey="id"
                size="small"
                pagination={false}
                columns={[
                  { title: "股票", dataIndex: "stock_name", width: 70 },
                  {
                    title: "现价",
                    width: 65,
                    render: (_: any, r: any) => positionPrices[r.stock_code]?.toFixed(2) ?? "-",
                  },
                  {
                    title: "盈亏%",
                    width: 70,
                    render: (_: any, r: any) => {
                      const cur = positionPrices[r.stock_code];
                      if (!cur || !r.buy_price) return "-";
                      const pct = ((cur - r.buy_price) / r.buy_price) * 100;
                      return <span style={{ color: pnlColor(pct), fontWeight: "bold" }}>
                        {pct >= 0 ? "+" : ""}{pct.toFixed(1)}%
                      </span>;
                    },
                  },
                  {
                    title: "盈亏额",
                    width: 80,
                    render: (_: any, r: any) => {
                      const cur = positionPrices[r.stock_code];
                      if (!cur || !r.buy_price || !r.quantity) return "-";
                      const pnl = (cur - r.buy_price) * r.quantity;
                      return <span style={{ color: pnlColor(pnl) }}>
                        {pnl >= 0 ? "+" : ""}{pnl.toFixed(0)}
                      </span>;
                    },
                  },
                ]}
              />
            ) : <div style={{ color: "#999" }}>暂无持仓</div>}
          </Card>

          {/* Active Trade Plans */}
          <Card title="活跃交易计划" size="small" style={{ marginBottom: 12 }}>
            {activePlans.length > 0 ? (
              <Table
                dataSource={activePlans}
                rowKey="id"
                size="small"
                pagination={false}
                columns={[
                  { title: "股票", dataIndex: "stock_name", width: 70 },
                  {
                    title: "止损",
                    dataIndex: "stop_loss_pct",
                    width: 55,
                    render: (v: number) => <span style={{ color: downColor }}>{v}%</span>,
                  },
                  {
                    title: "止盈",
                    dataIndex: "take_profit_pct",
                    width: 55,
                    render: (v: number) => <span style={{ color: upColor }}>+{v}%</span>,
                  },
                  {
                    title: "评分",
                    dataIndex: "composite_score",
                    width: 50,
                    render: (v: number) => v?.toFixed(0),
                  },
                ]}
              />
            ) : <div style={{ color: "#999" }}>暂无活跃计划</div>}
          </Card>

          <Card title="最近调度历史" size="small">
            {schedulerHistory.length > 0 ? (
              <Table
                dataSource={schedulerHistory}
                rowKey="id"
                size="small"
                pagination={false}
                columns={[
                  { title: "任务", dataIndex: "job_name", width: 80 },
                  {
                    title: "状态",
                    dataIndex: "status",
                    width: 70,
                    render: (v: string) => <Tag color={v === "success" ? "green" : v === "failed" ? "red" : "default"}>{v}</Tag>,
                  },
                  { title: "影响", dataIndex: "records_affected", width: 60 },
                  {
                    title: "时间",
                    dataIndex: "started_at",
                    render: (v: string) => <span style={{ fontSize: 12, color: "#666" }}>{String(v).slice(5, 16).replace("T", " ")}</span>,
                  },
                ]}
              />
            ) : <div style={{ color: "#999" }}>暂无运行历史</div>}
          </Card>
        </Col>
      </Row>
    </div>
  );
}
