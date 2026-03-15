import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Card,
  Col,
  Row,
  Table,
  Tag,
  Statistic,
  Switch,
  Radio,
  Badge,
  Space,
  Typography,
  Alert,
  InputNumber,
  Button,
  Modal,
  Input,
  Form,
  Divider,
  Tooltip,
} from "antd";
import {
  ArrowUpOutlined,
  ArrowDownOutlined,
  ReloadOutlined,
  SettingOutlined,
  PlusOutlined,
  DeleteOutlined,
} from "@ant-design/icons";
import {
  getDailySummary,
  get5mBrief,
  getTradingStatus,
  getWatchlist,
  updateWatchlist,
} from "../../services/api";

const { Title, Text } = Typography;

const upColor = "#cf1322";
const downColor = "#3f8600";
const pnlColor = (v: number) => (v >= 0 ? upColor : downColor);

// ---------------------------------------------------------------------------
// useInterval hook
// ---------------------------------------------------------------------------

function useInterval(callback: () => void, delay: number | null) {
  const savedCallback = useRef(callback);
  useEffect(() => {
    savedCallback.current = callback;
  });
  useEffect(() => {
    if (delay === null) return;
    const id = setInterval(() => savedCallback.current(), delay);
    return () => clearInterval(id);
  }, [delay]);
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function MarketSummary() {
  const [mode, setMode] = useState<"daily" | "5m">("daily");
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [loading, setLoading] = useState(false);
  const [lastUpdate, setLastUpdate] = useState("");

  // Trading status
  const [tradingStatus, setTradingStatus] = useState<any>(null);

  // Daily data
  const [dailyData, setDailyData] = useState<any>(null);
  const [industryTopN, setIndustryTopN] = useState(10);
  const [industryBottomN, setIndustryBottomN] = useState(10);
  const [conceptTopN, setConceptTopN] = useState(15);
  const [conceptBottomN, setConceptBottomN] = useState(15);

  // 5m data
  const [briefData, setBriefData] = useState<any>(null);

  // Watchlist
  const [watchlistModalOpen, setWatchlistModalOpen] = useState(false);
  const [watchlist, setWatchlist] = useState<any>({ watch_sectors: [], watch_stocks: [] });
  const [sectorInput, setSectorInput] = useState("");
  const [stockCodeInput, setStockCodeInput] = useState("");
  const [stockNameInput, setStockNameInput] = useState("");

  // Computed refresh interval
  const refreshDelay = useMemo(() => {
    if (!autoRefresh) return null;
    if (!tradingStatus) return null;
    if (tradingStatus.is_trading) return 5 * 60 * 1000; // 5 min
    return null; // stop when not trading
  }, [autoRefresh, tradingStatus]);

  // Fetch trading status
  const fetchStatus = useCallback(async () => {
    try {
      const status = await getTradingStatus();
      setTradingStatus(status);
    } catch {
      // ignore
    }
  }, []);

  // Fetch data based on mode
  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      if (mode === "daily") {
        const data = await getDailySummary({
          industry_top_n: industryTopN,
          industry_bottom_n: industryBottomN,
          concept_top_n: conceptTopN,
          concept_bottom_n: conceptBottomN,
        });
        setDailyData(data);
        setLastUpdate(data.timestamp);
      } else {
        const data = await get5mBrief();
        setBriefData(data);
        setLastUpdate(data.timestamp);
      }
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  }, [mode, industryTopN, industryBottomN, conceptTopN, conceptBottomN]);

  // Initial load
  useEffect(() => {
    fetchStatus();
    fetchData();
  }, [fetchStatus, fetchData]);

  // Check trading status every minute
  useInterval(fetchStatus, 60 * 1000);
  // Auto-refresh data
  useInterval(fetchData, refreshDelay);

  // Load watchlist
  useEffect(() => {
    getWatchlist().then(setWatchlist).catch(() => {});
  }, []);

  // Watchlist management
  const addSector = () => {
    if (!sectorInput.trim()) return;
    const updated = {
      ...watchlist,
      watch_sectors: [...watchlist.watch_sectors, sectorInput.trim()],
    };
    setWatchlist(updated);
    setSectorInput("");
  };

  const removeSector = (idx: number) => {
    const updated = {
      ...watchlist,
      watch_sectors: watchlist.watch_sectors.filter((_: any, i: number) => i !== idx),
    };
    setWatchlist(updated);
  };

  const addStock = () => {
    if (!stockCodeInput.trim()) return;
    const updated = {
      ...watchlist,
      watch_stocks: [
        ...watchlist.watch_stocks,
        { code: stockCodeInput.trim(), name: stockNameInput.trim() },
      ],
    };
    setWatchlist(updated);
    setStockCodeInput("");
    setStockNameInput("");
  };

  const removeStock = (idx: number) => {
    const updated = {
      ...watchlist,
      watch_stocks: watchlist.watch_stocks.filter((_: any, i: number) => i !== idx),
    };
    setWatchlist(updated);
  };

  const saveWatchlist = async () => {
    await updateWatchlist(watchlist);
    setWatchlistModalOpen(false);
    if (mode === "5m") fetchData();
  };

  // Table columns
  const indexColumns = [
    { title: "指数", dataIndex: "name", key: "name" },
    {
      title: "收盘",
      dataIndex: "close",
      key: "close",
      render: (v: number) => v.toFixed(2),
    },
    {
      title: "涨跌幅",
      dataIndex: "change_pct",
      key: "change_pct",
      render: (v: number) => (
        <span style={{ color: pnlColor(v) }}>
          {v >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
          {" "}{Math.abs(v).toFixed(2)}%
        </span>
      ),
    },
    {
      title: "成交额",
      dataIndex: "turnover",
      key: "turnover",
      render: (v: number) => `${(v / 1e8).toFixed(0)}亿`,
    },
  ];

  const sectorColumns = [
    { title: "#", key: "idx", render: (_: any, __: any, i: number) => i + 1, width: 40 },
    { title: "板块", dataIndex: "name", key: "name" },
    {
      title: "涨跌幅",
      dataIndex: "change_pct",
      key: "change_pct",
      render: (v: number) => (
        <span style={{ color: pnlColor(v) }}>{v >= 0 ? "+" : ""}{v.toFixed(2)}%</span>
      ),
    },
    {
      title: "涨/跌",
      key: "updown",
      render: (_: any, r: any) => `${r.up_count}/${r.down_count}`,
    },
    { title: "领涨股", dataIndex: "leader", key: "leader" },
  ];

  const stockColumns = [
    { title: "#", key: "idx", render: (_: any, __: any, i: number) => i + 1, width: 40 },
    { title: "股票", dataIndex: "name", key: "name" },
    { title: "代码", dataIndex: "code", key: "code" },
    {
      title: "涨跌幅",
      dataIndex: "change_pct",
      key: "change_pct",
      render: (v: number) => (
        <span style={{ color: pnlColor(v) }}>{v >= 0 ? "+" : ""}{v.toFixed(2)}%</span>
      ),
    },
  ];

  const watchSectorColumns = [
    { title: "板块", dataIndex: "name", key: "name" },
    { title: "匹配", dataIndex: "matched_name", key: "matched_name" },
    {
      title: "涨跌幅",
      dataIndex: "change_pct",
      key: "change_pct",
      render: (v: number) => (
        <span style={{ color: pnlColor(v) }}>{v >= 0 ? "+" : ""}{v.toFixed(2)}%</span>
      ),
    },
    {
      title: "涨/跌",
      key: "updown",
      render: (_: any, r: any) => `${r.up_count}/${r.down_count}`,
    },
    {
      title: "强度",
      dataIndex: "strength",
      key: "strength",
      render: (v: number) => v.toFixed(2),
    },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      render: (v: string) => {
        const colorMap: Record<string, string> = {
          strong: "red",
          weak: "green",
          neutral: "default",
          missing: "orange",
        };
        return <Tag color={colorMap[v] || "default"}>{v}</Tag>;
      },
    },
  ];

  const watchStockColumns = [
    { title: "股票", dataIndex: "name", key: "name" },
    { title: "代码", dataIndex: "code", key: "code" },
    {
      title: "现价",
      dataIndex: "price",
      key: "price",
      render: (v: number) => v.toFixed(2),
    },
    {
      title: "涨跌幅",
      dataIndex: "change_pct",
      key: "change_pct",
      render: (v: number) => (
        <span style={{ color: pnlColor(v) }}>{v >= 0 ? "+" : ""}{v.toFixed(2)}%</span>
      ),
    },
    {
      title: "换手率",
      dataIndex: "turnover_rate",
      key: "turnover_rate",
      render: (v: number) => `${v.toFixed(2)}%`,
    },
    {
      title: "量比",
      dataIndex: "volume_ratio",
      key: "volume_ratio",
      render: (v: number) => v.toFixed(2),
    },
    {
      title: "状态",
      dataIndex: "status",
      key: "status",
      render: (v: string) => {
        const colorMap: Record<string, string> = {
          breakout: "red",
          breakdown: "green",
          active: "blue",
          watch: "default",
          missing: "orange",
        };
        return <Tag color={colorMap[v] || "default"}>{v}</Tag>;
      },
    },
  ];

  // Regime badge
  const regimeColor = (label: string) => {
    if (label === "risk_on") return "#cf1322";
    if (label === "risk_off") return "#3f8600";
    return "#faad14";
  };

  const regimeText = (label: string) => {
    if (label === "risk_on") return "偏多";
    if (label === "risk_off") return "偏空";
    return "中性";
  };

  // Trading status indicator
  const statusBadge = tradingStatus ? (
    <Badge
      status={tradingStatus.is_trading ? "processing" : "default"}
      text={
        <Text type={tradingStatus.is_trading ? "success" : "secondary"}>
          {tradingStatus.message}
        </Text>
      }
    />
  ) : null;

  return (
    <div>
      {/* Header bar */}
      <Row justify="space-between" align="middle" style={{ marginBottom: 16 }}>
        <Col>
          <Space size="large">
            <Title level={4} style={{ margin: 0 }}>市场概览</Title>
            {statusBadge}
            {lastUpdate && (
              <Text type="secondary">更新: {lastUpdate}</Text>
            )}
          </Space>
        </Col>
        <Col>
          <Space>
            <Radio.Group value={mode} onChange={(e) => setMode(e.target.value)} size="small">
              <Radio.Button value="daily">日报</Radio.Button>
              <Radio.Button value="5m">5分钟</Radio.Button>
            </Radio.Group>
            <Tooltip title="自动刷新（交易时段内每5分钟）">
              <Switch
                checked={autoRefresh}
                onChange={setAutoRefresh}
                checkedChildren="自动"
                unCheckedChildren="手动"
                size="small"
              />
            </Tooltip>
            <Button
              icon={<ReloadOutlined />}
              size="small"
              onClick={fetchData}
              loading={loading}
            >
              刷新
            </Button>
            {mode === "5m" && (
              <Button
                icon={<SettingOutlined />}
                size="small"
                onClick={() => setWatchlistModalOpen(true)}
              >
                关注列表
              </Button>
            )}
          </Space>
        </Col>
      </Row>

      {/* Daily mode */}
      {mode === "daily" && dailyData && (
        <>
          {/* Index cards */}
          <Row gutter={[12, 12]} style={{ marginBottom: 16 }}>
            {dailyData.indices.map((idx: any) => (
              <Col key={idx.code} xs={12} sm={8} md={4}>
                <Card size="small">
                  <Statistic
                    title={idx.name}
                    value={idx.close}
                    precision={2}
                    valueStyle={{ color: pnlColor(idx.change_pct), fontSize: 16 }}
                    prefix={idx.change_pct >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
                    suffix={
                      <span style={{ fontSize: 12 }}>
                        {idx.change_pct >= 0 ? "+" : ""}{idx.change_pct.toFixed(2)}%
                      </span>
                    }
                  />
                </Card>
              </Col>
            ))}
          </Row>

          {/* Breadth */}
          <Card size="small" title="市场情绪" style={{ marginBottom: 16 }}>
            <Row gutter={16}>
              <Col span={4}>
                <Statistic title="上涨" value={dailyData.breadth.up} suffix="家" valueStyle={{ color: upColor }} />
              </Col>
              <Col span={4}>
                <Statistic title="下跌" value={dailyData.breadth.down} suffix="家" valueStyle={{ color: downColor }} />
              </Col>
              <Col span={4}>
                <Statistic title="平盘" value={dailyData.breadth.flat} suffix="家" />
              </Col>
              <Col span={4}>
                <Statistic title="涨停" value={dailyData.breadth.limit_up} suffix="家" valueStyle={{ color: upColor }} />
              </Col>
              <Col span={4}>
                <Statistic title="跌停" value={dailyData.breadth.limit_down} suffix="家" valueStyle={{ color: downColor }} />
              </Col>
              <Col span={4}>
                <Statistic
                  title="涨跌比"
                  value={dailyData.breadth.up > 0 ? `1:${(dailyData.breadth.down / dailyData.breadth.up).toFixed(1)}` : "N/A"}
                />
              </Col>
            </Row>
          </Card>

          {/* Sectors */}
          <Row gutter={16} style={{ marginBottom: 16 }}>
            <Col span={12}>
              <Card
                size="small"
                title="领涨行业"
                extra={
                  <Space size="small">
                    <Text type="secondary">显示</Text>
                    <InputNumber size="small" min={1} max={50} value={industryTopN} onChange={(v) => v && setIndustryTopN(v)} style={{ width: 60 }} />
                  </Space>
                }
              >
                <Table
                  dataSource={dailyData.industry_top}
                  columns={sectorColumns}
                  rowKey="code"
                  size="small"
                  pagination={false}
                />
              </Card>
            </Col>
            <Col span={12}>
              <Card
                size="small"
                title="领跌行业"
                extra={
                  <Space size="small">
                    <Text type="secondary">显示</Text>
                    <InputNumber size="small" min={1} max={50} value={industryBottomN} onChange={(v) => v && setIndustryBottomN(v)} style={{ width: 60 }} />
                  </Space>
                }
              >
                <Table
                  dataSource={dailyData.industry_bottom}
                  columns={sectorColumns}
                  rowKey="code"
                  size="small"
                  pagination={false}
                />
              </Card>
            </Col>
          </Row>

          <Row gutter={16} style={{ marginBottom: 16 }}>
            <Col span={12}>
              <Card
                size="small"
                title="领涨概念"
                extra={
                  <Space size="small">
                    <Text type="secondary">显示</Text>
                    <InputNumber size="small" min={1} max={50} value={conceptTopN} onChange={(v) => v && setConceptTopN(v)} style={{ width: 60 }} />
                  </Space>
                }
              >
                <Table
                  dataSource={dailyData.concept_top}
                  columns={sectorColumns}
                  rowKey="code"
                  size="small"
                  pagination={false}
                />
              </Card>
            </Col>
            <Col span={12}>
              <Card
                size="small"
                title="领跌概念"
                extra={
                  <Space size="small">
                    <Text type="secondary">显示</Text>
                    <InputNumber size="small" min={1} max={50} value={conceptBottomN} onChange={(v) => v && setConceptBottomN(v)} style={{ width: 60 }} />
                  </Space>
                }
              >
                <Table
                  dataSource={dailyData.concept_bottom}
                  columns={sectorColumns}
                  rowKey="code"
                  size="small"
                  pagination={false}
                />
              </Card>
            </Col>
          </Row>

          {/* Top/bottom stocks */}
          <Row gutter={16}>
            <Col span={12}>
              <Card size="small" title="涨幅前10">
                <Table
                  dataSource={dailyData.stocks_top}
                  columns={stockColumns}
                  rowKey="code"
                  size="small"
                  pagination={false}
                />
              </Card>
            </Col>
            <Col span={12}>
              <Card size="small" title="跌幅前10">
                <Table
                  dataSource={dailyData.stocks_bottom}
                  columns={stockColumns}
                  rowKey="code"
                  size="small"
                  pagination={false}
                />
              </Card>
            </Col>
          </Row>
        </>
      )}

      {/* 5m mode */}
      {mode === "5m" && briefData && (
        <>
          {/* Regime */}
          <Card size="small" title="市场状态" style={{ marginBottom: 16 }}>
            <Row gutter={16} align="middle">
              <Col>
                <Tag
                  color={regimeColor(briefData.regime.label)}
                  style={{ fontSize: 18, padding: "4px 16px" }}
                >
                  {regimeText(briefData.regime.label)}
                </Tag>
              </Col>
              <Col>
                <Statistic
                  title="Regime Score"
                  value={briefData.regime.score}
                  precision={2}
                  valueStyle={{ color: pnlColor(briefData.regime.score) }}
                />
              </Col>
              <Col>
                <Space direction="vertical" size={0}>
                  <Text type="secondary">
                    沪深300: {briefData.regime.drivers.hs300_change_pct >= 0 ? "+" : ""}
                    {briefData.regime.drivers.hs300_change_pct.toFixed(2)}%
                  </Text>
                  <Text type="secondary">
                    创业板: {briefData.regime.drivers.cyb_change_pct >= 0 ? "+" : ""}
                    {briefData.regime.drivers.cyb_change_pct.toFixed(2)}%
                  </Text>
                  <Text type="secondary">
                    涨跌家差: {briefData.regime.drivers.up_down_diff}
                  </Text>
                </Space>
              </Col>
            </Row>
          </Card>

          {/* Alerts */}
          {briefData.alerts.length > 0 && (
            <div style={{ marginBottom: 16 }}>
              {briefData.alerts.map((alert: string, i: number) => (
                <Alert
                  key={i}
                  message={alert}
                  type={alert.includes("偏强") || alert.includes("走强") || alert.includes("breakout") ? "error" : alert.includes("偏弱") || alert.includes("走弱") || alert.includes("breakdown") ? "success" : "info"}
                  showIcon
                  style={{ marginBottom: 8 }}
                />
              ))}
            </div>
          )}

          {/* Watchlists */}
          <Row gutter={16}>
            <Col span={24} style={{ marginBottom: 16 }}>
              <Card size="small" title="关注板块">
                <Table
                  dataSource={briefData.sector_watchlist}
                  columns={watchSectorColumns}
                  rowKey="name"
                  size="small"
                  pagination={false}
                />
              </Card>
            </Col>
            <Col span={24}>
              <Card size="small" title="关注个股">
                <Table
                  dataSource={briefData.stock_watchlist}
                  columns={watchStockColumns}
                  rowKey="code"
                  size="small"
                  pagination={false}
                />
              </Card>
            </Col>
          </Row>
        </>
      )}

      {/* Loading state */}
      {loading && !dailyData && !briefData && (
        <Card>
          <Text type="secondary">加载中...</Text>
        </Card>
      )}

      {/* Watchlist edit modal */}
      <Modal
        title="编辑关注列表"
        open={watchlistModalOpen}
        onOk={saveWatchlist}
        onCancel={() => setWatchlistModalOpen(false)}
        width={600}
      >
        <Divider orientation="left">关注板块</Divider>
        <Space direction="vertical" style={{ width: "100%" }}>
          {watchlist.watch_sectors.map((s: string, i: number) => (
            <Space key={i}>
              <Tag>{s}</Tag>
              <Button
                type="text"
                danger
                size="small"
                icon={<DeleteOutlined />}
                onClick={() => removeSector(i)}
              />
            </Space>
          ))}
          <Space>
            <Input
              placeholder="板块名称"
              value={sectorInput}
              onChange={(e) => setSectorInput(e.target.value)}
              onPressEnter={addSector}
              style={{ width: 200 }}
            />
            <Button icon={<PlusOutlined />} onClick={addSector}>添加</Button>
          </Space>
        </Space>

        <Divider orientation="left">关注个股</Divider>
        <Space direction="vertical" style={{ width: "100%" }}>
          {watchlist.watch_stocks.map((s: any, i: number) => (
            <Space key={i}>
              <Tag>{s.code} {s.name}</Tag>
              <Button
                type="text"
                danger
                size="small"
                icon={<DeleteOutlined />}
                onClick={() => removeStock(i)}
              />
            </Space>
          ))}
          <Space>
            <Input
              placeholder="股票代码"
              value={stockCodeInput}
              onChange={(e) => setStockCodeInput(e.target.value)}
              style={{ width: 120 }}
            />
            <Input
              placeholder="股票名称"
              value={stockNameInput}
              onChange={(e) => setStockNameInput(e.target.value)}
              onPressEnter={addStock}
              style={{ width: 120 }}
            />
            <Button icon={<PlusOutlined />} onClick={addStock}>添加</Button>
          </Space>
        </Space>
      </Modal>
    </div>
  );
}
