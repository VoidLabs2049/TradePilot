import { BrowserRouter, Routes, Route, NavLink } from "react-router-dom";
import { Layout, Menu } from "antd";
import {
  DashboardOutlined,
  LineChartOutlined,
  AppstoreOutlined,
  FundOutlined,
  FileTextOutlined,
  StockOutlined,
} from "@ant-design/icons";
import MarketSummary from "./pages/MarketSummary";
import Dashboard from "./pages/Dashboard";
import StockAnalysis from "./pages/StockAnalysis";
import SectorMap from "./pages/SectorMap";
import Portfolio from "./pages/Portfolio";
import TradePlan from "./pages/TradePlan";

const { Sider, Content } = Layout;

const menuItems = [
  { key: "/", icon: <StockOutlined />, label: <NavLink to="/">市场概览</NavLink> },
  { key: "/dashboard", icon: <DashboardOutlined />, label: <NavLink to="/dashboard">仪表盘</NavLink> },
  { key: "/analysis", icon: <LineChartOutlined />, label: <NavLink to="/analysis">个股分析</NavLink> },
  { key: "/sectors", icon: <AppstoreOutlined />, label: <NavLink to="/sectors">行业地图</NavLink> },
  { key: "/portfolio", icon: <FundOutlined />, label: <NavLink to="/portfolio">持仓管理</NavLink> },
  { key: "/plans", icon: <FileTextOutlined />, label: <NavLink to="/plans">交易计划</NavLink> },
];

function App() {
  return (
    <BrowserRouter>
      <Layout style={{ minHeight: "100vh" }}>
        <Sider collapsible>
          <div style={{ color: "#fff", textAlign: "center", padding: "16px", fontSize: "18px", fontWeight: "bold" }}>
            TradePilot
          </div>
          <Menu theme="dark" mode="inline" defaultSelectedKeys={["/"]} items={menuItems} />
        </Sider>
        <Content style={{ padding: 24 }}>
          <Routes>
            <Route path="/" element={<MarketSummary />} />
            <Route path="/dashboard" element={<Dashboard />} />
            <Route path="/analysis" element={<StockAnalysis />} />
            <Route path="/sectors" element={<SectorMap />} />
            <Route path="/portfolio" element={<Portfolio />} />
            <Route path="/plans" element={<TradePlan />} />
          </Routes>
        </Content>
      </Layout>
    </BrowserRouter>
  );
}

export default App;
