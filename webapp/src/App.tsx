import { BrowserRouter, Routes, Route, NavLink, Navigate, useLocation } from "react-router-dom";
import { Layout, Menu } from "antd";
import {
  DashboardOutlined,
  ExperimentOutlined,
  FundOutlined,
} from "@ant-design/icons";
import Dashboard from "./pages/Dashboard";
import EtfAllWeather from "./pages/EtfAllWeather";
import Portfolio from "./pages/Portfolio";

const { Sider, Content } = Layout;

const menuItems = [
  { key: "/", icon: <DashboardOutlined />, label: <NavLink to="/">Daily Workflow</NavLink> },
  { key: "/etf-aw", icon: <ExperimentOutlined />, label: <NavLink to="/etf-aw">ETF 全天候</NavLink> },
  { key: "/portfolio", icon: <FundOutlined />, label: <NavLink to="/portfolio">持仓管理</NavLink> },
];

function AppShell() {
  const location = useLocation();

  return (
    <Layout style={{ minHeight: "100vh" }}>
        <Sider collapsible breakpoint="lg" collapsedWidth={0}>
          <div style={{ color: "#fff", textAlign: "center", padding: "16px", fontSize: "18px", fontWeight: "bold" }}>
            TradePilot
          </div>
          <Menu theme="dark" mode="inline" selectedKeys={[location.pathname]} items={menuItems} />
        </Sider>
        <Content style={{ padding: "clamp(12px, 2.5vw, 24px)", minWidth: 0 }}>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/etf-aw" element={<EtfAllWeather />} />
            <Route path="/portfolio" element={<Portfolio />} />
            <Route path="/dashboard" element={<Navigate to="/" replace />} />
            <Route path="/analysis" element={<Navigate to="/" replace />} />
            <Route path="/sectors" element={<Navigate to="/" replace />} />
            <Route path="/plans" element={<Navigate to="/" replace />} />
          </Routes>
        </Content>
    </Layout>
  );
}

function App() {
  return (
    <BrowserRouter>
      <AppShell />
    </BrowserRouter>
  );
}

export default App;
