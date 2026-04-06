import { BrowserRouter, Routes, Route, NavLink, Navigate } from "react-router-dom";
import { Layout, Menu } from "antd";
import {
  DashboardOutlined,
  FundOutlined,
} from "@ant-design/icons";
import Dashboard from "./pages/Dashboard";
import Portfolio from "./pages/Portfolio";

const { Sider, Content } = Layout;

const menuItems = [
  { key: "/", icon: <DashboardOutlined />, label: <NavLink to="/">Daily Workflow</NavLink> },
  { key: "/portfolio", icon: <FundOutlined />, label: <NavLink to="/portfolio">持仓管理</NavLink> },
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
            <Route path="/" element={<Dashboard />} />
            <Route path="/portfolio" element={<Portfolio />} />
            <Route path="/dashboard" element={<Navigate to="/" replace />} />
            <Route path="/analysis" element={<Navigate to="/" replace />} />
            <Route path="/sectors" element={<Navigate to="/" replace />} />
            <Route path="/plans" element={<Navigate to="/" replace />} />
          </Routes>
        </Content>
      </Layout>
    </BrowserRouter>
  );
}

export default App;
