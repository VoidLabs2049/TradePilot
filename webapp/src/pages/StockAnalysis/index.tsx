import { Card, Typography } from "antd";

const { Paragraph } = Typography;

export default function StockAnalysis() {
  return (
    <Card title="个股分析已下线" size="small">
      <Paragraph style={{ marginBottom: 0 }}>
        个股分析不再作为主产品入口，当前版本请围绕 Daily Workflow 管理观察池与持仓健康度。
      </Paragraph>
    </Card>
  );
}
