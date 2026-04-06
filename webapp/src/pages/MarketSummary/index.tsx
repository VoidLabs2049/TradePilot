import { Card, Typography } from "antd";

const { Paragraph } = Typography;

export default function MarketSummary() {
  return (
    <Card title="市场概览已下线" size="small">
      <Paragraph style={{ marginBottom: 0 }}>
        市场概览已并入 Daily Workflow，请返回首页查看盘前准备与盘后复盘。
      </Paragraph>
    </Card>
  );
}
