import { Card, Typography } from "antd";

const { Paragraph } = Typography;

export default function TradePlan() {
  return (
    <Card title="交易计划已下线" size="small">
      <Paragraph style={{ marginBottom: 0 }}>
        交易计划模块已从当前主产品中移除，盘前操作框架与盘后明日准备已合并到 Daily Workflow。
      </Paragraph>
    </Card>
  );
}
