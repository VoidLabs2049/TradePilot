import { Card, Typography } from "antd";

const { Paragraph } = Typography;

export default function SectorMap() {
  return (
    <Card title="行业地图已下线" size="small">
      <Paragraph style={{ marginBottom: 0 }}>
        行业地图已从主产品中移除，相关观察已收口到 Daily Workflow 的板块定位中。
      </Paragraph>
    </Card>
  );
}
