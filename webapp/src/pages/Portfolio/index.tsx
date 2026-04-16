import { useEffect, useState } from "react";
import { Card, Table, Tag, Button, Modal, Form, Input, InputNumber, DatePicker, message } from "antd";
import { getPositions, addPosition, getTrades } from "../../services/api";

export default function Portfolio() {
  const [positions, setPositions] = useState<any[]>([]);
  const [trades, setTrades] = useState<any[]>([]);
  const [modalOpen, setModalOpen] = useState(false);
  const [form] = Form.useForm();

  const load = () => {
    getPositions().then(setPositions);
    getTrades().then(setTrades);
  };
  useEffect(load, []);

  const handleAdd = async (values: any) => {
    try {
      await addPosition({ ...values, buy_date: values.buy_date?.format("YYYY-MM-DD") });
      message.success("持仓已新增");
      setModalOpen(false);
      form.resetFields();
      load();
    } catch (error) {
      message.error("新增持仓失败，请检查后端日志");
      throw error;
    }
  };

  return (
    <div>
      <Card title="当前持仓" size="small" extra={<Button type="primary" size="small" onClick={() => setModalOpen(true)}>新增持仓</Button>} style={{ marginBottom: 16 }}>
        <Table
          dataSource={positions}
          rowKey="id"
          size="small"
          pagination={false}
          columns={[
            { title: "股票", dataIndex: "stock_name", width: 100 },
            { title: "代码", dataIndex: "stock_code", width: 80 },
            { title: "买入价", dataIndex: "buy_price", width: 80, render: (v: number) => v?.toFixed(2) },
            { title: "数量", dataIndex: "quantity", width: 60 },
            { title: "买入日期", dataIndex: "buy_date", width: 100 },
            { title: "状态", dataIndex: "status", width: 60, render: (v: string) => <Tag color={v === "open" ? "green" : "default"}>{v}</Tag> },
          ]}
        />
      </Card>

      <Card title="交易记录" size="small">
        <Table
          dataSource={trades}
          rowKey="id"
          size="small"
          pagination={{ pageSize: 10 }}
          columns={[
            { title: "日期", dataIndex: "date", width: 100 },
            { title: "股票", dataIndex: "stock_name", width: 100 },
            { title: "方向", dataIndex: "direction", width: 60, render: (v: string) => <Tag color={v === "buy" ? "red" : "green"}>{v === "buy" ? "买入" : "卖出"}</Tag> },
            { title: "价格", dataIndex: "price", width: 80, render: (v: number) => v?.toFixed(2) },
            { title: "数量", dataIndex: "quantity", width: 60 },
            { title: "理由", dataIndex: "reason" },
          ]}
        />
      </Card>

      <Modal title="新增持仓" open={modalOpen} onCancel={() => setModalOpen(false)} footer={null}>
        <Form form={form} onFinish={handleAdd} layout="vertical">
          <Form.Item name="stock_code" label="股票代码" rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="stock_name" label="股票名称" rules={[{ required: true }]}><Input /></Form.Item>
          <Form.Item name="buy_date" label="买入日期" rules={[{ required: true }]}><DatePicker style={{ width: "100%" }} /></Form.Item>
          <Form.Item name="buy_price" label="买入价" rules={[{ required: true }]}><InputNumber style={{ width: "100%" }} /></Form.Item>
          <Form.Item name="quantity" label="数量" rules={[{ required: true }]}><InputNumber style={{ width: "100%" }} /></Form.Item>
          <Button type="primary" htmlType="submit">确认</Button>
        </Form>
      </Modal>
    </div>
  );
}
