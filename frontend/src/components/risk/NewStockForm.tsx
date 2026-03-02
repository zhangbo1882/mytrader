/**
 * 新股买入表单组件
 */
import React from 'react';
import { Form, InputNumber, Button, Space, Card } from 'antd';
import { CalculatorOutlined } from '@ant-design/icons';

interface NewStockFormProps {
  loading: boolean;
  onCalculate: (buyPrice: number, stopLossPercent: number) => void;
}

const NewStockForm: React.FC<NewStockFormProps> = ({
  loading,
  onCalculate
}) => {
  const [form] = Form.useForm();

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      onCalculate(values.buy_price, values.stop_loss_percent);
    } catch {
      // 验证失败
    }
  };

  return (
    <Card title="新股买入计算" size="small">
      <Form form={form} layout="vertical">
        <Form.Item
          name="buy_price"
          label="买入价格"
          rules={[{ required: true, message: '请输入买入价格' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0.01}
            precision={2}
            placeholder="买入价格"
          />
        </Form.Item>

        <Form.Item
          name="stop_loss_percent"
          label="止损比例 (%)"
          rules={[{ required: true, message: '请输入止损比例' }]}
          initialValue={5}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0.1}
            max={50}
            precision={1}
            placeholder="止损比例"
          />
        </Form.Item>

        <Form.Item>
          <Space>
            <Button
              type="primary"
              icon={<CalculatorOutlined />}
              loading={loading}
              onClick={handleSubmit}
            >
              计算
            </Button>
            <Button onClick={() => form.resetFields()}>
              重置
            </Button>
          </Space>
        </Form.Item>
      </Form>
    </Card>
  );
};

export default NewStockForm;
