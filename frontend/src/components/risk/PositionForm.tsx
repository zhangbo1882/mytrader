/**
 * 添加持仓表单组件
 * 持仓添加后，现价会从行情数据自动获取
 */
import React from 'react';
import { Modal, Form, Input, InputNumber, message } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { addPositionToDb } from '../../services/riskService';

interface PositionFormProps {
  visible: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

const PositionForm: React.FC<PositionFormProps> = ({
  visible,
  onClose,
  onSuccess
}) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = React.useState(false);

  const handleOk = async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);

      // 使用新的数据库持久化 API，不需要传 current_price
      const response = await addPositionToDb({
        symbol: values.symbol,
        shares: values.shares,
        cost_price: values.cost_price,
        stop_loss_base: values.stop_loss_base || values.cost_price,
        stop_loss_percent: values.stop_loss_percent,
      });

      if (response.success) {
        message.success('持仓添加成功');
        form.resetFields();
        onSuccess();
        onClose();
      } else {
        message.error(response.error || '添加失败');
      }
    } catch (error) {
      message.error('添加失败');
    } finally {
      setLoading(false);
    }
  };

  const handleClose = () => {
    form.resetFields();
    onClose();
  };

  return (
    <Modal
      title={<><PlusOutlined /> 添加持仓</>}
      open={visible}
      onCancel={handleClose}
      onOk={handleOk}
      confirmLoading={loading}
      okText="添加"
      cancelText="取消"
      width={450}
    >
      <Form
        form={form}
        layout="vertical"
      >
        <Form.Item
          name="symbol"
          label="股票代码"
          rules={[{ required: true, message: '请输入股票代码' }]}
        >
          <Input placeholder="例如：600382" />
        </Form.Item>

        <Form.Item
          name="shares"
          label="股数"
          rules={[{ required: true, message: '请输入股数' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={100}
            step={100}
            placeholder="股数（100的倍数）"
          />
        </Form.Item>

        <Form.Item
          name="cost_price"
          label="成本价"
          rules={[{ required: true, message: '请输入成本价' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0.01}
            precision={2}
            placeholder="买入成本价"
          />
        </Form.Item>

        <Form.Item
          name="stop_loss_base"
          label="止损基数"
          extra="默认等于成本价，盈利后可调整为更高价格"
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0.01}
            precision={2}
            placeholder="止损基数（可选）"
          />
        </Form.Item>

        <Form.Item
          name="stop_loss_percent"
          label="止损比例 (%)"
          rules={[{ required: true, message: '请输入止损比例' }]}
          initialValue={8}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0.1}
            max={50}
            precision={1}
            placeholder="止损比例"
          />
        </Form.Item>
      </Form>
    </Modal>
  );
};

export default PositionForm;
