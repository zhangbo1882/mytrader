/**
 * 调整止损弹窗组件
 */
import React, { useState, useEffect } from 'react';
import { Modal, Form, InputNumber, Descriptions, Space, Tag, message } from 'antd';
import { LockOutlined, WarningOutlined } from '@ant-design/icons';
import type { PositionDetail, AdjustStopLossResponse } from '../../types/risk.types';
import { adjustStopLoss } from '../../services/riskService';

interface PositionEditorProps {
  visible: boolean;
  position: PositionDetail | null;
  onClose: () => void;
  onSuccess: (response: AdjustStopLossResponse) => void;
}

const PositionEditor: React.FC<PositionEditorProps> = ({
  visible,
  position,
  onClose,
  onSuccess
}) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState<AdjustStopLossResponse | null>(null);

  useEffect(() => {
    if (position && visible) {
      form.setFieldsValue({
        new_stop_loss_base: position.stop_loss_base,
        new_stop_loss_percent: position.stop_loss_percent,
      });
      calculatePreview();
    }
  }, [position, visible, form]);

  const calculatePreview = async () => {
    if (!position) return;

    const values = form.getFieldsValue();
    try {
      const response = await adjustStopLoss({
        position: {
          symbol: position.symbol,
          shares: position.shares,
          cost_price: position.cost_price,
          current_price: position.current_price,
          stop_loss_base: position.stop_loss_base,
          stop_loss_percent: position.stop_loss_percent,
        },
        new_stop_loss_base: values.new_stop_loss_base,
        new_stop_loss_percent: values.new_stop_loss_percent,
      });

      if (response.success) {
        setPreview(response);
      }
    } catch {
      // 忽略预览错误
    }
  };

  const handleOk = async () => {
    if (!position) return;

    try {
      const values = await form.validateFields();
      setLoading(true);

      const response = await adjustStopLoss({
        position: {
          symbol: position.symbol,
          shares: position.shares,
          cost_price: position.cost_price,
          current_price: position.current_price,
          stop_loss_base: position.stop_loss_base,
          stop_loss_percent: position.stop_loss_percent,
        },
        new_stop_loss_base: values.new_stop_loss_base,
        new_stop_loss_percent: values.new_stop_loss_percent,
      });

      if (response.success) {
        message.success('止损调整成功');
        onSuccess(response);
        onClose();
      } else {
        message.error(response.error || '调整失败');
      }
    } catch (error) {
      message.error('调整失败');
    } finally {
      setLoading(false);
    }
  };

  const handleValuesChange = () => {
    calculatePreview();
  };

  if (!position) return null;

  return (
    <Modal
      title={
        <Space>
          <LockOutlined />
          调整止损 - {position.symbol}
        </Space>
      }
      open={visible}
      onCancel={onClose}
      onOk={handleOk}
      confirmLoading={loading}
      okText="确认调整"
      cancelText="取消"
      width={500}
    >
      <Form
        form={form}
        layout="vertical"
        onValuesChange={handleValuesChange}
      >
        <Descriptions column={2} size="small" style={{ marginBottom: 16 }}>
          <Descriptions.Item label="成本价">¥{position.cost_price.toFixed(2)}（不变）</Descriptions.Item>
          <Descriptions.Item label="当前价">¥{position.current_price.toFixed(2)}</Descriptions.Item>
          <Descriptions.Item label="持仓数量">{position.shares.toLocaleString()} 股</Descriptions.Item>
          <Descriptions.Item label="原止损价">¥{position.stop_loss_price.toFixed(2)}</Descriptions.Item>
        </Descriptions>

        <Form.Item
          name="new_stop_loss_base"
          label="新止损基数"
          rules={[{ required: true, message: '请输入止损基数' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0}
            precision={2}
            placeholder="止损基数价格"
          />
        </Form.Item>

        <Form.Item
          name="new_stop_loss_percent"
          label="止损比例 (%)"
          rules={[{ required: true, message: '请输入止损比例' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0}
            max={100}
            precision={2}
            placeholder="止损比例"
          />
        </Form.Item>
      </Form>

      {preview && (
        <Descriptions
          title="调整效果预览"
          column={2}
          size="small"
          bordered
        >
          <Descriptions.Item label="原风险占用">
            ¥{preview.adjustment.old_risk.toLocaleString()}
          </Descriptions.Item>
          <Descriptions.Item label="新风险占用">
            ¥{preview.adjustment.new_risk.toLocaleString()}
          </Descriptions.Item>
          <Descriptions.Item label="释放风险">
            {preview.adjustment.released_risk >= 0 ? (
              <Tag color="green">+¥{preview.adjustment.released_risk.toLocaleString()}</Tag>
            ) : (
              <Tag color="red">-¥{Math.abs(preview.adjustment.released_risk).toLocaleString()}</Tag>
            )}
          </Descriptions.Item>
          <Descriptions.Item label="新止损价">
            ¥{preview.adjustment.new_stop_loss_price.toFixed(2)}
          </Descriptions.Item>
          <Descriptions.Item label="锁定利润" span={2}>
            {preview.adjustment.locked_profit > 0 ? (
              <Tag color="green" icon={<LockOutlined />}>
                +¥{preview.adjustment.locked_profit.toLocaleString()}
              </Tag>
            ) : (
              <Tag color="red" icon={<WarningOutlined />}>
                {preview.adjustment.locked_profit.toLocaleString()}
              </Tag>
            )}
          </Descriptions.Item>
        </Descriptions>
      )}
    </Modal>
  );
};

export default PositionEditor;
