/**
 * 编辑持仓弹窗组件
 * 支持编辑：股数、成本价、止损基数、止损比例
 */
import React, { useState, useEffect } from 'react';
import { Modal, Form, InputNumber, Descriptions, Space, Tag, message, Divider } from 'antd';
import { LockOutlined, WarningOutlined, EditOutlined } from '@ant-design/icons';
import type { PositionDetail, AdjustStopLossResponse } from '../../types/risk.types';
import { adjustStopLoss, updatePositionInDb } from '../../services/riskService';

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
        shares: position.shares,
        cost_price: position.cost_price,
        new_stop_loss_base: position.stop_loss_base,
        new_stop_loss_percent: position.stop_loss_percent,
      });
      calculatePreview();
    }
  }, [position, visible, form]);

  const calculatePreview = async () => {
    if (!position) return;

    const values = form.getFieldsValue();
    // 只有止损相关字段变化时才计算预览
    if (values.new_stop_loss_base !== undefined && values.new_stop_loss_percent !== undefined) {
      try {
        const response = await adjustStopLoss({
          position: {
            symbol: position.symbol,
            shares: values.shares ?? position.shares,
            cost_price: values.cost_price ?? position.cost_price,
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
    }
  };

  const handleOk = async () => {
    if (!position) return;

    try {
      const values = await form.validateFields();
      setLoading(true);

      // 检查是否有股数或成本价变化
      const hasSharesChange = values.shares !== position.shares;
      const hasCostPriceChange = values.cost_price !== position.cost_price;
      const hasStopLossChange = values.new_stop_loss_base !== position.stop_loss_base ||
                                 values.new_stop_loss_percent !== position.stop_loss_percent;

      // 只要有任何变化，就更新数据库
      if (hasSharesChange || hasCostPriceChange || hasStopLossChange) {
        const updateResult = await updatePositionInDb(position.symbol, {
          shares: values.shares,
          cost_price: values.cost_price,
          stop_loss_base: values.new_stop_loss_base,
          stop_loss_percent: values.new_stop_loss_percent,
        });

        if (!updateResult.success) {
          message.error(updateResult.error || '更新持仓失败');
          setLoading(false);
          return;
        }
      }

      // 如果有止损变化，调用止损调整API获取预览结果用于显示
      if (hasStopLossChange) {
        const response = await adjustStopLoss({
          position: {
            symbol: position.symbol,
            shares: values.shares,
            cost_price: values.cost_price,
            current_price: position.current_price,
            stop_loss_base: position.stop_loss_base,
            stop_loss_percent: position.stop_loss_percent,
          },
          new_stop_loss_base: values.new_stop_loss_base,
          new_stop_loss_percent: values.new_stop_loss_percent,
        });

        if (response.success) {
          message.success('持仓更新成功');
          onSuccess(response);
          onClose();
        } else {
          message.error(response.error || '更新失败');
        }
      } else if (hasSharesChange || hasCostPriceChange) {
        // 只有股数或成本价变化，没有止损变化
        message.success('持仓更新成功');
        // 触发刷新
        onSuccess({
          success: true,
          position: {
            ...position,
            shares: values.shares,
            cost_price: values.cost_price,
          },
          adjustment: {
            old_risk: position.total_risk,
            new_risk: position.total_risk,
            released_risk: 0,
            old_stop_loss_price: position.stop_loss_price,
            new_stop_loss_price: position.stop_loss_price,
            locked_profit: position.locked_profit,
          },
        });
        onClose();
      }
    } catch (error) {
      message.error('更新失败');
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
          <EditOutlined />
          编辑持仓 - {position.symbol} {position.name}
        </Space>
      }
      open={visible}
      onCancel={onClose}
      onOk={handleOk}
      confirmLoading={loading}
      okText="确认保存"
      cancelText="取消"
      width={550}
    >
      <Form
        form={form}
        layout="vertical"
        onValuesChange={handleValuesChange}
      >
        {/* 基本信息 */}
        <Descriptions column={2} size="small" style={{ marginBottom: 16 }}>
          <Descriptions.Item label="当前价">¥{position.current_price.toFixed(2)}</Descriptions.Item>
          <Descriptions.Item label="原止损价">¥{position.stop_loss_price.toFixed(2)}</Descriptions.Item>
        </Descriptions>

        <Divider orientation="left" plain style={{ margin: '12px 0' }}>持仓信息</Divider>

        {/* 可编辑：股数 */}
        <Form.Item
          name="shares"
          label="持仓数量（股）"
          rules={[{ required: true, message: '请输入持仓数量' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0}
            step={100}
            precision={0}
            placeholder="持仓数量"
          />
        </Form.Item>

        {/* 可编辑：成本价 */}
        <Form.Item
          name="cost_price"
          label="成本价"
          rules={[{ required: true, message: '请输入成本价' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0}
            precision={2}
            placeholder="成本价"
            formatter={(value) => `¥ ${value}`}
            // @ts-ignore
            parser={(displayValue) => {
              if (!displayValue) return 0;
              const cleaned = displayValue.replace('¥', '').replace(',', '');
              const parsed = parseFloat(cleaned);
              return isNaN(parsed) ? 0 : parsed;
            }}
          />
        </Form.Item>

        <Divider orientation="left" plain style={{ margin: '12px 0' }}>止损设置</Divider>

        {/* 可编辑：止损基数 */}
        <Form.Item
          name="new_stop_loss_base"
          label="止损基数"
          rules={[{ required: true, message: '请输入止损基数' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0}
            precision={2}
            placeholder="止损基数价格"
            formatter={(value) => `¥ ${value}`}
            // @ts-ignore
            parser={(displayValue) => {
              if (!displayValue) return 0;
              const cleaned = displayValue.replace('¥', '').replace(',', '');
              const parsed = parseFloat(cleaned);
              return isNaN(parsed) ? 0 : parsed;
            }}
          />
        </Form.Item>

        {/* 可编辑：止损比例 */}
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
