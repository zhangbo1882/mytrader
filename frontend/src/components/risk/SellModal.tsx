/**
 * 卖出弹窗组件
 */
import React, { useState, useEffect } from 'react';
import { Modal, Form, InputNumber, Descriptions, Space, Tag, message, Alert } from 'antd';
import { DollarOutlined, RiseOutlined, FallOutlined } from '@ant-design/icons';
import type { PositionDetail, SellResponse, Position } from '../../types/risk.types';
import { sellPosition } from '../../services/riskService';

interface SellModalProps {
  visible: boolean;
  position: PositionDetail | null;
  totalCapital: number;
  maxTotalRiskPercent: number;
  maxSingleRiskPercent: number;
  allPositions: Position[];
  onClose: () => void;
  onSuccess: (response: SellResponse) => void;
}

const SellModal: React.FC<SellModalProps> = ({
  visible,
  position,
  totalCapital,
  maxTotalRiskPercent,
  maxSingleRiskPercent,
  allPositions,
  onClose,
  onSuccess
}) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState<{
    sellValue: number;
    realizedProfit: number;
    releasedRisk: number;
    remainingShares: number;
  } | null>(null);

  useEffect(() => {
    if (position && visible) {
      form.setFieldsValue({
        sell_shares: position.shares,
        sell_price: position.current_price,
      });
      calculatePreview();
    }
  }, [position, visible, form]);

  const calculatePreview = () => {
    if (!position) return;

    const values = form.getFieldsValue();
    const sellShares = values.sell_shares || 0;
    const sellPrice = values.sell_price || 0;

    const sellValue = sellShares * sellPrice;
    const realizedProfit = sellShares * (sellPrice - position.cost_price);
    const releasedRisk = sellShares * position.risk_per_share;
    const remainingShares = position.shares - sellShares;

    setPreview({
      sellValue,
      realizedProfit,
      releasedRisk,
      remainingShares,
    });
  };

  const handleOk = async () => {
    if (!position) return;

    try {
      const values = await form.validateFields();
      setLoading(true);

      const response = await sellPosition({
        total_capital: totalCapital,
        max_total_risk_percent: maxTotalRiskPercent,
        max_single_risk_percent: maxSingleRiskPercent,
        positions: allPositions,
        symbol: position.symbol,
        sell_shares: values.sell_shares,
        sell_price: values.sell_price,
      });

      if (response.success) {
        message.success('卖出成功');
        onSuccess(response);
        onClose();
      } else {
        message.error(response.error || '卖出失败');
      }
    } catch (error) {
      message.error('卖出失败');
    } finally {
      setLoading(false);
    }
  };

  const handleValuesChange = () => {
    calculatePreview();
  };

  if (!position) return null;

  const isPartialSell = preview && preview.remainingShares > 0;

  return (
    <Modal
      title={
        <Space>
          <DollarOutlined />
          卖出股票 - {position.symbol}
        </Space>
      }
      open={visible}
      onCancel={onClose}
      onOk={handleOk}
      confirmLoading={loading}
      okText="确认卖出"
      okButtonProps={{ danger: true }}
      cancelText="取消"
      width={500}
    >
      <Descriptions column={2} size="small" style={{ marginBottom: 16 }}>
        <Descriptions.Item label="当前持仓">{position.shares.toLocaleString()} 股</Descriptions.Item>
        <Descriptions.Item label="成本价">¥{position.cost_price.toFixed(2)}</Descriptions.Item>
        <Descriptions.Item label="当前价">¥{position.current_price.toFixed(2)}</Descriptions.Item>
        <Descriptions.Item label="风险占用">
          ¥{position.total_risk.toLocaleString()}
        </Descriptions.Item>
      </Descriptions>

      <Form
        form={form}
        layout="vertical"
        onValuesChange={handleValuesChange}
      >
        <Form.Item
          name="sell_shares"
          label="卖出数量"
          rules={[
            { required: true, message: '请输入卖出数量' },
            { type: 'number', min: 1, max: position.shares, message: `卖出数量必须在 1-${position.shares} 之间` }
          ]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={1}
            max={position.shares}
            step={100}
            placeholder="卖出股数"
          />
        </Form.Item>

        <Form.Item
          name="sell_price"
          label="卖出价格"
          rules={[{ required: true, message: '请输入卖出价格' }]}
        >
          <InputNumber
            style={{ width: '100%' }}
            min={0}
            precision={2}
            placeholder="卖出价格"
          />
        </Form.Item>
      </Form>

      {preview && (
        <>
          <Descriptions title="卖出预览" column={2} size="small" bordered>
            <Descriptions.Item label="卖出金额">
              ¥{preview.sellValue.toLocaleString()}
            </Descriptions.Item>
            <Descriptions.Item label="实现盈亏">
              {preview.realizedProfit >= 0 ? (
                <Tag color="red" icon={<RiseOutlined />}>
                  +¥{preview.realizedProfit.toLocaleString()}
                </Tag>
              ) : (
                <Tag color="green" icon={<FallOutlined />}>
                  -¥{Math.abs(preview.realizedProfit).toLocaleString()}
                </Tag>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="释放风险">
              <Tag color="blue">¥{preview.releasedRisk.toLocaleString()}</Tag>
            </Descriptions.Item>
            <Descriptions.Item label="剩余持仓">
              {preview.remainingShares.toLocaleString()} 股
            </Descriptions.Item>
          </Descriptions>

          {!isPartialSell && (
            <Alert
              message="全部卖出"
              description="此操作将清空该持仓，回收全部资金和释放全部风险占用。"
              type="warning"
              showIcon
              style={{ marginTop: 16 }}
            />
          )}
        </>
      )}
    </Modal>
  );
};

export default SellModal;
