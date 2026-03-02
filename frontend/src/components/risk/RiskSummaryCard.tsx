/**
 * 风险汇总卡片组件
 */
import React, { useState } from 'react';
import { Card, Progress, Descriptions, Space, Tag, Divider, InputNumber, Button, message } from 'antd';
import {
  ThunderboltOutlined,
  RiseOutlined,
  FallOutlined,
  EditOutlined,
  CheckOutlined,
  CloseOutlined
} from '@ant-design/icons';
import type { PortfolioState, CapitalState } from '../../types/risk.types';

interface RiskSummaryCardProps {
  portfolio: PortfolioState | null;
  capitalState?: CapitalState | null;
  onUpdateCash?: (cash: number) => Promise<void>;
}

const RiskSummaryCard: React.FC<RiskSummaryCardProps> = ({ portfolio, capitalState, onUpdateCash }) => {
  const [editingCash, setEditingCash] = useState(false);
  const [cashValue, setCashValue] = useState<number>(0);
  const [loading, setLoading] = useState(false);

  if (!portfolio) {
    return (
      <Card title="风险汇总" size="small">
        <div style={{ textAlign: 'center', color: '#999' }}>
          请设置总资金后查看
        </div>
      </Card>
    );
  }

  const riskUsagePercent = portfolio.risk_usage_percent;
  const riskStatus = riskUsagePercent < 50 ? 'success' : riskUsagePercent < 80 ? 'normal' : 'exception';

  // 计算总仓位 = 持仓市值 / 当前资金
  const positionPercent = capitalState && capitalState.current_capital > 0
    ? (capitalState.positions_value / capitalState.current_capital * 100)
    : (portfolio.total_capital > 0 ? (portfolio.positions_value / portfolio.total_capital * 100) : 0);

  // 浮动盈亏比例
  const floatingPnlPercent = capitalState && capitalState.initial_capital > 0
    ? ((capitalState.current_capital - capitalState.initial_capital) / capitalState.initial_capital * 100)
    : 0;

  // 浮动盈亏颜色
  const floatingPnlColor = capitalState && capitalState.floating_pnl >= 0 ? '#cf1322' : '#3f8600';

  const handleEditCash = () => {
    if (capitalState) {
      setCashValue(capitalState.cash);
      setEditingCash(true);
    }
  };

  const handleSaveCash = async () => {
    if (onUpdateCash) {
      setLoading(true);
      try {
        await onUpdateCash(cashValue);
        message.success('剩余现金已更新');
      } catch (error) {
        message.error('更新失败');
      } finally {
        setLoading(false);
      }
    }
    setEditingCash(false);
  };

  const handleCancelEdit = () => {
    setEditingCash(false);
  };

  return (
    <Card title="风险汇总" size="small">
      {/* 资金概况 */}
      {capitalState && (
        <>
          <Descriptions column={2} size="small">
            <Descriptions.Item label="初始资金">
              <strong>¥{capitalState.initial_capital.toLocaleString()}</strong>
            </Descriptions.Item>
            <Descriptions.Item label="当前资金">
              <strong>¥{capitalState.current_capital.toLocaleString()}</strong>
            </Descriptions.Item>
            <Descriptions.Item label="持仓市值">
              ¥{capitalState.positions_value.toLocaleString()}
            </Descriptions.Item>
            <Descriptions.Item label="剩余现金">
              {editingCash ? (
                <Space size="small">
                  <InputNumber
                    size="small"
                    value={cashValue}
                    onChange={(v) => setCashValue(v || 0)}
                    formatter={(v) => `¥ ${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
                    parser={(v) => v?.replace(/¥\s?|(,*)/g, '') as unknown as number}
                    style={{ width: 120 }}
                  />
                  <Button
                    type="primary"
                    size="small"
                    icon={<CheckOutlined />}
                    loading={loading}
                    onClick={handleSaveCash}
                  />
                  <Button
                    size="small"
                    icon={<CloseOutlined />}
                    onClick={handleCancelEdit}
                  />
                </Space>
              ) : (
                <Space size="small">
                  <Tag color={capitalState.cash >= 0 ? 'green' : 'red'}>
                    ¥{capitalState.cash.toLocaleString()}
                  </Tag>
                  {onUpdateCash && (
                    <Button
                      type="link"
                      size="small"
                      icon={<EditOutlined />}
                      onClick={handleEditCash}
                    />
                  )}
                </Space>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="浮动盈亏">
              <span style={{ color: floatingPnlColor, fontWeight: 'bold' }}>
                {capitalState.floating_pnl >= 0 ? <RiseOutlined /> : <FallOutlined />}
                {' '}{capitalState.floating_pnl >= 0 ? '+' : ''}¥{capitalState.floating_pnl.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                {' '}({floatingPnlPercent >= 0 ? '+' : ''}{floatingPnlPercent.toFixed(2)}%)
              </span>
            </Descriptions.Item>
          </Descriptions>
          <Divider style={{ margin: '12px 0' }} />
        </>
      )}

      {/* 风险信息 */}
      <Descriptions column={2} size="small">
        <Descriptions.Item label="风险额度">
          <Tag color="blue">¥{portfolio.total_risk_budget.toLocaleString()}</Tag>
        </Descriptions.Item>
        <Descriptions.Item label="总仓位">
          <Tag color={positionPercent > 80 ? 'red' : positionPercent > 50 ? 'orange' : 'green'}>
            {positionPercent.toFixed(1)}%
          </Tag>
        </Descriptions.Item>
        <Descriptions.Item label="已用风险">
          <Tag color={portfolio.used_risk > 0 ? 'orange' : 'default'}>
            ¥{portfolio.used_risk.toLocaleString()}
          </Tag>
        </Descriptions.Item>
        <Descriptions.Item label="可用风险">
          <Tag color="green">¥{portfolio.available_risk.toLocaleString()}</Tag>
        </Descriptions.Item>
      </Descriptions>

      <div style={{ marginTop: 16 }}>
        <Space direction="vertical" style={{ width: '100%' }}>
          <span><ThunderboltOutlined /> 风险使用率</span>
          <Progress
            percent={riskUsagePercent}
            status={riskStatus}
            format={(percent) => `${percent?.toFixed(1)}%`}
          />
        </Space>
      </div>
    </Card>
  );
};

export default RiskSummaryCard;
