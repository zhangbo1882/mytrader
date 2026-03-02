/**
 * 月度总结卡片组件
 */
import React from 'react';
import { Card, Descriptions, Tag, Empty, Divider, Button } from 'antd';
import {
  RiseOutlined,
  FallOutlined,
  SyncOutlined
} from '@ant-design/icons';
import type { CapitalState, MonthlySnapshot } from '../../types/risk.types';

interface MonthlySummaryCardProps {
  capitalState: CapitalState | null;
  currentMonthSnapshot?: MonthlySnapshot | null;
  onCreateSnapshot?: () => void;
  loading?: boolean;
}

const MonthlySummaryCard: React.FC<MonthlySummaryCardProps> = ({
  capitalState,
  currentMonthSnapshot,
  onCreateSnapshot,
  loading
}) => {
  if (!capitalState) {
    return (
      <Card title="本月总结" size="small">
        <Empty description="暂无数据" />
      </Card>
    );
  }

  // 获取当前月份
  const currentMonth = new Date().toISOString().slice(0, 7);

  // 月初资金
  const monthStartCapital = currentMonthSnapshot?.month_start_capital ?? capitalState.initial_capital;

  // 本月盈亏 = 当前资金 - 月初资金
  const monthPnl = capitalState.current_capital - monthStartCapital;

  // 盈亏比例 = 本月盈亏 / 月初资金 * 100%
  const monthPnlPercent = monthStartCapital > 0 ? (monthPnl / monthStartCapital * 100) : 0;

  // 盈亏颜色
  const pnlColor = monthPnl >= 0 ? '#cf1322' : '#3f8600';

  return (
    <Card
      title={`本月总结 (${currentMonth})`}
      size="small"
      extra={
        <Button
          type="link"
          size="small"
          icon={<SyncOutlined spin={loading} />}
          onClick={onCreateSnapshot}
          loading={loading}
        >
          保存快照
        </Button>
      }
    >
      <Descriptions column={2} size="small">
        <Descriptions.Item label="月初资金">
          ¥{monthStartCapital.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </Descriptions.Item>
        <Descriptions.Item label="当前资金">
          ¥{capitalState.current_capital.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </Descriptions.Item>
        <Descriptions.Item label="本月盈亏">
          <span style={{ color: pnlColor, fontWeight: 'bold' }}>
            {monthPnl >= 0 ? <RiseOutlined /> : <FallOutlined />}
            {' '}{monthPnl >= 0 ? '+' : ''}¥{monthPnl.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </span>
        </Descriptions.Item>
        <Descriptions.Item label="盈亏比例">
          <Tag color={monthPnl >= 0 ? 'red' : 'green'}>
            {monthPnl >= 0 ? '+' : ''}{monthPnlPercent.toFixed(2)}%
          </Tag>
        </Descriptions.Item>
      </Descriptions>

      <Divider style={{ margin: '12px 0' }} />

      <Descriptions column={2} size="small">
        <Descriptions.Item label="持仓市值">
          ¥{capitalState.positions_value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </Descriptions.Item>
        <Descriptions.Item label="剩余现金">
          ¥{capitalState.cash.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </Descriptions.Item>
        <Descriptions.Item label="浮动盈亏">
          <span style={{ color: capitalState.floating_pnl >= 0 ? '#cf1322' : '#3f8600' }}>
            {capitalState.floating_pnl >= 0 ? '+' : ''}¥{capitalState.floating_pnl.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </span>
        </Descriptions.Item>
        <Descriptions.Item label="资金变动">
          {currentMonthSnapshot?.capital_change ? (
            <Tag color={currentMonthSnapshot.capital_change > 0 ? 'blue' : 'orange'}>
              {currentMonthSnapshot.capital_change > 0 ? '+' : ''}¥{currentMonthSnapshot.capital_change.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            </Tag>
          ) : (
            <Tag>无</Tag>
          )}
        </Descriptions.Item>
      </Descriptions>
    </Card>
  );
};

export default MonthlySummaryCard;
