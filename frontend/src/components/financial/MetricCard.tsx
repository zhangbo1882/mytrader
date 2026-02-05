import { memo } from 'react';
import { Card, Statistic, Row, Col, Typography } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined, MinusOutlined } from '@ant-design/icons';
import type { MetricCard as MetricCardType } from '@/types';
import { formatNumber, formatCurrency, formatPercent } from '@/utils';

const { Text } = Typography;

interface MetricCardProps extends MetricCardType {
  loading?: boolean;
}

export const MetricCard = memo(function MetricCard({ title, value, unit, trend, description, loading }: MetricCardProps) {
  const formatValue = () => {
    if (value == null) return '-';

    if (unit === 'currency') {
      return formatCurrency(Number(value));
    } else if (unit === 'percent') {
      return formatPercent(Number(value));
    } else if (unit === 'number') {
      return formatNumber(Number(value));
    }
    return String(value);
  };

  const getTrendIcon = () => {
    if (trend === 'up') return <ArrowUpOutlined aria-hidden="true" />;
    if (trend === 'down') return <ArrowDownOutlined aria-hidden="true" />;
    return <MinusOutlined aria-hidden="true" />;
  };

  const getTrendColor = () => {
    if (trend === 'up') return '#cf1322'; // Red for up in Chinese stock market
    if (trend === 'down') return '#3f8600'; // Green for down
    return '#999';
  };

  return (
    <Card loading={loading} size="small">
      <Row gutter={8} align="middle">
        <Col flex={1}>
          <Statistic
            title={title}
            value={formatValue()}
            valueStyle={{
              fontSize: 20,
              fontWeight: 'bold',
              color: value != null ? undefined : '#999',
            }}
          />
        </Col>
        {trend && (
          <Col>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                color: getTrendColor(),
                fontSize: 12,
              }}
            >
              {getTrendIcon()}
            </div>
          </Col>
        )}
      </Row>
      {description && (
        <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8 }}>
          {description}
        </Text>
      )}
    </Card>
  );
});
