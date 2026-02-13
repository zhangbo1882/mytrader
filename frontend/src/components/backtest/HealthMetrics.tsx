import { Row, Col } from 'antd';
import { MetricCard } from '../financial/MetricCard';
import type { BacktestResult } from '@/types';

interface HealthMetricsProps {
  metrics: BacktestResult['health_metrics'];
}

// Health metrics categories
const METRIC_CATEGORIES = {
  returns: [
    { key: 'annual_return', title: '年化收益率', unit: 'percent' },
    { key: 'total_return', title: '总收益率', unit: 'percent' },
    { key: 'avg_monthly_return', title: '月均收益率', unit: 'percent' },
  ],
  risk: [
    { key: 'volatility', title: '波动率', unit: 'percent' },
    { key: 'max_drawdown', title: '最大回撤', unit: 'percent' },
  ],
  riskAdjusted: [
    { key: 'sharpe_ratio', title: '夏普比率', unit: 'number' },
    { key: 'sortino_ratio', title: '索提诺比率', unit: 'number' },
    { key: 'calmar_ratio', title: '卡玛比率', unit: 'number' },
  ],
  trading: [
    { key: 'profit_factor', title: '盈亏比', unit: 'number' },
    { key: 'monthly_win_rate', title: '月度胜率', unit: 'percent' },
  ],
};

export function HealthMetrics({ metrics }: HealthMetricsProps) {
  // 健康指标中的百分比字段需要乘以100后传递给MetricCard
  // MetricCard使用formatPercent显示，会直接加%符号
  const metricsMap = {
    annual_return: { key: 'annual_return', value: (metrics.annual_return ?? 0) * 100 },
    total_return: { key: 'total_return', value: (metrics.total_return ?? 0) * 100 },
    avg_monthly_return: { key: 'avg_monthly_return', value: (metrics.avg_monthly_return ?? 0) * 100 },
    volatility: { key: 'volatility', value: (metrics.volatility ?? 0) * 100 },
    max_drawdown: { key: 'max_drawdown', value: (metrics.max_drawdown ?? 0) * 100 },
    sharpe_ratio: { key: 'sharpe_ratio', value: metrics.sharpe_ratio ?? 0 },
    sortino_ratio: { key: 'sortino_ratio', value: metrics.sortino_ratio ?? 0 },
    calmar_ratio: { key: 'calmar_ratio', value: metrics.calmar_ratio ?? 0 },
    profit_factor: { key: 'profit_factor', value: metrics.profit_factor ?? 0 },
    monthly_win_rate: { key: 'monthly_win_rate', value: (metrics.monthly_win_rate ?? 0) * 100 },
  };

  return (
    <div>
      {/* Returns */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>收益指标</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.returns.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={8}>
              <MetricCard
                {...metricsMap[key as keyof typeof metricsMap]}
                title={title}
                unit={unit}
              />
            </Col>
          ))}
        </Row>
      </div>

      {/* Risk */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>风险指标</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.risk.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={12}>
              <MetricCard
                {...metricsMap[key as keyof typeof metricsMap]}
                title={title}
                unit={unit}
              />
            </Col>
          ))}
        </Row>
      </div>

      {/* Risk Adjusted */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>风险调整后收益</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.riskAdjusted.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={8}>
              <MetricCard
                {...metricsMap[key as keyof typeof metricsMap]}
                title={title}
                unit={unit}
              />
            </Col>
          ))}
        </Row>
      </div>

      {/* Trading */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>交易指标</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.trading.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={12}>
              <MetricCard
                {...metricsMap[key as keyof typeof metricsMap]}
                title={title}
                unit={unit}
              />
            </Col>
          ))}
        </Row>
      </div>
    </div>
  );
}
