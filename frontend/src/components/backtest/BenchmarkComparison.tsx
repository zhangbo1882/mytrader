import { Card, Empty, Descriptions, Row, Col } from 'antd';
import { MetricCard } from '../financial/MetricCard';
import type { BacktestResult } from '@/types';

interface BenchmarkComparisonProps {
  comparison: NonNullable<BacktestResult['benchmark_comparison']>;
}

export function BenchmarkComparison({ comparison }: BenchmarkComparisonProps) {
  if (!comparison) {
    return (
      <Empty
        description="未设置基准指数"
        image={Empty.PRESENTED_IMAGE_SIMPLE}
      />
    );
  }

  const { benchmark, benchmark_name, benchmark_type, benchmark_metrics, excess_return, excess_sharpe } = comparison;

  // 显示基准名称和类型
  const displayBenchmarkName = benchmark_name || benchmark;
  const displayBenchmarkType = benchmark_type || '指数对比';
  const benchmarkLabel = displayBenchmarkType === '买入持有'
    ? `${displayBenchmarkName} (买入持有)`
    : displayBenchmarkName;

  return (
    <div>
      <Card title="基准对比" style={{ marginBottom: 24 }}>
        <Descriptions column={1} bordered size="small">
          <Descriptions.Item label="基准">{benchmarkLabel}</Descriptions.Item>
          <Descriptions.Item label="基准类型">{displayBenchmarkType}</Descriptions.Item>
          <Descriptions.Item label="超额收益率">
            <span style={{ color: excess_return >= 0 ? '#cf1322' : '#3f8600', fontWeight: 'bold' }}>
              {(excess_return * 100).toFixed(2)}%
            </span>
          </Descriptions.Item>
          <Descriptions.Item label="超额夏普比率">
            <span style={{ color: excess_sharpe >= 0 ? '#cf1322' : '#3f8600', fontWeight: 'bold' }}>
              {excess_sharpe.toFixed(2)}
            </span>
          </Descriptions.Item>
        </Descriptions>
      </Card>

      {benchmark_metrics && (
        <Card title="基准指标">
          <Row gutter={[16, 16]}>
            <Col xs={12} sm={8} md={6}>
              <MetricCard
                key="benchmark_return"
                title="基准收益率"
                value={(benchmark_metrics.total_return || 0) * 100}
                unit="percent"
              />
            </Col>
            <Col xs={12} sm={8} md={6}>
              <MetricCard
                key="benchmark_volatility"
                title="基准波动率"
                value={(benchmark_metrics.volatility || 0) * 100}
                unit="percent"
              />
            </Col>
            <Col xs={12} sm={8} md={6}>
              <MetricCard
                key="benchmark_sharpe"
                title="基准夏普比率"
                value={benchmark_metrics.sharpe_ratio || 0}
                unit="number"
              />
            </Col>
            <Col xs={12} sm={8} md={6}>
              <MetricCard
                key="benchmark_max_drawdown"
                title="基准最大回撤"
                value={(benchmark_metrics.max_drawdown || 0) * 100}
                unit="percent"
              />
            </Col>
          </Row>
        </Card>
      )}
    </div>
  );
}
