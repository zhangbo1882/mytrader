import { Row, Col } from 'antd';
import { MetricCard } from './MetricCard';
import type { MetricCard as MetricCardType } from '@/types';

interface SummaryCardsProps {
  metrics: MetricCardType[];
  loading?: boolean;
}

// 财务指标分类（与后端返回的key保持一致）
const METRIC_CATEGORIES = {
  profitability: [
    { key: 'roe', title: '净资产收益率', unit: 'percent' },
    { key: 'roa', title: '总资产报酬率', unit: 'percent' },
    { key: 'grossprofit_margin', title: '销售毛利率', unit: 'percent' },
    { key: 'netprofit_margin', title: '销售净利率', unit: 'percent' },
  ],
  growth: [
    { key: 'or_yoy', title: '营业收入增长率', unit: 'percent' },
    { key: 'netprofit_yoy', title: '净利润增长率', unit: 'percent' },
    { key: 'assets_yoy', title: '总资产增长率', unit: 'percent' },
  ],
  valuation: [
    { key: 'pe', title: '市盈率(静态)', unit: 'number' },
    { key: 'pe_ttm', title: '市盈率TTM', unit: 'number' },
    { key: 'pb', title: '市净率', unit: 'number' },
    { key: 'ps', title: '市销率(静态)', unit: 'number' },
    { key: 'ps_ttm', title: '市销率TTM', unit: 'number' },
  ],
  liquidity: [
    { key: 'current_ratio', title: '流动比率', unit: 'number' },
    { key: 'quick_ratio', title: '速动比率', unit: 'number' },
    { key: 'cash_ratio', title: '现金比率', unit: 'number' },
  ],
  leverage: [
    { key: 'debt_to_assets', title: '资产负债率', unit: 'percent' },
  ],
  efficiency: [
    { key: 'assets_turn', title: '总资产周转率', unit: 'number' },
    { key: 'ar_turn', title: '应收账款周转率', unit: 'number' },
    { key: 'inv_turn', title: '存货周转率', unit: 'number' },
  ],
  perShare: [
    { key: 'basic_eps', title: '基本每股收益', unit: 'currency' },
    { key: 'bps', title: '每股净资产', unit: 'currency' },
    { key: 'ocfps', title: '每股经营现金流', unit: 'currency' },
  ],
  market: [
    { key: 'total_mv', title: '总市值', unit: 'currency' },
    { key: 'circ_mv', title: '流通市值', unit: 'currency' },
    { key: 'close', title: '收盘价', unit: 'currency' },
  ],
};

export function SummaryCards({ metrics, loading }: SummaryCardsProps) {
  const metricsMap = metrics.reduce((acc, metric) => {
    acc[metric.key] = metric;
    return acc;
  }, {} as Record<string, MetricCardType>);

  return (
    <div>
      {/* 盈利能力 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>盈利能力</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.profitability.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={6}>
              <MetricCard
                {...metricsMap[key]}
                title={title}
                unit={unit}
                loading={loading}
              />
            </Col>
          ))}
        </Row>
      </div>

      {/* 成长能力 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>成长能力</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.growth.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={6}>
              <MetricCard {...metricsMap[key]} title={title} unit={unit} loading={loading} />
            </Col>
          ))}
        </Row>
      </div>

      {/* 估值指标 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>估值指标</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.valuation.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={6}>
              <MetricCard {...metricsMap[key]} title={title} unit={unit} loading={loading} />
            </Col>
          ))}
        </Row>
      </div>

      {/* 偿债能力 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>偿债能力</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.liquidity.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={8}>
              <MetricCard {...metricsMap[key]} title={title} unit={unit} loading={loading} />
            </Col>
          ))}
        </Row>
      </div>

      {/* 运营效率 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>运营效率</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.efficiency.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={8}>
              <MetricCard {...metricsMap[key]} title={title} unit={unit} loading={loading} />
            </Col>
          ))}
        </Row>
      </div>

      {/* 每股指标 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>每股指标</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.perShare.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={8}>
              <MetricCard {...metricsMap[key]} title={title} unit={unit} loading={loading} />
            </Col>
          ))}
        </Row>
      </div>

      {/* 市场数据 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>市场数据</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.market.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={8}>
              <MetricCard {...metricsMap[key]} title={title} unit={unit} loading={loading} />
            </Col>
          ))}
        </Row>
      </div>
    </div>
  );
}
