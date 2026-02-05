import { Row, Col } from 'antd';
import { MetricCard } from './MetricCard';
import type { MetricCard as MetricCardType } from '@/types';

interface SummaryCardsProps {
  metrics: MetricCardType[];
  loading?: boolean;
}

// 29个财务指标分类
const METRIC_CATEGORIES = {
  profitability: [
    { key: 'roe', title: '净资产收益率', unit: 'percent' },
    { key: 'roa', title: '总资产净利率', unit: 'percent' },
    { key: 'grossProfitMargin', title: '毛利率', unit: 'percent' },
    { key: 'netProfitMargin', title: '净利率', unit: 'percent' },
  ],
  growth: [
    { key: 'revenueGrowth', title: '营收增长率', unit: 'percent' },
    { key: 'profitGrowth', title: '利润增长率', unit: 'percent' },
    { key: 'epsGrowth', title: '每股收益增长率', unit: 'percent' },
  ],
  valuation: [
    { key: 'pe', title: '市盈率', unit: 'number' },
    { key: 'pb', title: '市净率', unit: 'number' },
    { key: 'ps', title: '市销率', unit: 'number' },
    { key: 'pcf', title: '市现率', unit: 'number' },
  ],
  liquidity: [
    { key: 'currentRatio', title: '流动比率', unit: 'number' },
    { key: 'quickRatio', title: '速动比率', unit: 'number' },
    { key: 'cashRatio', title: '现金比率', unit: 'number' },
  ],
  leverage: [
    { key: 'debtToAsset', title: '资产负债率', unit: 'percent' },
    { key: 'debtToEquity', title: '权益乘数', unit: 'number' },
    { key: 'currentLiabilityRatio', title: '流动负债比率', unit: 'percent' },
  ],
  efficiency: [
    { key: 'inventoryTurnover', title: '存货周转率', unit: 'number' },
    { key: 'receivablesTurnover', title: '应收账款周转率', unit: 'number' },
    { key: 'totalAssetTurnover', title: '总资产周转率', unit: 'number' },
  ],
  perShare: [
    { key: 'eps', title: '每股收益', unit: 'currency' },
    { key: 'bps', title: '每股净资产', unit: 'currency' },
    { key: 'cashPerShare', title: '每股现金流', unit: 'currency' },
  ],
  market: [
    { key: 'marketCap', title: '总市值', unit: 'currency' },
    { key: 'circulatingMarketCap', title: '流通市值', unit: 'currency' },
    { key: 'peRatio', title: '市盈率(TTM)', unit: 'number' },
  ],
  other: [
    { key: 'dividendYield', title: '股息率', unit: 'percent' },
    { key: 'bookValuePerShare', title: '每股账面价值', unit: 'currency' },
    { key: 'operatingCashFlowPerShare', title: '每股经营现金流', unit: 'currency' },
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

      {/* 杠杆比率 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>杠杆比率</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.leverage.map(({ key, title, unit }) => (
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

      {/* 其他指标 */}
      <div style={{ marginBottom: 24 }}>
        <h4 style={{ marginBottom: 16, color: '#666' }}>其他指标</h4>
        <Row gutter={[16, 16]}>
          {METRIC_CATEGORIES.other.map(({ key, title, unit }) => (
            <Col key={key} xs={12} sm={8} md={8}>
              <MetricCard {...metricsMap[key]} title={title} unit={unit} loading={loading} />
            </Col>
          ))}
        </Row>
      </div>
    </div>
  );
}
