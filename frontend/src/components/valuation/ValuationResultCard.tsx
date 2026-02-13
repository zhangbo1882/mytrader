import { Card, Row, Col, Statistic, Tag, Progress, Alert } from 'antd';
import {
  ArrowUpOutlined,
  ArrowDownOutlined,
  MinusOutlined,
} from '@ant-design/icons';
import type { ValuationResult } from '@/types';

interface ValuationResultCardProps {
  result: ValuationResult;
}

const RATING_CONFIG = {
  '强烈买入': { color: '#cf1322', icon: '🔥' },
  '买入': { color: '#f5222d', icon: '📈' },
  '持有': { color: '#faad14', icon: '➖' },
  '卖出': { color: '#52c41a', icon: '📉' },
  '强烈卖出': { color: '#237804', icon: '💀' },
};

export function ValuationResultCard({ result }: ValuationResultCardProps) {
  const ratingConfig = RATING_CONFIG[result.rating as keyof typeof RATING_CONFIG] ||
    RATING_CONFIG['持有'];

  const getUpDownIcon = () => {
    if (result.upside_downside > 5) return <ArrowUpOutlined />;
    if (result.upside_downside < -5) return <ArrowDownOutlined />;
    return <MinusOutlined />;
  };

  const getUpDownColor = () => {
    if (result.upside_downside > 0) return '#cf1322'; // Red for upside
    if (result.upside_downside < 0) return '#3f8600'; // Green for downside
    return '#999';
  };

  return (
    <Card
      title={`估值结果 - ${result.model}`}
      extra={
        <Tag color={ratingConfig.color} style={{ fontSize: 14, padding: '4px 12px' }}>
          {ratingConfig.icon} {result.rating}
        </Tag>
      }
    >
      {/* Warning messages */}
      {result.warnings.length > 0 && (
        <Alert
          message="数据警告"
          description={result.warnings.join('; ')}
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
        />
      )}

      {/* Core metrics */}
      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col xs={12} sm={6}>
          <Statistic
            title="当前价格"
            value={result.current_price}
            precision={2}
            prefix="¥"
            valueStyle={{ fontSize: 20 }}
          />
        </Col>
        <Col xs={12} sm={6}>
          <Statistic
            title="合理价值"
            value={result.fair_value}
            precision={2}
            prefix="¥"
            valueStyle={{
              fontSize: 20,
              color: result.fair_value > result.current_price ? '#3f8600' : '#cf1322'
            }}
          />
        </Col>
        <Col xs={12} sm={6}>
          <Statistic
            title="上下空间"
            value={result.upside_downside}
            precision={2}
            suffix="%"
            valueStyle={{
              fontSize: 20,
              color: getUpDownColor()
            }}
            prefix={getUpDownIcon()}
          />
        </Col>
        <Col xs={12} sm={6}>
          <div>
            <div style={{ fontSize: 14, color: '#666', marginBottom: 8 }}>置信度</div>
            <Progress
              percent={Math.round(result.confidence * 100)}
              status="active"
              strokeColor={result.confidence > 0.7 ? '#52c41a' : result.confidence > 0.4 ? '#faad14' : '#ff4d4f'}
            />
          </div>
        </Col>
      </Row>

      {/* Detailed metrics */}
      {result.metrics && Object.keys(result.metrics).length > 0 && (
        <>
          <div style={{ marginBottom: 12, fontWeight: 'bold', color: '#666' }}>
            详细指标
          </div>
          <Row gutter={[16, 16]}>
            {result.metrics.pe !== undefined && (
              <Col xs={12} sm={8}>
                <Card size="small" style={{ textAlign: 'center' }}>
                  <div style={{ color: '#666', fontSize: 12 }}>市盈率 (PE)</div>
                  <div style={{ fontSize: 18, fontWeight: 'bold' }}>
                    {result.metrics.pe.toFixed(2)}
                  </div>
                  {result.metrics.industry_pe && (
                    <div style={{ color: '#999', fontSize: 12 }}>
                      行业: {result.metrics.industry_pe.toFixed(2)}
                    </div>
                  )}
                </Card>
              </Col>
            )}
            {result.metrics.pb !== undefined && (
              <Col xs={12} sm={8}>
                <Card size="small" style={{ textAlign: 'center' }}>
                  <div style={{ color: '#666', fontSize: 12 }}>市净率 (PB)</div>
                  <div style={{ fontSize: 18, fontWeight: 'bold' }}>
                    {result.metrics.pb.toFixed(2)}
                  </div>
                  {result.metrics.industry_pb && (
                    <div style={{ color: '#999', fontSize: 12 }}>
                      行业: {result.metrics.industry_pb.toFixed(2)}
                    </div>
                  )}
                </Card>
              </Col>
            )}
            {result.metrics.ps !== undefined && (
              <Col xs={12} sm={8}>
                <Card size="small" style={{ textAlign: 'center' }}>
                  <div style={{ color: '#666', fontSize: 12 }}>市销率 (PS)</div>
                  <div style={{ fontSize: 18, fontWeight: 'bold' }}>
                    {result.metrics.ps.toFixed(2)}
                  </div>
                  {result.metrics.industry_ps && (
                    <div style={{ color: '#999', fontSize: 12 }}>
                      行业: {result.metrics.industry_ps.toFixed(2)}
                    </div>
                  )}
                </Card>
              </Col>
            )}
          </Row>
        </>
      )}

      {/* Assumptions */}
      {result.assumptions && Object.keys(result.assumptions).length > 0 && (
        <>
          <div style={{ marginTop: 16, marginBottom: 8, fontWeight: 'bold', color: '#666' }}>
            估值假设
          </div>
          <Card size="small" style={{ background: '#fafafa' }}>
            {Object.entries(result.assumptions).map(([key, value]) => (
              <div key={key} style={{ marginBottom: 8 }}>
                <div style={{ fontWeight: 'bold', color: '#333', marginBottom: 4 }}>
                  {key}
                </div>
                {typeof value === 'object' && value !== null ? (
                  <div style={{ marginLeft: 16 }}>
                    {Object.entries(value).map(([subKey, subValue]) => (
                      <div key={subKey} style={{ marginBottom: 2, fontSize: 13 }}>
                        <span style={{ color: '#666' }}>{subKey}:</span>{' '}
                        <span style={{ color: '#333' }}>
                          {typeof subValue === 'object' && subValue !== null
                            ? JSON.stringify(subValue)
                            : String(subValue)}
                        </span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <span style={{ color: '#333' }}>{String(value)}</span>
                )}
              </div>
            ))}
          </Card>
        </>
      )}
    </Card>
  );
}
