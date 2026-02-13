import { Row, Col, Descriptions, Card } from 'antd';
import { MetricCard } from '../financial/MetricCard';
import type { BacktestResult } from '@/types';

interface BasicInfoCardsProps {
  result: BacktestResult;
}

export function BasicInfoCards({ result }: BasicInfoCardsProps) {
  const { basic_info, trade_stats, strategy_info } = result;

  return (
    <div>
      {/* Basic Information */}
      <Card title="回测概览" style={{ marginBottom: 24 }}>
        <Descriptions column={2} bordered size="small">
          <Descriptions.Item label="股票代码">{basic_info.stock}</Descriptions.Item>
          <Descriptions.Item label="策略">{strategy_info.strategy_name || strategy_info.strategy}</Descriptions.Item>
          <Descriptions.Item label="开始日期">{basic_info.start_date}</Descriptions.Item>
          <Descriptions.Item label="结束日期">{basic_info.end_date}</Descriptions.Item>
          <Descriptions.Item label="初始资金">
            ¥{basic_info.initial_cash.toLocaleString()}
          </Descriptions.Item>
          <Descriptions.Item label="最终资金">
            ¥{basic_info.final_value.toLocaleString()}
          </Descriptions.Item>
          <Descriptions.Item label="总收益率">
            <span style={{ color: basic_info.total_return >= 0 ? '#cf1322' : '#3f8600' }}>
              {(basic_info.total_return * 100).toFixed(2)}%
            </span>
          </Descriptions.Item>
          <Descriptions.Item label="手续费率">
            {(basic_info.commission * 100).toFixed(4)}%
          </Descriptions.Item>
        </Descriptions>
      </Card>

      {/* Trade Statistics */}
      <Card title="交易统计" style={{ marginBottom: 24 }}>
        <Row gutter={[16, 16]}>
          <Col xs={12} sm={8} md={6}>
            <MetricCard
              key="total_trades"
              title="总交易次数"
              value={trade_stats.total_trades}
              unit="number"
            />
          </Col>
          <Col xs={12} sm={8} md={6}>
            <MetricCard
              key="winning_trades"
              title="盈利次数"
              value={trade_stats.winning_trades}
              unit="number"
            />
          </Col>
          <Col xs={12} sm={8} md={6}>
            <MetricCard
              key="losing_trades"
              title="亏损次数"
              value={trade_stats.losing_trades}
              unit="number"
            />
          </Col>
          <Col xs={12} sm={8} md={6}>
            <MetricCard
              key="win_rate"
              title="胜率"
              value={(trade_stats.win_rate * 100).toFixed(2)}
              unit="percent"
            />
          </Col>
          <Col xs={12} sm={8} md={6}>
            <MetricCard
              key="total_profit"
              title="总盈利"
              value={trade_stats.total_profit}
              unit="currency"
            />
          </Col>
          <Col xs={12} sm={8} md={6}>
            <MetricCard
              key="total_loss"
              title="总亏损"
              value={trade_stats.total_loss}
              unit="currency"
            />
          </Col>
          <Col xs={12} sm={8} md={6}>
            <MetricCard
              key="profit_factor"
              title="盈亏比"
              value={trade_stats.profit_factor.toFixed(2)}
              unit="number"
            />
          </Col>
        </Row>
      </Card>

      {/* Strategy Parameters */}
      <Card title="策略参数">
        <Descriptions column={1} bordered size="small">
          {Object.entries(strategy_info.strategy_params || {})
            .filter(([key]) => key !== 'commission')  // 过滤掉commission字段
            .map(([key, value]) => (
            <Descriptions.Item key={key} label={key}>
              {String(value)}
            </Descriptions.Item>
          ))}
        </Descriptions>
      </Card>
    </div>
  );
}
