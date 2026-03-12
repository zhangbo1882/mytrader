/**
 * 计算结果展示组件
 */
import React from 'react';
import { Card, Descriptions, Tag, Space, Alert, Statistic, Row, Col } from 'antd';
import {
  InfoCircleOutlined,
  WarningFilled
} from '@ant-design/icons';
import type { CalculateResponse } from '../../types/risk.types';

interface CalculationResultProps {
  result: CalculateResponse | null;
}

const CalculationResult: React.FC<CalculationResultProps> = ({ result }) => {
  if (!result) {
    return null;
  }

  if (!result.success) {
    return (
      <Alert
        message="计算失败"
        description={result.error}
        type="error"
        showIcon
      />
    );
  }

  const { new_position: pos } = result;
  const hasLimitingFactors = pos.limiting_factors.length > 0;

  return (
    <Card title="计算结果" size="small">
      {pos.max_shares > 0 ? (
        <>
          <Row gutter={16} style={{ marginBottom: 16 }}>
            <Col span={8}>
              <Statistic
                title="可买股数"
                value={pos.max_shares}
                suffix="股"
                valueStyle={{ color: '#1890ff', fontSize: 24 }}
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="需要资金"
                value={pos.required_capital}
                prefix="¥"
                valueStyle={{ fontSize: 24 }}
              />
            </Col>
            <Col span={8}>
              <Statistic
                title="最大亏损"
                value={pos.max_loss}
                prefix="¥"
                valueStyle={{ color: '#cf1322', fontSize: 24 }}
              />
            </Col>
          </Row>

          <Descriptions column={2} size="small" bordered>
            <Descriptions.Item label="买入价格">
              ¥{pos.buy_price.toFixed(2)}
            </Descriptions.Item>
            <Descriptions.Item label="止损比例">
              {pos.stop_loss_percent}%
            </Descriptions.Item>
            <Descriptions.Item label="每股风险">
              ¥{pos.loss_per_share.toFixed(4)}
            </Descriptions.Item>
            <Descriptions.Item label="止损价">
              ¥{(pos.buy_price * (1 - pos.stop_loss_percent / 100)).toFixed(2)}
            </Descriptions.Item>
          </Descriptions>

          <Descriptions title="限制因素" column={3} size="small" style={{ marginTop: 16 }}>
            <Descriptions.Item label="总风险限制">
              {pos.max_by_total_risk.toLocaleString()} 股
              {pos.limiting_factors.includes('total_risk') && (
                <Tag color="orange" style={{ marginLeft: 8 }}>限制中</Tag>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="单笔风险限制">
              {pos.max_by_single_risk.toLocaleString()} 股
              {pos.limiting_factors.includes('single_risk') && (
                <Tag color="orange" style={{ marginLeft: 8 }}>限制中</Tag>
              )}
            </Descriptions.Item>
            <Descriptions.Item label="剩余资金">
              {pos.max_by_cash.toLocaleString()} 股
              {pos.limiting_factors.includes('cash') && (
                <Tag color="orange" style={{ marginLeft: 8 }}>限制中</Tag>
              )}
            </Descriptions.Item>
          </Descriptions>

          {hasLimitingFactors && (
            <Alert
              message={
                <Space>
                  <InfoCircleOutlined />
                  <span>
                    当前受 <strong>{pos.limiting_factor_names.join('、')}</strong> 限制
                  </span>
                </Space>
              }
              type="info"
              showIcon
              style={{ marginTop: 16 }}
            />
          )}

          <Descriptions title="买入后预测" column={2} size="small" style={{ marginTop: 16 }}>
            <Descriptions.Item label="新总风险">
              ¥{pos.after_buy.new_total_risk.toLocaleString()}
            </Descriptions.Item>
            <Descriptions.Item label="新风险使用率">
              <Tag color={pos.after_buy.new_risk_usage > 80 ? 'red' : pos.after_buy.new_risk_usage > 50 ? 'orange' : 'green'}>
                {pos.after_buy.new_risk_usage.toFixed(1)}%
              </Tag>
            </Descriptions.Item>
            <Descriptions.Item label="新持仓市值">
              ¥{pos.after_buy.new_positions_value.toLocaleString()}
            </Descriptions.Item>
            <Descriptions.Item label="新剩余资金">
              ¥{pos.after_buy.new_remaining_cash.toLocaleString()}
            </Descriptions.Item>
          </Descriptions>
        </>
      ) : (
        <Alert
          message="无法买入"
          description={
            <Space direction="vertical">
              <span>当前条件下无法买入任何股票，原因可能是：</span>
              <ul style={{ margin: 0, paddingLeft: 20 }}>
                <li>剩余资金不足</li>
                <li>可用风险额度不足</li>
                <li>止损比例设置过高</li>
              </ul>
            </Space>
          }
          type="warning"
          showIcon
          icon={<WarningFilled />}
        />
      )}
    </Card>
  );
};

export default CalculationResult;
