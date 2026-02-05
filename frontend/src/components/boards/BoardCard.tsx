import { memo } from 'react';
import { Card, Tag, Progress, Col, Typography, Row, Button } from 'antd';
import { TeamOutlined, RiseOutlined, FallOutlined } from '@ant-design/icons';
import type { Board } from '@/types';
import { formatNumber, formatPercent, formatCurrency } from '@/utils';

const { Text, Statistic } = Typography;

interface BoardCardProps {
  board: Board;
  onClick: () => void;
}

export const BoardCard = memo(function BoardCard({ board, onClick }: BoardCardProps) {
  return (
    <Card
      hoverable
      onClick={onClick}
      style={{ cursor: 'pointer', height: '100%' }}
      bodyStyle={{ padding: 16 }}
    >
      {/* Header */}
      <div style={{ marginBottom: 12 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div style={{ flex: 1 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
              <Text strong style={{ fontSize: 16 }}>
                {board.name}
              </Text>
              {board.category && <Tag color="blue">{board.category}</Tag>}
            </div>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {board.code}
            </Text>
          </div>
          <TeamOutlined style={{ fontSize: 20, color: '#1890ff' }} aria-hidden="true" />
        </div>
      </div>

      {/* Stats */}
      {board.stockCount && (
        <div style={{ marginBottom: 12 }}>
          <Row gutter={16}>
            <Col span={12}>
              <Statistic
                title="股票数"
                value={board.stockCount}
                valueStyle={{ fontSize: 16 }}
                prefix={<TeamOutlined style={{ fontSize: 12 }} aria-hidden="true" />}
              />
            </Col>
          </Row>
        </div>
      )}

      {/* Valuation */}
      {board.valuation && (
        <div style={{ marginTop: 16 }}>
          <Text strong style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
            估值指标
          </Text>
          <Row gutter={[8, 8]}>
            {board.valuation.pe != null && (
              <Col span={8}>
                <div>
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    市盈率
                  </Text>
                  <div style={{ fontSize: 14, fontWeight: 'bold' }}>
                    {formatNumber(board.valuation.pe, 2)}
                  </div>
                </div>
              </Col>
            )}
            {board.valuation.pb != null && (
              <Col span={8}>
                <div>
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    市净率
                  </Text>
                  <div style={{ fontSize: 14, fontWeight: 'bold' }}>
                    {formatNumber(board.valuation.pb, 2)}
                  </div>
                </div>
              </Col>
            )}
            {board.valuation.marketCap != null && (
              <Col span={8}>
                <div>
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    总市值
                  </Text>
                  <div style={{ fontSize: 14, fontWeight: 'bold' }}>
                    {formatCurrency(board.valuation.marketCap)}
                  </div>
                </div>
              </Col>
            )}
          </Row>
        </div>
      )}

      {/* Description */}
      {board.description && (
        <div style={{ marginTop: 12 }}>
          <Text
            ellipsis={{ rows: 2 }}
            style={{ fontSize: 12, color: '#666', display: 'block' }}
          >
            {board.description}
          </Text>
        </div>
      )}
    </Card>
  );
});
