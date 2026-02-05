import { Card, Table, Tag, Statistic, Row, Col, Typography, Progress } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined, MinusOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { PredictionResult } from '@/types';
import { formatPercent, formatNumber } from '@/utils';

const { Text } = Typography;

interface PredictionResultsProps {
  results: PredictionResult[];
  loading?: boolean;
}

export function PredictionResults({ results, loading }: PredictionResultsProps) {
  // 计算统计信息
  const stats = {
    total: results.length,
    up: results.filter((r) => r.prediction > 0).length,
    down: results.filter((r) => r.prediction < 0).length,
    avgConfidence: results.length > 0
      ? results.reduce((sum, r) => sum + r.confidence, 0) / results.length
      : 0,
  };

  const getPredictionTag = (value: number) => {
    if (value > 0) {
      return (
        <Tag color="red" icon={<ArrowUpOutlined />}>
          上涨 {formatPercent(value)}
        </Tag>
      );
    } else if (value < 0) {
      return (
        <Tag color="green" icon={<ArrowDownOutlined />}>
          下跌 {formatPercent(Math.abs(value))}
        </Tag>
      );
    } else {
      return (
        <Tag icon={<MinusOutlined />}>
          持平
        </Tag>
      );
    }
  };

  const getConfidenceColor = (confidence: number) => {
    if (confidence >= 0.8) return '#52c41a';
    if (confidence >= 0.6) return '#faad14';
    return '#ff4d4f';
  };

  const columns: ColumnsType<PredictionResult> = [
    {
      title: '股票',
      dataIndex: 'stockCode',
      key: 'stock',
      width: 120,
      render: (code, record) => (
        <div>
          <div style={{ fontWeight: 'bold' }}>{code}</div>
          <div style={{ fontSize: 12, color: '#999' }}>{record.stockName}</div>
        </div>
      ),
    },
    {
      title: '日期',
      dataIndex: 'date',
      key: 'date',
      width: 120,
    },
    {
      title: '预测方向',
      dataIndex: 'prediction',
      key: 'prediction',
      width: 120,
      render: (value) => getPredictionTag(value),
    },
    {
      title: '预测值',
      dataIndex: 'prediction',
      key: 'predictionValue',
      width: 100,
      render: (value) => <Text strong>{formatPercent(value)}</Text>,
    },
    {
      title: '置信度',
      dataIndex: 'confidence',
      key: 'confidence',
      width: 150,
      render: (confidence) => (
        <div>
          <Progress
            percent={Math.round(confidence * 100)}
            strokeColor={getConfidenceColor(confidence)}
            size="small"
          />
          <Text style={{ fontSize: 12 }}>{formatPercent(confidence)}</Text>
        </div>
      ),
    },
    {
      title: '实际值',
      dataIndex: 'actual',
      key: 'actual',
      width: 100,
      render: (value) => (value != null ? <Text>{formatPercent(value)}</Text> : '-'),
    },
    {
      title: '误差',
      key: 'error',
      width: 100,
      render: (_, record) =>
        record.error != null ? (
          <Text type={Math.abs(record.error) > 0.05 ? 'danger' : 'secondary'}>
            {formatPercent(record.error)}
          </Text>
        ) : (
          '-'
        ),
    },
  ];

  return (
    <div>
      {/* 统计信息 */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card>
            <Statistic title="预测总数" value={stats.total} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="预测上涨"
              value={stats.up}
              valueStyle={{ color: '#cf1322' }}
              prefix={<ArrowUpOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="预测下跌"
              value={stats.down}
              valueStyle={{ color: '#3f8600' }}
              prefix={<ArrowDownOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="平均置信度"
              value={formatPercent(stats.avgConfidence)}
              valueStyle={{ color: stats.avgConfidence >= 0.7 ? '#3f8600' : '#faad14' }}
            />
          </Card>
        </Col>
      </Row>

      {/* 预测结果表格 */}
      <Card title="预测详情">
        <Table
          columns={columns}
          dataSource={results}
          rowKey={(record) => `${record.stockCode}-${record.date}`}
          loading={loading}
          scroll={{ x: 1000 }}
          pagination={{
            pageSize: 20,
            showSizeChanger: true,
            showTotal: (total) => `共 ${total} 条`,
          }}
        />
      </Card>
    </div>
  );
}
