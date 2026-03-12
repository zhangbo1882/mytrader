import { useState } from 'react';
import { Card, Table, Tag, Statistic, Row, Col, Typography, Progress } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined, MinusOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { PredictionResult } from '@/types';
import { formatPercent } from '@/utils';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine,
} from 'recharts';

const { Text } = Typography;

interface PredictionResultsProps {
  results: PredictionResult[];
  loading?: boolean;
}

export function PredictionResults({ results, loading }: PredictionResultsProps) {
  const [pagination, setPagination] = useState({ current: 1, pageSize: 20 });
  const isDirectionType = (targetType?: string) => targetType?.includes('direction');

  // 有实际值的记录
  const withActual = results.filter((r) => r.actual != null);
  const targetType = results[0]?.targetType;
  const isDirection = isDirectionType(targetType);

  // 计算统计信息（方向模型以0.5为界，回归模型以0为界）
  const stats = {
    total: results.length,
    up: results.filter((r) =>
      isDirectionType(r.targetType) ? r.prediction > 0.5 : r.prediction > 0
    ).length,
    down: results.filter((r) =>
      isDirectionType(r.targetType) ? r.prediction < 0.5 : r.prediction < 0
    ).length,
    // 分类：方向准确率；回归：MAE
    accuracy: withActual.length > 0
      ? isDirection
        ? withActual.filter((r) =>
            (r.prediction > 0.5) === (r.actual! > 0.5)
          ).length / withActual.length
        : withActual.reduce((sum, r) => sum + Math.abs(r.prediction - r.actual!), 0) / withActual.length
      : null,
  };

  // 获取预测目标类型的中文标签
  const getTargetTypeLabel = (targetType?: string) => {
    if (!targetType) return '-';
    const typeMap: Record<string, string> = {
      'return_1d': '1日收益率',
      'return_5d': '5日收益率',
      'direction_1d': '1日趋势',
      'direction_5d': '5日趋势',
      'high_low_5d': '5日波动率',
    };
    return typeMap[targetType] || targetType;
  };

  // 将预测值转换为文字描述
  const getPredictionLabel = (value: number, targetType?: string): { text: string; color: string } => {
    if (isDirectionType(targetType)) {
      // 分类任务：按上涨概率分级
      if (value >= 0.75) return { text: '强烈看涨', color: '#cf1322' };
      if (value >= 0.60) return { text: '看涨',     color: '#f5222d' };
      if (value >= 0.50) return { text: '弱看涨',   color: '#fa541c' };
      if (value >= 0.40) return { text: '弱看跌',   color: '#389e0d' };
      if (value >= 0.25) return { text: '看跌',     color: '#237804' };
      return                      { text: '强烈看跌', color: '#135200' };
    } else {
      // 回归任务：5日收益率阈值扩大（5天累计波动更大）
      const is5d = targetType === 'return_5d';
      if (is5d) {
        if (value >= 0.08)  return { text: '强势上涨', color: '#cf1322' };
        if (value >= 0.03)  return { text: '温和上涨', color: '#f5222d' };
        if (value >= 0.01)  return { text: '小幅上涨', color: '#fa541c' };
        if (value > 0)      return { text: '微涨',     color: '#d4380d' };
        if (value >= -0.01) return { text: '微跌',     color: '#389e0d' };
        if (value >= -0.03) return { text: '小幅下跌', color: '#237804' };
        if (value >= -0.08) return { text: '温和下跌', color: '#135200' };
        return                      { text: '强势下跌', color: '#092b00' };
      }
      // 1日收益率阈值
      if (value >= 0.03)  return { text: '强势上涨', color: '#cf1322' };
      if (value >= 0.01)  return { text: '温和上涨', color: '#f5222d' };
      if (value >= 0.003) return { text: '小幅上涨', color: '#fa541c' };
      if (value > 0)      return { text: '微涨',     color: '#d4380d' };
      if (value >= -0.003)return { text: '微跌',     color: '#389e0d' };
      if (value >= -0.01) return { text: '小幅下跌', color: '#237804' };
      if (value >= -0.03) return { text: '温和下跌', color: '#135200' };
      return                      { text: '强势下跌', color: '#092b00' };
    }
  };

  const getPredictionTag = (value: number, targetType?: string) => {
    const isDir = isDirectionType(targetType);
    const isUp = isDir ? value > 0.5 : value > 0;

    if (Math.abs(value - (isDir ? 0.5 : 0)) < (isDir ? 0.02 : 0.001)) {
      return <Tag icon={<MinusOutlined />}>持平</Tag>;
    }
    return isUp ? (
      <Tag color="red" icon={<ArrowUpOutlined />}>上涨</Tag>
    ) : (
      <Tag color="green" icon={<ArrowDownOutlined />}>下跌</Tag>
    );
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
      title: '预测目标',
      dataIndex: 'targetType',
      key: 'targetType',
      width: 120,
      render: (targetType) => (
        <Text>{getTargetTypeLabel(targetType)}</Text>
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
      render: (value, record) => getPredictionTag(value, record.targetType),
    },
    {
      title: '预测值',
      dataIndex: 'prediction',
      key: 'predictionValue',
      width: 100,
      render: (value, record) => {
        const { text, color } = getPredictionLabel(value, record.targetType);
        return <Text strong style={{ color }}>{text}</Text>;
      },
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
      render: (value, record) => {
        if (value == null) return <Text type="secondary">-</Text>;

        if (isDirectionType(record.targetType)) {
          return value > 0.5
            ? <Text style={{ color: '#cf1322' }}>上涨</Text>
            : <Text style={{ color: '#389e0d' }}>下跌</Text>;
        }

        // 回归任务：直接显示实际涨跌幅
        const pct = value * 100;
        const sign = pct >= 0 ? '+' : '';
        const color = pct >= 0 ? '#cf1322' : '#389e0d';
        return <Text style={{ color }}>{sign}{pct.toFixed(2)}%</Text>;
      },
    },
    {
      title: '误差',
      key: 'error',
      width: 100,
      render: (_, record) => {
        if (record.error == null) return '-';

        const isDirection = record.targetType?.includes('direction');
        if (isDirection) {
          // 分类任务显示是否正确
          const isCorrect = Math.abs(record.error) < 0.5;
          return isCorrect ? (
            <Text style={{ color: '#52c41a' }}>✓ 正确</Text>
          ) : (
            <Text type="danger">✗ 错误</Text>
          );
        }

        return (
          <Text type={Math.abs(record.error) > 0.05 ? 'danger' : 'secondary'}>
            {formatPercent(record.error)}
          </Text>
        );
      },
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
            {stats.accuracy == null ? (
              <Statistic title={isDirection ? '方向准确率' : '平均误差(MAE)'} value="暂无实际数据" />
            ) : isDirection ? (
              <Statistic
                title={`方向准确率（${withActual.length}条有效）`}
                value={(stats.accuracy * 100).toFixed(1)}
                suffix="%"
                valueStyle={{ color: stats.accuracy >= 0.55 ? '#3f8600' : stats.accuracy >= 0.5 ? '#faad14' : '#cf1322' }}
              />
            ) : (
              <Statistic
                title={`平均误差 MAE（${withActual.length}条有效）`}
                value={(stats.accuracy * 100).toFixed(2)}
                suffix="%"
                valueStyle={{ color: stats.accuracy < 0.01 ? '#3f8600' : stats.accuracy < 0.02 ? '#faad14' : '#cf1322' }}
              />
            )}
          </Card>
        </Col>
      </Row>

      {/* 预测 vs 实际对比曲线图 */}
      {results.length > 0 && results.some(r => r.actual !== undefined) && (
        <Card title="预测 vs 实际对比" style={{ marginBottom: 16 }}>
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={results} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => value.substring(5)} // Show MM-DD only
              />
              <YAxis
                tickFormatter={(value) => {
                  // 检查是否是分类任务
                  const isDirection = results[0]?.targetType?.includes('direction');
                  if (isDirection) {
                    // 分类任务：0/1 显示为下跌/上涨
                    return value > 0.5 ? '上涨' : '下跌';
                  }
                  // 回归任务：显示百分比
                  return `${(value * 100).toFixed(1)}%`;
                }}
              />
              <Tooltip
                formatter={(value: any, name: any, props: any) => {
                  const isDirection = props.payload?.targetType?.includes('direction');

                  if (name === '预测值') {
                    if (isDirection) {
                      return [value > 0.5 ? '上涨' : '下跌', '预测值'];
                    }
                    return [formatPercent(value), '预测值'];
                  }
                  if (name === '实际值') {
                    if (isDirection) {
                      return [value > 0.5 ? '上涨' : '下跌', '实际值'];
                    }
                    return [formatPercent(value), '实际值'];
                  }
                  return [value, name];
                }}
                labelFormatter={(label) => `日期: ${label}`}
              />
              <Legend />
              <ReferenceLine
                y={results[0]?.targetType?.includes('direction') ? 0.5 : 0}
                stroke="#666"
                strokeDasharray="2 2"
              />
              <Line
                type="monotone"
                dataKey="prediction"
                name="预测值"
                stroke="#1890ff"
                strokeWidth={2}
                dot={{ r: 3 }}
                connectNulls={false}
              />
              <Line
                type="monotone"
                dataKey="actual"
                name="实际值"
                stroke="#52c41a"
                strokeWidth={2}
                dot={{ r: 3 }}
                connectNulls={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </Card>
      )}

      {/* 预测结果表格 */}
      <Card title="预测详情">
        <Table
          columns={columns}
          dataSource={results}
          rowKey={(record) => `${record.stockCode}-${record.date}`}
          loading={loading}
          scroll={{ x: 1000 }}
          pagination={{
            current: pagination.current,
            pageSize: pagination.pageSize,
            pageSizeOptions: ['10', '20', '50', '100'],
            showSizeChanger: true,
            showTotal: (total) => `共 ${total} 条`,
            hideOnSinglePage: false,
            onChange: (page, pageSize) => {
              setPagination({ current: page, pageSize });
            },
          }}
        />
      </Card>
    </div>
  );
}
