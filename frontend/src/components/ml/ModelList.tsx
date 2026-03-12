import { useState } from 'react';
import { Card, Table, Tag, Space, Button, Typography, Progress, Popconfirm } from 'antd';
import {
  DeleteOutlined,
  EyeOutlined,
  LineChartOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  SyncOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { MLModel, ModelType, PredictionTarget } from '@/types';
import { formatDate } from '@/utils';

const { Text } = Typography;

interface ModelListProps {
  models: MLModel[];
  loading?: boolean;
  onDelete?: (id: string) => void;
  onView?: (model: MLModel) => void;
  onPredict?: (model: MLModel) => void;
}

export function ModelList({ models, loading, onDelete, onView, onPredict }: ModelListProps) {
  const [pagination, setPagination] = useState({ current: 1, pageSize: 10 });

  const handleDelete = async (id: string) => {
    if (onDelete) {
      onDelete(id);
    }
  };

  const getModelTypeLabel = (type: ModelType) => {
    const typeMap: Record<ModelType, { label: string; color: string }> = {
      lightgbm: { label: 'LightGBM', color: 'blue' },
      lstm: { label: 'LSTM', color: 'purple' },
      xgboost: { label: 'XGBoost', color: 'green' },
      random_forest: { label: '随机森林', color: 'orange' },
    };
    const info = typeMap[type];
    return <Tag color={info.color}>{info.label}</Tag>;
  };

  const getTargetLabel = (target: PredictionTarget) => {
    const targetMap: Record<PredictionTarget, string> = {
      '1d_return': '1日收益率',
      '5d_return': '5日收益率',
      'trend': '1日趋势',
      'trend_5d': '5日趋势',
      volatility: '波动率',
    };
    return targetMap[target] ?? target;
  };

  const getStatusTag = (status: string) => {
    switch (status) {
      case 'training':
        return <Tag icon={<SyncOutlined spin />} color="processing">训练中</Tag>;
      case 'completed':
        return <Tag icon={<CheckCircleOutlined />} color="success">已完成</Tag>;
      case 'failed':
        return <Tag icon={<CloseCircleOutlined />} color="error">失败</Tag>;
      default:
        return <Tag>{status}</Tag>;
    }
  };

  const columns: ColumnsType<MLModel> = [
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
      title: '模型类型',
      dataIndex: 'modelType',
      key: 'modelType',
      width: 120,
      render: (type) => getModelTypeLabel(type),
    },
    {
      title: '预测目标',
      dataIndex: 'target',
      key: 'target',
      width: 120,
      render: (target) => <Text>{getTargetLabel(target)}</Text>,
    },
    {
      title: '训练期间',
      key: 'period',
      width: 180,
      render: (_, record) => (
        <Text style={{ fontSize: 12 }}>
          {record.startDate} 至 {record.endDate}
        </Text>
      ),
    },
    {
      title: '准确率/MAE',
      key: 'metrics',
      width: 160,
      render: (_, record) => {
        // Walk-forward模型显示CV指标
        if (record.walkForward && record.cvScore != null) {
          const isClassification = record.cvMetric === 'accuracy';
          const scoreDisplay = isClassification
            ? `${(record.cvScore * 100).toFixed(1)}%`
            : record.cvScore.toFixed(4);
          const stdDisplay = record.cvStd != null
            ? (isClassification
              ? `±${(record.cvStd * 100).toFixed(1)}%`
              : `±${record.cvStd.toFixed(4)}`)
            : '';
          return (
            <div>
              <Tag color="cyan" style={{ fontSize: 10 }}>WF {record.nSplits}折</Tag>
              <div>
                <Text strong>{scoreDisplay}</Text>
                {stdDisplay && <Text type="secondary" style={{ fontSize: 11 }}> {stdDisplay}</Text>}
              </div>
              <Text type="secondary" style={{ fontSize: 11 }}>CV {record.cvMetric}</Text>
            </div>
          );
        }
        // 优先显示准确率（分类指标）
        if (record.accuracy != null) {
          return (
            <div>
              <Progress
                percent={Math.round(record.accuracy * 100)}
                size="small"
                format={(percent) => `${percent}%`}
              />
            </div>
          );
        }
        // 否则显示MAE（回归指标）
        else if (record.mae != null) {
          return (
            <div>
              <Text strong>{record.mae.toFixed(4)}</Text>
              <Text type="secondary" style={{ fontSize: 12 }}> (MAE)</Text>
            </div>
          );
        }
        return <Text type="secondary">-</Text>;
      },
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status) => getStatusTag(status),
    },
    {
      title: '创建时间',
      dataIndex: 'createdAt',
      key: 'createdAt',
      width: 170,
      render: (time) => formatDate(time, 'YYYY-MM-DD HH:mm'),
    },
    {
      title: '操作',
      key: 'action',
      width: 180,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          {record.status === 'completed' && (
            <>
              <Button
                size="small"
                icon={<EyeOutlined />}
                onClick={() => onView?.(record)}
              >
                详情
              </Button>
              <Button
                size="small"
                type="primary"
                icon={<LineChartOutlined />}
                onClick={() => onPredict?.(record)}
              >
                预测
              </Button>
            </>
          )}
          <Popconfirm
            title="确认删除"
            description="确定要删除此模型吗？"
            onConfirm={() => handleDelete(record.id)}
            okText="确定"
            cancelText="取消"
          >
            <Button size="small" danger icon={<DeleteOutlined />}>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Card title={<Text strong>已训练模型</Text>}>
      <Table
        columns={columns}
        dataSource={models}
        rowKey="id"
        loading={loading}
        scroll={{ x: 1250 }}
        pagination={{
          current: pagination.current,
          pageSize: pagination.pageSize,
          pageSizeOptions: ['10', '20', '50', '100'],
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 个模型`,
          hideOnSinglePage: false,
          onChange: (page, pageSize) => {
            setPagination({ current: page, pageSize });
          },
        }}
      />
    </Card>
  );
}
