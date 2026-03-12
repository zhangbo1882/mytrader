import { useEffect, useState } from 'react';
import { Button, Card, Col, Input, message, Modal, Popconfirm, Row, Select, Space, Statistic, Table, Tabs } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { DeleteOutlined, EyeOutlined, ReloadOutlined, SearchOutlined } from '@ant-design/icons';
import { backtestService } from '@/services';
import type {
  BacktestHistory as BacktestHistoryItem,
  BacktestHistoryDetail,
  BacktestHistoryFilters,
} from '@/types';
import { BasicInfoCards, BenchmarkComparison, EquityCurve, HealthMetrics, TradeTable } from '@/components/backtest';

interface BacktestHistoryProps {
  refreshTrigger?: number;
}

function BacktestHistory({ refreshTrigger = 0 }: BacktestHistoryProps) {
  const [loading, setLoading] = useState(false);
  const [histories, setHistories] = useState<BacktestHistoryItem[]>([]);
  const [total, setTotal] = useState(0);
  const [filters, setFilters] = useState<BacktestHistoryFilters>({
    page: 1,
    page_size: 20,
    stock: '',
    strategy: '',
  });
  const [detailVisible, setDetailVisible] = useState(false);
  const [selectedDetail, setSelectedDetail] = useState<BacktestHistoryDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  const loadHistory = async () => {
    setLoading(true);
    try {
      const response = await backtestService.getHistory(filters);
      setHistories(response.history);
      setTotal(response.total);
    } catch (err) {
      message.error(err instanceof Error ? err.message : '加载历史记录失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadHistory();
  }, [refreshTrigger, filters.page, filters.page_size]);

  const handleViewDetail = async (taskId: string) => {
    setDetailLoading(true);
    setDetailVisible(true);
    try {
      const response = await backtestService.getHistoryDetail(taskId);
      if (response.success) {
        setSelectedDetail(response.detail);
      } else {
        message.error(response.message || '加载详情失败');
        setDetailVisible(false);
      }
    } catch (err) {
      message.error(err instanceof Error ? err.message : '加载详情失败');
      setDetailVisible(false);
    } finally {
      setDetailLoading(false);
    }
  };

  const handleDelete = async (taskId: string) => {
    try {
      await backtestService.deleteHistory(taskId);
      message.success('删除成功');
      loadHistory();
    } catch (err) {
      message.error(err instanceof Error ? err.message : '删除失败');
    }
  };

  const handleFilterChange = <K extends keyof BacktestHistoryFilters>(key: K, value: BacktestHistoryFilters[K]) => {
    setFilters((prev) => ({ ...prev, [key]: value, page: 1 }));
  };

  const handleSearch = () => {
    loadHistory();
  };

  const handleReset = () => {
    setFilters({ page: 1, page_size: 20, stock: '', strategy: '' });
  };

  const columns: ColumnsType<BacktestHistoryItem> = [
    {
      title: 'ID',
      dataIndex: 'task_id',
      width: 150,
      ellipsis: true,
    },
    {
      title: '名称',
      dataIndex: 'name',
      width: 200,
      ellipsis: true,
    },
    {
      title: '股票',
      dataIndex: 'stock',
      width: 100,
    },
    {
      title: '股票名称',
      dataIndex: 'stock_name',
      width: 150,
      ellipsis: true,
    },
    {
      title: '策略',
      dataIndex: 'strategy_name',
      width: 200,
      ellipsis: true,
    },
    {
      title: '收益率',
      dataIndex: 'total_return',
      width: 100,
      render: (value: number) => `${(value * 100).toFixed(2)}%`,
      sorter: (a, b) => a.total_return - b.total_return,
    },
    {
      title: '夏普比率',
      dataIndex: 'sharpe_ratio',
      width: 100,
      render: (value: number) => value?.toFixed(2),
      sorter: (a, b) => (a.sharpe_ratio || 0) - (b.sharpe_ratio || 0),
    },
    {
      title: '最大回撤',
      dataIndex: 'max_drawdown',
      width: 100,
      render: (value: number) => `${(value * 100).toFixed(2)}%`,
      sorter: (a, b) => a.max_drawdown - b.max_drawdown,
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      width: 180,
      sorter: (a, b) => a.created_at.localeCompare(b.created_at),
    },
    {
      title: '操作',
      key: 'actions',
      width: 150,
      fixed: 'right',
      render: (_, record) => (
        <Space>
          <Button
            type="link"
            size="small"
            icon={<EyeOutlined />}
            onClick={() => handleViewDetail(record.task_id)}
          >
            查看
          </Button>
          <Popconfirm
            title="确定要删除这条历史记录吗？"
            onConfirm={() => handleDelete(record.task_id)}
            okText="确定"
            cancelText="取消"
          >
            <Button type="link" size="small" danger icon={<DeleteOutlined />}>
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={16}>
          <Col span={6}>
            <Input
              placeholder="股票代码"
              value={filters.stock}
              onChange={(e) => handleFilterChange('stock', e.target.value)}
              onPressEnter={handleSearch}
            />
          </Col>
          <Col span={6}>
            <Select
              placeholder="策略类型"
              value={filters.strategy || undefined}
              onChange={(value) => handleFilterChange('strategy', value)}
              allowClear
              style={{ width: '100%' }}
              options={[
                { value: 'sma_cross', label: '简单移动平均线交叉策略' },
                { value: 'price_breakout', label: '价格突破策略' },
              ]}
            />
          </Col>
          <Col span={12}>
            <Space>
              <Button type="primary" icon={<SearchOutlined />} onClick={handleSearch}>
                搜索
              </Button>
              <Button icon={<ReloadOutlined />} onClick={handleReset}>
                重置
              </Button>
              <Button icon={<ReloadOutlined />} onClick={loadHistory}>
                刷新
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={6}>
          <Card>
            <Statistic title="总回测次数" value={total} />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="平均收益率"
              value={histories.length > 0
                ? (histories.reduce((sum, history) => sum + history.total_return, 0) / histories.length * 100).toFixed(2)
                : 0}
              suffix="%"
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="最佳收益率"
              value={histories.length > 0
                ? (Math.max(...histories.map((history) => history.total_return)) * 100).toFixed(2)
                : 0}
              suffix="%"
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="最差收益率"
              value={histories.length > 0
                ? (Math.min(...histories.map((history) => history.total_return)) * 100).toFixed(2)
                : 0}
              suffix="%"
            />
          </Card>
        </Col>
      </Row>

      <Table
        columns={columns}
        dataSource={histories}
        rowKey="task_id"
        loading={loading}
        pagination={{
          total,
          current: filters.page,
          pageSize: filters.page_size,
          pageSizeOptions: ['10', '20', '50', '100'],
          showSizeChanger: true,
          showTotal: (count) => `共 ${count} 条记录`,
          onChange: (page, pageSize) => {
            setFilters((prev) => ({ ...prev, page, page_size: pageSize }));
          },
          hideOnSinglePage: false,
        }}
        scroll={{ x: 1200 }}
      />

      <Modal
        title="回测详情"
        open={detailVisible}
        onCancel={() => setDetailVisible(false)}
        width={1200}
        footer={null}
        style={{ top: 20 }}
      >
        {detailLoading ? (
          <div style={{ textAlign: 'center', padding: 40 }}>加载中...</div>
        ) : selectedDetail ? (
          <Tabs
            defaultActiveKey="basic"
            items={[
              {
                key: 'basic',
                label: '基础信息',
                children: <BasicInfoCards result={selectedDetail.result} />,
              },
              {
                key: 'trades',
                label: `交易明细 (${selectedDetail.result.trades.length})`,
                children: <TradeTable trades={selectedDetail.result.trades} />,
              },
              {
                key: 'metrics',
                label: '健康指标',
                children: <HealthMetrics metrics={selectedDetail.result.health_metrics} />,
              },
              {
                key: 'curve',
                label: '收益曲线',
                children: <EquityCurve result={selectedDetail.result} />,
              },
              {
                key: 'benchmark',
                label: '基准对比',
                children: selectedDetail.result.benchmark_comparison ? (
                  <BenchmarkComparison comparison={selectedDetail.result.benchmark_comparison} />
                ) : (
                  <div style={{ padding: 24, textAlign: 'center', color: '#999' }}>
                    未设置基准指数
                  </div>
                ),
              },
            ]}
          />
        ) : null}
      </Modal>
    </div>
  );
}

export default BacktestHistory;
