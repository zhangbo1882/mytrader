import { useEffect, useState } from 'react';
import { Table, Button, Space, message, Popconfirm, Modal, Descriptions, Tag, Spin } from 'antd';
import { EyeOutlined, ReloadOutlined, DeleteOutlined, HistoryOutlined, StarOutlined } from '@ant-design/icons';
import { screeningService } from '@/services';
import { useFavoriteStore } from '@/stores';
import type { ScreeningHistory, ScreeningHistoryDetail } from '@/types';

interface ScreeningHistoryProps {
  onLoadHistory?: (detail: ScreeningHistoryDetail) => void;
  onReRunComplete?: () => void; // Called when re-run completes, can be used to switch tabs
  refreshTrigger?: number; // Triggers reload when value changes
}

function ScreeningHistory({ onLoadHistory, onReRunComplete, refreshTrigger }: ScreeningHistoryProps) {
  const [loading, setLoading] = useState(false);
  const [histories, setHistories] = useState<ScreeningHistory[]>([]);
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [selectedDetail, setSelectedDetail] = useState<ScreeningHistoryDetail | null>(null);
  const [reRunning, setReRunning] = useState<number | null>(null);
  const [detailPagination, setDetailPagination] = useState({ current: 1, pageSize: 50 });
  const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
  const [batchAdding, setBatchAdding] = useState(false);
  const { batchAddFavorites, addFavorite, isInFavorites } = useFavoriteStore();

  const loadHistories = async () => {
    setLoading(true);
    try {
      const response = await screeningService.getHistory();
      if (response.success) {
        setHistories(response.history);
      }
    } catch (err) {
      message.error(err instanceof Error ? err.message : '加载历史记录失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadHistories();
  }, [refreshTrigger]);

  const handleViewDetail = async (id: number) => {
    setLoading(true);
    try {
      const response = await screeningService.getHistoryDetail(id);
      if (response.success) {
        setSelectedDetail(response.detail);
        setDetailPagination({ current: 1, pageSize: 50 }); // Reset pagination when opening detail
        setDetailModalVisible(true);
      } else {
        message.error('获取详情失败');
      }
    } catch (err) {
      message.error(err instanceof Error ? err.message : '加载详情失败');
    } finally {
      setLoading(false);
    }
  };

  const handleReRun = async (id: number) => {
    setReRunning(id);
    try {
      // First get historical detail to get the config and metadata
      const detailResponse = await screeningService.getHistoryDetail(id);
      if (!detailResponse.success) {
        message.error('获取历史记录详情失败');
        return;
      }

      // Then re-run the screening with the same configuration
      const response = await screeningService.reRunHistory(id, 100);
      if (response.success) {
        message.success(`重新筛选完成，共找到 ${response.count} 只股票`);

        // Create a combined detail object with re-run results
        const reRunDetail: ScreeningHistoryDetail = {
          ...detailResponse.detail,
          result_count: response.count,
          stocks_count: response.count,
          stocks: response.stocks
        };

        // Pass the updated detail to parent
        if (onLoadHistory) {
          onLoadHistory(reRunDetail);
        }

        // Notify parent that re-run is complete (can be used to switch tabs)
        if (onReRunComplete) {
          onReRunComplete();
        }
      }
    } catch (err) {
      message.error(err instanceof Error ? err.message : '重新筛选失败');
    } finally {
      setReRunning(null);
    }
  };

  const handleDelete = async (id: number) => {
    try {
      const response = await screeningService.deleteHistory(id);
      if (response.success) {
        message.success('删除成功');
        loadHistories();
      }
    } catch (err) {
      message.error(err instanceof Error ? err.message : '删除失败');
    }
  };

  const handleBatchAddFavorites = async () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请先选择要收藏的股票');
      return;
    }

    // 过滤已收藏的股票
    const newCodes = selectedRowKeys.filter((code) => !isInFavorites(code));

    if (newCodes.length === 0) {
      message.warning('所选股票已在收藏列表中');
      return;
    }

    setBatchAdding(true);
    try {
      const response = await batchAddFavorites(newCodes);
      setSelectedRowKeys([]);

      if (response.failed === 0) {
        message.success(`成功收藏 ${response.success} 只股票`);
      } else {
        message.warning(
          `收藏完成：成功 ${response.success} 只，失败 ${response.failed} 只`
        );
      }
    } catch (error) {
      message.error(error instanceof Error ? error.message : '批量收藏失败');
    } finally {
      setBatchAdding(false);
    }
  };

  const handleAddSingleFavorite = async (stock: any) => {
    if (isInFavorites(stock.code)) {
      return;
    }

    try {
      await addFavorite(stock.code, stock.name);
      message.success(`已收藏：${stock.name} (${stock.code})`);
    } catch (error) {
      message.error(error instanceof Error ? error.message : '收藏失败');
    }
  };

  const columns = [
    {
      title: 'ID',
      dataIndex: 'id',
      key: 'id',
      width: 80,
    },
    {
      title: '筛选名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => <Tag color="blue">{name}</Tag>
    },
    {
      title: '结果数量',
      dataIndex: 'result_count',
      key: 'result_count',
      width: 120,
      render: (count: number) => `${count} 只`
    },
    {
      title: '保存股票数',
      dataIndex: 'stocks_count',
      key: 'stocks_count',
      width: 120,
      render: (count: number) => `${count} 只`
    },
    {
      title: '创建时间',
      dataIndex: 'created_at',
      key: 'created_at',
      width: 180,
    },
    {
      title: '操作',
      key: 'action',
      width: 200,
      render: (_: unknown, record: ScreeningHistory) => (
        <Space size="small">
          <Button
            type="link"
            size="small"
            icon={<EyeOutlined />}
            onClick={() => handleViewDetail(record.id)}
          >
            详情
          </Button>
          <Button
            type="link"
            size="small"
            icon={<ReloadOutlined />}
            loading={reRunning === record.id}
            onClick={() => handleReRun(record.id)}
          >
            重跑
          </Button>
          <Popconfirm
            title="确认删除"
            description="确定要删除这条历史记录吗？"
            onConfirm={() => handleDelete(record.id)}
            okText="确定"
            cancelText="取消"
          >
            <Button
              type="link"
              size="small"
              danger
              icon={<DeleteOutlined />}
            >
              删除
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div>
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span style={{ fontSize: 14, color: '#666' }}>
          <HistoryOutlined style={{ marginRight: 8 }} />
          共 {histories.length} 条历史记录
        </span>
        <Button onClick={loadHistories} loading={loading}>
          刷新
        </Button>
      </div>

      <Table
        columns={columns}
        dataSource={histories}
        rowKey="id"
        loading={loading}
        pagination={{
          pageSize: 10,
          pageSizeOptions: ['10', '20', '50', '100'],
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 条`,
          hideOnSinglePage: false,
        }}
        size="small"
      />

      {/* Detail Modal */}
      <Modal
        title="筛选历史详情"
        open={detailModalVisible}
        onCancel={() => {
          setDetailModalVisible(false);
          setSelectedRowKeys([]);
        }}
        footer={null}
        width={1200}
        style={{ top: 20 }}
        bodyStyle={{ maxHeight: 'calc(100vh - 200px)', overflow: 'auto' }}
      >
        {selectedDetail && (
          <Spin spinning={loading}>
            <Descriptions bordered column={2} size="small">
              <Descriptions.Item label="ID">{selectedDetail.id}</Descriptions.Item>
              <Descriptions.Item label="名称">{selectedDetail.name}</Descriptions.Item>
              <Descriptions.Item label="结果数量">{selectedDetail.result_count}</Descriptions.Item>
              <Descriptions.Item label="保存股票数">{selectedDetail.stocks_count}</Descriptions.Item>
              <Descriptions.Item label="创建时间" span={2}>{selectedDetail.created_at}</Descriptions.Item>
            </Descriptions>

            <div style={{ marginTop: 16 }}>
              <h4>筛选配置:</h4>
              <pre style={{
                background: '#f5f5f5',
                padding: 12,
                borderRadius: 4,
                fontSize: 12,
                maxHeight: 150,
                overflow: 'auto'
              }}>
                {JSON.stringify(selectedDetail.config, null, 2)}
              </pre>
            </div>

            {(selectedDetail.stocks?.length ?? 0) > 0 && (
              <div style={{ marginTop: 16 }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                  <h4 style={{ margin: 0 }}>股票列表 (共 {selectedDetail.stocks.length} 只):</h4>
                  {selectedRowKeys.length > 0 && (
                    <Space>
                      <span>已选 {selectedRowKeys.length} 只</span>
                      <Button
                        type="primary"
                        icon={<StarOutlined />}
                        onClick={handleBatchAddFavorites}
                        loading={batchAdding}
                      >
                        批量收藏
                      </Button>
                    </Space>
                  )}
                </div>
                <Table
                  dataSource={selectedDetail.stocks}
                  rowKey="code"
                  pagination={{
                    current: detailPagination.current,
                    pageSize: detailPagination.pageSize,
                    pageSizeOptions: ['20', '50', '100', '200'],
                    showSizeChanger: true,
                    showTotal: (total) => `共 ${total} 只股票`,
                    showQuickJumper: true,
                    onChange: (page, pageSize) => {
                      setDetailPagination({ current: page, pageSize });
                    }
                  }}
                  rowSelection={{
                    selectedRowKeys,
                    onChange: (keys) => setSelectedRowKeys(keys as string[]),
                    getCheckboxProps: (record) => ({
                      disabled: isInFavorites(record.code),
                    }),
                  }}
                  columns={[
                    { title: '代码', dataIndex: 'code', key: 'code', width: 100 },
                    { title: '名称', dataIndex: 'name', key: 'name', width: 120 },
                    { title: '最新价', dataIndex: 'latest_close', key: 'latest_close', width: 100, render: (v) => v ? v.toFixed(2) : '-', align: 'right' },
                    { title: 'PE(TTM)', dataIndex: 'pe_ttm', key: 'pe_ttm', width: 100, render: (v) => v ? v.toFixed(2) : '-', align: 'right' },
                    { title: 'PB', dataIndex: 'pb', key: 'pb', width: 100, render: (v) => v ? v.toFixed(2) : '-', align: 'right' },
                    { title: '总市值(亿)', dataIndex: 'total_mv_yi', key: 'total_mv_yi', width: 120, render: (v) => v ? v.toFixed(2) : '-', align: 'right' },
                    {
                      title: '操作',
                      key: 'action',
                      width: 100,
                      fixed: 'right' as const,
                      render: (_, record) => (
                        <Button
                          size="small"
                          type={isInFavorites(record.code) ? 'default' : 'primary'}
                          onClick={() => handleAddSingleFavorite(record)}
                          disabled={isInFavorites(record.code)}
                        >
                          {isInFavorites(record.code) ? '已收藏' : '收藏'}
                        </Button>
                      ),
                    },
                  ]}
                  size="small"
                  scroll={{ x: 800 }}
                />
              </div>
            )}
          </Spin>
        )}
      </Modal>
    </div>
  );
}

export default ScreeningHistory;
