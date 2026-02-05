import { useState } from 'react';
import { Table, Button, Space, Popconfirm, Input, message, Tag, Typography } from 'antd';
import { DeleteOutlined, SearchOutlined, StarFilled, StarOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { FavoriteStock } from '@/stores/favoriteStore';
import { useFavoriteStore } from '@/stores';
import { formatDate } from '@/utils';

const { Text } = Typography;

interface FavoritesListProps {
  selectedRowKeys?: string[];
  onSelectionChange?: (selectedRowKeys: string[]) => void;
  showSelection?: boolean;
}

export function FavoritesList({
  selectedRowKeys = [],
  onSelectionChange,
  showSelection = false,
}: FavoritesListProps) {
  const { favorites, removeFavorite, isInFavorites } = useFavoriteStore();
  const [searchText, setSearchText] = useState('');

  // 过滤收藏列表
  const filteredFavorites = favorites.filter((f) => {
    if (!searchText) return true;
    const search = searchText.toLowerCase();
    return f.code.toLowerCase().includes(search) || f.name.toLowerCase().includes(search);
  });

  // 删除收藏
  const handleRemove = (code: string, name: string) => {
    removeFavorite(code);
    message.success(`已删除收藏：${name} (${code})`);
  };

  // 批量删除
  const handleBatchRemove = () => {
    if (selectedRowKeys.length === 0) {
      message.warning('请先选择要删除的股票');
      return;
    }

    selectedRowKeys.forEach((code) => {
      removeFavorite(code);
    });

    message.success(`已删除 ${selectedRowKeys.length} 只股票`);
    onSelectionChange?.([]);
  };

  const columns: ColumnsType<FavoriteStock> = [
    {
      title: '代码',
      dataIndex: 'code',
      key: 'code',
      width: 120,
      sorter: (a, b) => a.code.localeCompare(b.code),
      render: (code) => <Text strong>{code}</Text>,
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (name) => <Text>{name}</Text>,
    },
    {
      title: '添加时间',
      dataIndex: 'addedAt',
      key: 'addedAt',
      width: 180,
      sorter: (a, b) => new Date(a.addedAt).getTime() - new Date(b.addedAt).getTime(),
      render: (date) => <Text type="secondary">{formatDate(date, 'YYYY-MM-DD HH:mm')}</Text>,
    },
    {
      title: '操作',
      key: 'action',
      width: 100,
      render: (_, record) => (
        <Popconfirm
          title="确认删除"
          description={`确定要删除 ${record.name} (${record.code}) 吗？`}
          onConfirm={() => handleRemove(record.code, record.name)}
          okText="确定"
          cancelText="取消"
        >
          <Button size="small" danger icon={<DeleteOutlined aria-hidden="true" />}>
            删除
          </Button>
        </Popconfirm>
      ),
    },
  ];

  // 行选择配置
  const rowSelection = showSelection
    ? {
        selectedRowKeys,
        onChange: (selectedRowKeys: React.Key[]) => {
          onSelectionChange?.(selectedRowKeys as string[]);
        },
      }
    : undefined;

  return (
    <div>
      {/* 工具栏 */}
      <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <Space>
          <Input
            placeholder="搜索股票代码或名称"
            prefix={<SearchOutlined aria-hidden="true" />}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 250 }}
            allowClear
          />
          <Tag color="blue">共 {favorites.length} 只股票</Tag>
        </Space>

        {showSelection && selectedRowKeys.length > 0 && (
          <Space>
            <Text>已选 {selectedRowKeys.length} 只</Text>
            <Popconfirm
              title="确认删除"
              description={`确定要删除选中的 ${selectedRowKeys.length} 只股票吗？`}
              onConfirm={handleBatchRemove}
              okText="确定"
              cancelText="取消"
            >
              <Button danger icon={<DeleteOutlined aria-hidden="true" />}>
                批量删除
              </Button>
            </Popconfirm>
          </Space>
        )}
      </div>

      {/* 收藏列表 */}
      <Table
        columns={columns}
        dataSource={filteredFavorites}
        rowKey="code"
        rowSelection={rowSelection}
        pagination={{
          pageSize: 20,
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 只`,
        }}
        size="small"
      />

      {/* 空状态提示 */}
      {favorites.length === 0 && (
        <div style={{ textAlign: 'center', padding: '40px 0', color: '#999' }}>
          <StarOutlined style={{ fontSize: 48, marginBottom: 16 }} aria-hidden="true" />
          <div>还没有收藏任何股票</div>
          <div style={{ fontSize: 12, marginTop: 8 }}>
            在查询结果或AI筛选中点击"收藏"按钮添加股票
          </div>
        </div>
      )}
    </div>
  );
}
