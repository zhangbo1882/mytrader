import { useState, useEffect, useMemo } from 'react';
import { Table, Button, Space, Popconfirm, Input, message, Tag, Typography, Modal, Form, Select, InputNumber, Tooltip } from 'antd';
import { DeleteOutlined, SearchOutlined, StarOutlined, EditOutlined, FilterOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { FavoriteStock } from '@/stores/favoriteStore';
import { useFavoriteStore } from '@/stores';
import { formatDate } from '@/utils';
import { stockService } from '@/services';

const { Text } = Typography;

interface FavoritesListProps {
  selectedRowKeys?: string[];
  onSelectionChange?: (selectedRowKeys: string[]) => void;
  showSelection?: boolean;
}

interface PriceData {
  close: number;
  date: string;
}

// 评级选项
const RATING_OPTIONS = [
  { value: 'A', label: 'A' },
  { value: 'B', label: 'B' },
  { value: 'C', label: 'C' },
  { value: 'D', label: 'D' },
  { value: 'E', label: 'E' },
];

// 评级转换为数字用于排序
const ratingToNumber = (rating: string | null): number => {
  if (!rating) return 999; // 无评级的排最后
  const map: Record<string, number> = {
    'A': 1, 'A-': 2, 'B+': 3, 'B': 4, 'B-': 5,
    'C+': 6, 'C': 7, 'C-': 8, 'D': 9, 'E': 10
  };
  return map[rating] ?? 999;
};

export function FavoritesList({
  selectedRowKeys = [],
  onSelectionChange,
  showSelection = false,
}: FavoritesListProps) {
  const { favorites, removeFavorite, updateFavorite, isInFavorites } = useFavoriteStore();
  const [searchText, setSearchText] = useState('');
  const [pagination, setPagination] = useState({ current: 1, pageSize: 20 });
  const [priceMap, setPriceMap] = useState<Record<string, PriceData>>({});
  const [loadingPrices, setLoadingPrices] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editingStock, setEditingStock] = useState<FavoriteStock | null>(null);
  const [editForm] = Form.useForm();

  // 过滤条件
  const [safetyFilter, setSafetyFilter] = useState<string[]>([]);
  const [fundamentalFilter, setFundamentalFilter] = useState<string[]>([]);

  // 获取最新价格
  const loadLatestPrices = async () => {
    if (favorites.length === 0) return;

    setLoadingPrices(true);
    try {
      const codes = favorites.map(f => f.code);
      // 使用最近30天的日期范围，确保能获取到最新数据
      const today = new Date();
      const thirtyDaysAgo = new Date(today);
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

      const formatDateStr = (d: Date) => d.toISOString().split('T')[0];

      const response = await stockService.query({
        symbols: codes,
        startDate: formatDateStr(thirtyDaysAgo),
        endDate: formatDateStr(today),
        interval: '1d',
        priceType: 'bfq'  // 不复权，获取真实收盘价
      });

      // 构建价格映射 - response 直接是 { code: [bars] } 格式
      const newPriceMap: Record<string, PriceData> = {};
      for (const [code, bars] of Object.entries(response)) {
        const barArray = bars as any[];
        if (barArray && barArray.length > 0) {
          // 获取最后一条数据（最新的）
          const lastBar = barArray[barArray.length - 1];
          newPriceMap[code] = {
            close: lastBar.close,
            date: lastBar.datetime
          };
        }
      }
      setPriceMap(newPriceMap);
    } catch (error) {
      console.error('Failed to load prices:', error);
    } finally {
      setLoadingPrices(false);
    }
  };

  // 当收藏列表变化时加载价格
  useEffect(() => {
    loadLatestPrices();
  }, [favorites.map(f => f.code).join(',')]);

  // 计算差距百分比
  const calcChangePercent = (entryPrice: number | null, closePrice: number): number | null => {
    if (!entryPrice || entryPrice <= 0) return null;
    return ((closePrice - entryPrice) / entryPrice) * 100;
  };

  // 格式化价格显示
  const formatPriceDisplay = (priceData: PriceData | undefined) => {
    if (!priceData) return '-';
    return `${priceData.close.toFixed(2)} (${priceData.date})`;
  };

  // 过滤和排序后的收藏列表
  const filteredFavorites = useMemo(() => {
    let result = favorites.filter((f) => {
      // 文本搜索
      if (searchText) {
        const search = searchText.toLowerCase();
        if (!f.code.toLowerCase().includes(search) && !f.name.toLowerCase().includes(search)) {
          return false;
        }
      }

      // 安全性评级过滤（多选）
      if (safetyFilter.length > 0 && !safetyFilter.includes(f.safetyRating || '')) {
        return false;
      }

      // 基本面评级过滤（多选）
      if (fundamentalFilter.length > 0 && !fundamentalFilter.includes(f.fundamentalRating || '')) {
        return false;
      }

      return true;
    });

    return result;
  }, [favorites, searchText, safetyFilter, fundamentalFilter]);

  // 清除所有过滤条件
  const clearFilters = () => {
    setSearchText('');
    setSafetyFilter([]);
    setFundamentalFilter([]);
  };

  // 是否有活动的过滤条件
  const hasActiveFilters = searchText || safetyFilter.length > 0 || fundamentalFilter.length > 0;

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

  // 打开编辑弹窗
  const handleEdit = (stock: FavoriteStock) => {
    setEditingStock(stock);
    editForm.setFieldsValue({
      safety_rating: stock.safetyRating || undefined,
      fundamental_rating: stock.fundamentalRating || undefined,
      entry_price: stock.entryPrice || undefined,
    });
    setEditModalVisible(true);
  };

  // 保存编辑
  const handleSaveEdit = async () => {
    if (!editingStock) return;

    try {
      const values = await editForm.validateFields();
      await updateFavorite(editingStock.code, {
        safety_rating: values.safety_rating || null,
        fundamental_rating: values.fundamental_rating || null,
        entry_price: values.entry_price || null,
      });
      message.success('更新成功');
      setEditModalVisible(false);
      setEditingStock(null);
    } catch (error) {
      message.error('更新失败');
    }
  };

  const columns: ColumnsType<FavoriteStock> = [
    {
      title: '代码',
      dataIndex: 'code',
      key: 'code',
      width: 100,
      fixed: 'left',
      sorter: (a, b) => a.code.localeCompare(b.code),
      render: (code) => <Text strong>{code}</Text>,
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      width: 100,
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (name) => <Text>{name}</Text>,
    },
    {
      title: '安全性',
      dataIndex: 'safetyRating',
      key: 'safetyRating',
      width: 80,
      align: 'center',
      sorter: (a, b) => ratingToNumber(a.safetyRating) - ratingToNumber(b.safetyRating),
      render: (rating) => rating ? (
        <Tag color={rating === 'A' ? 'green' : rating === 'B' ? 'blue' : rating === 'C' ? 'orange' : 'red'}>
          {rating}
        </Tag>
      ) : '-',
    },
    {
      title: '基本面',
      dataIndex: 'fundamentalRating',
      key: 'fundamentalRating',
      width: 80,
      align: 'center',
      sorter: (a, b) => ratingToNumber(a.fundamentalRating) - ratingToNumber(b.fundamentalRating),
      render: (rating) => rating ? (
        <Tag color={rating === 'A' ? 'green' : rating === 'B' ? 'blue' : rating === 'C' ? 'orange' : 'red'}>
          {rating}
        </Tag>
      ) : '-',
    },
    {
      title: '进场价',
      dataIndex: 'entryPrice',
      key: 'entryPrice',
      width: 90,
      align: 'right',
      sorter: (a, b) => (a.entryPrice || 0) - (b.entryPrice || 0),
      render: (price) => price ? <Text>{price.toFixed(2)}</Text> : '-',
    },
    {
      title: '最新收盘价',
      key: 'latestClose',
      width: 150,
      align: 'right',
      render: (_, record) => {
        const priceData = priceMap[record.code];
        return loadingPrices ? '加载中...' : formatPriceDisplay(priceData);
      },
    },
    {
      title: '差距%',
      key: 'changePercent',
      width: 100,
      align: 'right',
      sorter: (a, b) => {
        const aPrice = priceMap[a.code];
        const bPrice = priceMap[b.code];
        const aChange = aPrice && a.entryPrice ? calcChangePercent(a.entryPrice, aPrice.close) || 0 : -999999;
        const bChange = bPrice && b.entryPrice ? calcChangePercent(b.entryPrice, bPrice.close) || 0 : -999999;
        return aChange - bChange;
      },
      render: (_, record) => {
        const priceData = priceMap[record.code];
        if (!priceData || !record.entryPrice) return '-';

        const changeValue = calcChangePercent(record.entryPrice, priceData.close);
        if (changeValue === null) return '-';

        const color = changeValue > 0 ? '#52c41a' : changeValue < 0 ? '#ff4d4f' : '#666';

        return (
          <Text style={{ color, fontWeight: 'bold' }}>
            {changeValue > 0 ? '+' : ''}{changeValue.toFixed(2)}%
          </Text>
        );
      },
    },
    {
      title: '添加时间',
      dataIndex: 'addedAt',
      key: 'addedAt',
      width: 140,
      sorter: (a, b) => new Date(a.addedAt).getTime() - new Date(b.addedAt).getTime(),
      render: (date) => <Text type="secondary">{formatDate(date, 'YYYY-MM-DD HH:mm')}</Text>,
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="编辑">
            <Button
              size="small"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
            />
          </Tooltip>
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
        </Space>
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
      <div style={{ marginBottom: 16 }}>
        <Space wrap>
          <Input
            placeholder="搜索股票代码或名称"
            prefix={<SearchOutlined aria-hidden="true" />}
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            style={{ width: 200 }}
            allowClear
          />

          <Select
            mode="multiple"
            placeholder="安全性评级"
            style={{ width: 150 }}
            value={safetyFilter}
            onChange={setSafetyFilter}
            allowClear
            maxTagCount="responsive"
            options={RATING_OPTIONS}
          />

          <Select
            mode="multiple"
            placeholder="基本面评级"
            style={{ width: 150 }}
            value={fundamentalFilter}
            onChange={setFundamentalFilter}
            allowClear
            maxTagCount="responsive"
            options={RATING_OPTIONS}
          />

          {hasActiveFilters && (
            <Button onClick={clearFilters}>
              清除筛选
            </Button>
          )}

          <Tag color="blue">共 {favorites.length} 只</Tag>
          {filteredFavorites.length !== favorites.length && (
            <Tag color="orange">筛选后 {filteredFavorites.length} 只</Tag>
          )}

          <Button onClick={loadLatestPrices} loading={loadingPrices}>
            刷新价格
          </Button>
        </Space>

        {showSelection && selectedRowKeys.length > 0 && (
          <Space style={{ marginLeft: 16 }}>
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
          current: pagination.current,
          pageSize: pagination.pageSize,
          pageSizeOptions: ['20', '50', '100', '200'],
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 只`,
          hideOnSinglePage: false,
          onChange: (page, pageSize) => {
            setPagination({ current: page, pageSize });
          },
        }}
        size="small"
        scroll={{ x: 1100 }}
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

      {/* 编辑弹窗 */}
      <Modal
        title={`编辑 ${editingStock?.name} (${editingStock?.code})`}
        open={editModalVisible}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalVisible(false);
          setEditingStock(null);
        }}
        okText="保存"
        cancelText="取消"
      >
        <Form form={editForm} layout="vertical">
          <Form.Item name="safety_rating" label="安全性评级">
            <Select placeholder="选择安全性评级" allowClear>
              {RATING_OPTIONS.map(opt => (
                <Select.Option key={opt.value} value={opt.value}>{opt.label}</Select.Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="fundamental_rating" label="基本面评级">
            <Select placeholder="选择基本面评级" allowClear>
              {RATING_OPTIONS.map(opt => (
                <Select.Option key={opt.value} value={opt.value}>{opt.label}</Select.Option>
              ))}
            </Select>
          </Form.Item>
          <Form.Item name="entry_price" label="进场价格">
            <InputNumber
              placeholder="输入进场价格"
              min={0}
              precision={2}
              style={{ width: '100%' }}
            />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
