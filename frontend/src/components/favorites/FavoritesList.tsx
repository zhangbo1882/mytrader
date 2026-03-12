import { useState, useEffect, useMemo } from 'react';
import { Table, Button, Space, Popconfirm, Input, message, Tag, Typography, Modal, Form, Select, InputNumber, Tooltip, Rate, Progress } from 'antd';
import { DeleteOutlined, SearchOutlined, StarOutlined, EditOutlined, WalletOutlined, ReloadOutlined, SyncOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { FavoriteStock } from '@/stores/favoriteStore';
import { useFavoriteStore } from '@/stores';
import { formatDate } from '@/utils';
import { stockService, favoriteService, valuationService } from '@/services';
import { loadPositions as loadRiskPositions } from '@/services/riskService';
import type { PositionOutput } from '@/types/risk.types';
import type { ValuationMethod, CombineMethod, DCFConfig } from '@/types';

const { Text } = Typography;

interface FavoritesListProps {
  selectedRowKeys?: string[];
  onSelectionChange?: (selectedRowKeys: string[]) => void;
  showSelection?: boolean;
  valuationMethods: ValuationMethod[];
  valuationDate: string;
  valuationFiscalDate: string;
  combineMethod: CombineMethod;
  dcfConfig: DCFConfig;
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
  valuationMethods,
  valuationDate,
  valuationFiscalDate,
  combineMethod,
  dcfConfig,
}: FavoritesListProps) {
  const { favorites, removeFavorite, updateFavorite, loadFavorites } = useFavoriteStore();
  const [searchText, setSearchText] = useState('');
  const [pagination, setPagination] = useState({ current: 1, pageSize: 50 });
  const [priceMap, setPriceMap] = useState<Record<string, PriceData>>({});
  const [positionMap, setPositionMap] = useState<Map<string, PositionOutput>>(new Map());
  const [loadingPrices, setLoadingPrices] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editingStock, setEditingStock] = useState<FavoriteStock | null>(null);
  const [editForm] = Form.useForm();

  // 批量更新估值状态
  const [batchUpdating, setBatchUpdating] = useState(false);
  const [batchProgress, setBatchProgress] = useState({ done: 0, total: 0, success: 0, failed: 0 });

  // 过滤条件
  const [safetyFilter, setSafetyFilter] = useState<string[]>([]);
  const [fundamentalFilter, setFundamentalFilter] = useState<string[]>([]);
  const [urgencyFilter, setUrgencyFilter] = useState<number[]>([]);
  const [showOnlyHolding, setShowOnlyHolding] = useState(false);
  const [marketFilter, setMarketFilter] = useState<'all' | 'A' | 'HK'>('all');

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

  // 加载持仓数据
  const loadPositionsData = async () => {
    try {
      const response = await loadRiskPositions();
      const map = new Map(response.positions.map(p => [p.symbol, p]));
      setPositionMap(map);
    } catch (error) {
      console.error('Failed to load positions:', error);
    }
  };

  // 刷新单只股票的估值
  const refreshSingleValuation = async (stockCode: string, stockName: string) => {
    if (valuationMethods.length === 0) {
      message.warning('请先选择至少一种估值方法');
      return;
    }

    const hide = message.loading(`正在刷新 ${stockName} 的估值...`, 0);
    try {
      // 调用估值API获取该股票的估值，保持与收藏页批量刷新一致
      const response = await valuationService.getSummary(stockCode, {
        methods: valuationMethods,
        date: valuationDate || undefined,
        fiscal_date: valuationFiscalDate || undefined,
        combine_method: combineMethod,
        dcf_config: dcfConfig
      }) as any;
      
      if (response.success && response.valuation) {
        const valuation = response.valuation;
        
        // 更新收藏列表中的估值数据
        await favoriteService.update(stockCode, {
          fair_value: valuation.fair_value,
          upside_downside: valuation.upside_downside,
          valuation_date: valuation.date,
          valuation_confidence: valuation.confidence
        });
        
        // 重新加载收藏列表
        await loadFavorites();
        
        message.success(`${stockName} 估值已更新: ¥${valuation.fair_value.toFixed(2)}`);
      } else {
        message.error('获取估值失败');
      }
    } catch (error) {
      console.error('Failed to refresh valuation:', error);
      message.error(`刷新 ${stockName} 估值失败`);
    } finally {
      hide();
    }
  };


  // 批量更新选中股票的估值（逐只串行，避免超时）
  const refreshBatchValuation = async () => {
    if (valuationMethods.length === 0) {
      message.warning('请先选择至少一种估值方法');
      return;
    }

    if (!selectedRowKeys || selectedRowKeys.length === 0) return;

    const selected = favorites.filter(f => selectedRowKeys.includes(f.code));
    const total = selected.length;
    setBatchProgress({ done: 0, total, success: 0, failed: 0 });
    setBatchUpdating(true);

    let success = 0;
    let failed = 0;
    const failedNames: string[] = [];

    for (let i = 0; i < selected.length; i++) {
      const stock = selected[i];
      try {
        const response = await valuationService.getSummary(stock.code, {
          methods: valuationMethods,
          date: valuationDate || undefined,
          fiscal_date: valuationFiscalDate || undefined,
          combine_method: combineMethod,
          dcf_config: dcfConfig,
        }) as any;

        if (response.success && response.valuation) {
          const v = response.valuation;
          await favoriteService.update(stock.code, {
            fair_value: v.fair_value,
            upside_downside: v.upside_downside,
            valuation_date: v.date,
            valuation_confidence: v.confidence,
          });
          success++;
        } else {
          failed++;
          failedNames.push(stock.name || stock.code);
        }
      } catch {
        failed++;
        failedNames.push(stock.name || stock.code);
      }

      setBatchProgress({ done: i + 1, total, success, failed });
    }

    await loadFavorites();
    setBatchUpdating(false);

    if (failed === 0) {
      message.success(`已成功更新 ${success} 只股票的估值`);
    } else {
      message.warning(
        `更新完成：成功 ${success} 只，失败 ${failed} 只（${failedNames.slice(0, 3).join('、')}${failedNames.length > 3 ? '等' : ''}）`
      );
    }
  };

  // 当收藏列表变化时加载价格和持仓
  useEffect(() => {
    loadLatestPrices();
    loadPositionsData();
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

  // 判断股票市场类型
  const getStockMarket = (code: string): 'A' | 'HK' => {
    if (code.includes('.HK')) return 'HK';
    return 'A';
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

      // 市场过滤
      if (marketFilter !== 'all') {
        const market = getStockMarket(f.code);
        if (market !== marketFilter) {
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

      // 紧急程度过滤（多选）
      if (urgencyFilter.length > 0 && !urgencyFilter.includes(f.urgency || 0)) {
        return false;
      }

      // 持仓过滤
      if (showOnlyHolding && !positionMap.has(f.code)) {
        return false;
      }

      return true;
    });

    return result;
  }, [favorites, searchText, marketFilter, safetyFilter, fundamentalFilter, urgencyFilter, showOnlyHolding, positionMap]);

  // 清除所有过滤条件
  const clearFilters = () => {
    setSearchText('');
    setSafetyFilter([]);
    setFundamentalFilter([]);
    setUrgencyFilter([]);
    setShowOnlyHolding(false);
    setMarketFilter('all');
  };

  // 是否有活动的过滤条件
  const hasActiveFilters = searchText || safetyFilter.length > 0 || fundamentalFilter.length > 0 || urgencyFilter.length > 0 || showOnlyHolding || marketFilter !== 'all';

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
      notes: stock.notes || undefined,
      safety_rating: stock.safetyRating || undefined,
      fundamental_rating: stock.fundamentalRating || undefined,
      entry_price: stock.entryPrice || undefined,
      urgency: stock.urgency || undefined,
    });
    setEditModalVisible(true);
  };

  // 保存编辑
  const handleSaveEdit = async () => {
    if (!editingStock) return;

    try {
      const values = await editForm.validateFields();
      // 构建更新数据，确保数值类型正确传递
      const updateData: Record<string, any> = {
        notes: values.notes || '',
        safety_rating: values.safety_rating || '',
        fundamental_rating: values.fundamental_rating || '',
      };
      // 数值字段：如果有值则发送，否则不发送（避免 null 验证问题）
      if (values.entry_price !== undefined && values.entry_price !== null) {
        updateData.entry_price = values.entry_price;
      }
      if (values.urgency !== undefined && values.urgency !== null) {
        updateData.urgency = values.urgency;
      }

      await updateFavorite(editingStock.code, updateData);
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
      render: (code) => {
        const position = positionMap.get(code);
        return (
          <Space size={4}>
            {position && (
              <Tooltip title={`已持仓 ${position.shares} 股，成本 ¥${position.cost_price.toFixed(2)}`}>
                <WalletOutlined style={{ color: '#52c41a', fontSize: 12 }} />
              </Tooltip>
            )}
            <Text strong>{code}</Text>
          </Space>
        );
      },
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      width: 180,
      sorter: (a, b) => a.name.localeCompare(b.name),
      render: (name, record) => {
        const hasIndustry = record.swL1 || record.swL2 || record.swL3;
        if (!hasIndustry) {
          return <div>{name}</div>;
        }
        return (
          <Tooltip title={
            <div>
              {record.swL1 && <div><Tag color="blue" style={{ marginBottom: 2, fontSize: 11 }}>L1: {record.swL1}</Tag></div>}
              {record.swL2 && <div><Tag color="green" style={{ marginBottom: 2, fontSize: 11 }}>L2: {record.swL2}</Tag></div>}
              {record.swL3 && <div><Tag color="orange" style={{ marginBottom: 2, fontSize: 11 }}>L3: {record.swL3}</Tag></div>}
            </div>
          }>
            <div>{name}</div>
          </Tooltip>
        );
      },
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
      title: '紧急度',
      dataIndex: 'urgency',
      key: 'urgency',
      width: 130,
      align: 'center',
      sorter: (a, b) => (a.urgency || 0) - (b.urgency || 0),
      render: (urgency) => urgency ? (
        <Rate disabled value={urgency} count={5} style={{ fontSize: 14 }} />
      ) : '-',
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
      title: '合理价格',
      key: 'fairValue',
      width: 100,
      align: 'right',
      sorter: (a, b) => (a.fairValue || 0) - (b.fairValue || 0),
      render: (_, record) => {
        if (!record.fairValue) {
          return <Text type="secondary">-</Text>;
        }
        
        // 根据合理价格与当前价格的差距设置颜色
        const priceData = priceMap[record.code];
        if (priceData) {
          const diff = record.fairValue - priceData.close;
          // 合理价格 > 当前价格 → 红色（高估，可能下跌）
          // 合理价格 < 当前价格 → 绿色（低估，可能上涨）
          const color = diff > 0 ? '#ff4d4f' : diff < 0 ? '#52c41a' : '#666';
          return (
            <Tooltip title={record.valuationDate ? `估值日期: ${record.valuationDate}` : ''}>
              <Text strong style={{ color }}>
                ¥{record.fairValue.toFixed(2)}
              </Text>
            </Tooltip>
          );
        }
        
        return <Text strong>¥{record.fairValue.toFixed(2)}</Text>;
      },
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
      title: '备注',
      dataIndex: 'notes',
      key: 'notes',
      width: 150,
      ellipsis: true,
      render: (notes) => notes ? (
        <Tooltip title={notes}>
          <Text style={{ maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis', display: 'inline-block' }}>
            {notes}
          </Text>
        </Tooltip>
      ) : <Text type="secondary">-</Text>,
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
      width: 180,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="刷新估值">
            <Button
              size="small"
              icon={<ReloadOutlined />}
              onClick={() => refreshSingleValuation(record.code, record.name)}
            />
          </Tooltip>
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

          <Select
            mode="multiple"
            placeholder="紧急程度"
            style={{ width: 150 }}
            value={urgencyFilter}
            onChange={setUrgencyFilter}
            allowClear
            maxTagCount="responsive"
          >
            <Select.Option value={5}>5星</Select.Option>
            <Select.Option value={4}>4星</Select.Option>
            <Select.Option value={3}>3星</Select.Option>
            <Select.Option value={2}>2星</Select.Option>
            <Select.Option value={1}>1星</Select.Option>
          </Select>

          <Button
            type={showOnlyHolding ? 'primary' : 'default'}
            icon={<WalletOutlined />}
            onClick={() => setShowOnlyHolding(!showOnlyHolding)}
          >
            {showOnlyHolding ? '显示全部' : '仅持仓'}
          </Button>

          <Space.Compact>
            <Button
              type={marketFilter === 'all' ? 'primary' : 'default'}
              onClick={() => setMarketFilter('all')}
            >
              全部
            </Button>
            <Button
              type={marketFilter === 'A' ? 'primary' : 'default'}
              onClick={() => setMarketFilter('A')}
            >
              A股
            </Button>
            <Button
              type={marketFilter === 'HK' ? 'primary' : 'default'}
              onClick={() => setMarketFilter('HK')}
            >
              港股
            </Button>
          </Space.Compact>

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
          <Space style={{ marginTop: 10 }} wrap>
            <Text>已选 {selectedRowKeys.length} 只</Text>
            <Button
              icon={<SyncOutlined spin={batchUpdating} />}
              loading={batchUpdating}
              onClick={refreshBatchValuation}
              disabled={batchUpdating}
            >
              更新估值
            </Button>
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

        {/* 批量更新进度条 */}
        {batchUpdating && (
          <div style={{ marginTop: 10 }}>
            <Progress
              percent={Math.round((batchProgress.done / batchProgress.total) * 100)}
              status="active"
              size="small"
              format={() =>
                `${batchProgress.done}/${batchProgress.total}（成功 ${batchProgress.success}${batchProgress.failed > 0 ? `，失败 ${batchProgress.failed}` : ''}）`
              }
            />
          </div>
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
          pageSizeOptions: ['50', '100', '200'],
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 只`,
          hideOnSinglePage: false,
          onChange: (page, pageSize) => {
            setPagination({ current: page, pageSize });
          },
        }}
        size="small"
        scroll={{ x: 1380 }}
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
          <Form.Item name="notes" label="备注">
            <Input.TextArea
              placeholder="输入备注信息"
              rows={3}
              maxLength={500}
              showCount
            />
          </Form.Item>
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
          <Form.Item name="urgency" label="紧急程度">
            <Rate count={5} style={{ width: '100%' }} />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
