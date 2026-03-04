import { useState } from 'react';
import {
  Card,
  Tabs,
  Alert,
  Table,
  Select,
  DatePicker,
  Input,
  Button,
  Space,
  Typography,
  Statistic,
  Row,
  Col,
  message,
} from 'antd';
import {
  FundOutlined,
  RiseOutlined,
  FallOutlined,
  SearchOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import { moneyflowService } from '@/services';
import type { StockMoneyflow, IndustryMoneyflow, IndustryLevel } from '@/types/moneyflow.types';
import dayjs from 'dayjs';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

type TabKey = 'industry' | 'stock';

// 表格列定义
const industryColumns = [
  {
    title: '排名',
    key: 'rank',
    render: (_: unknown, __: unknown, index: number) => index + 1,
    width: 60,
  },
  {
    title: '行业',
    dataIndex: 'sw_l1',
    key: 'sw_l1',
    render: (name: string, record: IndustryMoneyflow) => {
      // 根据级别显示不同的行业名称
      if (record.level === 'L1') return name;
      if (record.level === 'L2') return record.sw_l2 || name;
      if (record.level === 'L3') return record.sw_l3 || record.sw_l2 || name;
      return name;
    },
  },
  {
    title: '成分股数',
    dataIndex: 'stock_count',
    key: 'stock_count',
    width: 80,
    align: 'right' as const,
  },
  {
    title: '净流入(万元)',
    dataIndex: 'net_mf_amount',
    key: 'net_mf_amount',
    render: (val: number) => (
      <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
        {val ? val.toFixed(2) : '-'}
      </span>
    ),
    width: 120,
    align: 'right' as const,
    sorter: (a: IndustryMoneyflow, b: IndustryMoneyflow) => (a.net_mf_amount || 0) - (b.net_mf_amount || 0),
  },
  {
    title: '特大单净流入(万元)',
    dataIndex: 'net_elg_amount',
    key: 'net_elg_amount',
    render: (val: number) => (
      <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
        {val ? val.toFixed(2) : '-'}
      </span>
    ),
    width: 140,
    align: 'right' as const,
    sorter: (a: IndustryMoneyflow, b: IndustryMoneyflow) => (a.net_elg_amount || 0) - (b.net_elg_amount || 0),
  },
  {
    title: '大单净流入(万元)',
    dataIndex: 'net_lg_amount',
    key: 'net_lg_amount',
    render: (val: number) => (
      <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
        {val ? val.toFixed(2) : '-'}
      </span>
    ),
    width: 120,
    align: 'right' as const,
    sorter: (a: IndustryMoneyflow, b: IndustryMoneyflow) => (a.net_lg_amount || 0) - (b.net_lg_amount || 0),
  },
  {
    title: '日期',
    dataIndex: 'trade_date',
    key: 'trade_date',
    width: 100,
  },
];

const stockColumns = [
  {
    title: '股票代码',
    dataIndex: 'ts_code',
    key: 'ts_code',
    width: 100,
  },
  {
    title: '交易日期',
    dataIndex: 'trade_date',
    key: 'trade_date',
    width: 100,
    sorter: (a: StockMoneyflow, b: StockMoneyflow) => a.trade_date.localeCompare(b.trade_date),
    defaultSortOrder: 'descend' as const,
  },
  {
    title: '净流入(万元)',
    dataIndex: 'net_mf_amount',
    key: 'net_mf_amount',
    render: (val: number) => (
      <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
        {val ? val.toFixed(2) : '-'}
      </span>
    ),
    width: 120,
    align: 'right' as const,
    sorter: (a: StockMoneyflow, b: StockMoneyflow) => (a.net_mf_amount || 0) - (b.net_mf_amount || 0),
  },
  {
    title: '特大单净流入(万元)',
    dataIndex: 'net_elg_amount',
    key: 'net_elg_amount',
    render: (val: number) => (
      <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
        {val ? val.toFixed(2) : '-'}
      </span>
    ),
    width: 140,
    align: 'right' as const,
  },
  {
    title: '大单净流入(万元)',
    dataIndex: 'net_lg_amount',
    key: 'net_lg_amount',
    render: (val: number) => (
      <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
        {val ? val.toFixed(2) : '-'}
      </span>
    ),
    width: 120,
    align: 'right' as const,
  },
  {
    title: '特大单买入(万元)',
    dataIndex: 'buy_elg_amount',
    key: 'buy_elg_amount',
    width: 120,
    align: 'right' as const,
    render: (val: number) => val ? val.toFixed(2) : '-',
  },
  {
    title: '特大单卖出(万元)',
    dataIndex: 'sell_elg_amount',
    key: 'sell_elg_amount',
    width: 120,
    align: 'right' as const,
    render: (val: number) => val ? val.toFixed(2) : '-',
  },
];

interface ExpandedRow {
  key: string;
  stocks: StockMoneyflow[];
  loading: boolean;
  industryNetMfAmount?: number;
  accumulateDays?: number;
}

function MoneyFlowPage() {
  const [activeTab, setActiveTab] = useState<TabKey>('industry');
  const [loading, setLoading] = useState(false);
  const [stockData, setStockData] = useState<StockMoneyflow[]>([]);
  const [industryData, setIndustryData] = useState<IndustryMoneyflow[]>([]);
  const [error, setError] = useState('');

  // 查询参数
  const [stockCode, setStockCode] = useState('');
  const [stockLimit, setStockLimit] = useState(50);
  const [level, setLevel] = useState<IndustryLevel>('L1');
  const [tradeDate, setTradeDate] = useState<string | undefined>(undefined);
  const [accumulateDays, setAccumulateDays] = useState(1);

  // 展开行数据
  const [expandedRows, setExpandedRows] = useState<Record<string, ExpandedRow>>({});

  // 分页状态
  const [stockPagination, setStockPagination] = useState({ current: 1, pageSize: 20 });

  // 查询个股资金流向
  const handleQueryStock = async () => {
    if (!stockCode.trim()) {
      message.warning('请输入股票代码');
      return;
    }

    setLoading(true);
    setError('');
    try {
      const response = await moneyflowService.getStockMoneyflow(stockCode.trim(), undefined, undefined, stockLimit);
      if (response.success) {
        setStockData(response.data);
        message.success(`获取到 ${response.count} 条记录`);
      } else {
        setError('查询失败');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '查询失败');
    } finally {
      setLoading(false);
    }
  };

  // 查询行业排名
  const handleQueryTopIndustries = async () => {
    setLoading(true);
    setError('');
    try {
      const response = await moneyflowService.getTopIndustries(level, tradeDate, undefined, accumulateDays);
      if (response.success) {
        setIndustryData(response.data);
        setTradeDate(response.trade_date);
        const daysText = accumulateDays > 1 ? `（最近${accumulateDays}个交易日累计）` : '';
        message.success(`获取到 ${response.count} 条记录${daysText}`);
      } else {
        setError('查询失败');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : '查询失败');
    } finally {
      setLoading(false);
    }
  };

  // 刷新数据
  const handleRefresh = () => {
    if (activeTab === 'industry') {
      handleQueryTopIndustries();
    } else {
      handleQueryStock();
    }
  };

  // 获取行业内股票列表
  const handleExpand = async (record: IndustryMoneyflow) => {
    // 生成唯一的 rowKey，处理 null 值
    const rowKey = `${record.trade_date}-${record.sw_l1 || ''}-${record.sw_l2 || ''}-${record.sw_l3 || ''}`;

    // 如果已经加载过，直接返回
    if (expandedRows[rowKey]?.stocks) {
      return;
    }

    // 标记为加载中，保存当前查询的累计天数
    setExpandedRows(prev => ({
      ...prev,
      [rowKey]: {
        key: rowKey,
        stocks: [],
        loading: true,
        industryNetMfAmount: record.net_mf_amount || 0,
        accumulateDays: accumulateDays  // 保存累计天数
      }
    }));

    try {
      // 根据级别确定行业名称
      const industryName = record.level === 'L1' ? record.sw_l1 :
                          record.level === 'L2' ? record.sw_l2 : record.sw_l3;

      const response = await moneyflowService.getIndustryStocksMoneyflow(
        industryName!,
        record.level,
        tradeDate,
        accumulateDays
      );

      if (response.success) {
        setExpandedRows(prev => ({
          ...prev,
          [rowKey]: {
            key: rowKey,
            stocks: response.data,
            loading: false,
            industryNetMfAmount: record.net_mf_amount || 0,
            accumulateDays: accumulateDays  // 保存累计天数
          }
        }));
      } else {
        message.error('获取行业股票列表失败');
        setExpandedRows(prev => ({
          ...prev,
          [rowKey]: { key: rowKey, stocks: [], loading: false, industryNetMfAmount: 0, accumulateDays }
        }));
      }
    } catch (err) {
      message.error('获取行业股票列表失败');
      setExpandedRows(prev => ({
        ...prev,
        [rowKey]: { key: rowKey, stocks: [], loading: false, industryNetMfAmount: 0, accumulateDays }
      }));
    }
  };

  // 计算统计信息
  const industryStats = {
    total: industryData.length,
    netInflow: industryData.reduce((sum, item) => sum + (item.net_mf_amount || 0), 0),
    upCount: industryData.filter(item => (item.net_mf_amount || 0) > 0).length,
    downCount: industryData.filter(item => (item.net_mf_amount || 0) < 0).length,
  };

  const stockStats = {
    total: stockData.length,
    netInflow: stockData.reduce((sum, item) => sum + (item.net_mf_amount || 0), 0),
    upCount: stockData.filter(item => (item.net_mf_amount || 0) > 0).length,
    downCount: stockData.filter(item => (item.net_mf_amount || 0) < 0).length,
  };

  return (
    <div>
      <Title level={2}>
        <FundOutlined style={{ marginRight: 8 }} />
        资金流向
      </Title>
      <Text type="secondary">查看股票和行业资金流向数据</Text>

      <Card style={{ marginTop: 16 }}>
        <Tabs activeKey={activeTab} onChange={(key) => setActiveTab(key as TabKey)}>
          <Tabs.TabPane tab="行业汇总" key="industry">
            <Space direction="vertical" style={{ width: '100%' }} size="large">
              {/* 查询条件 */}
              <Space wrap>
                <Select<IndustryLevel>
                  value={level}
                  onChange={setLevel}
                  style={{ width: 120 }}
                >
                  <Option value="L1">一级行业</Option>
                  <Option value="L2">二级行业</Option>
                  <Option value="L3">三级行业</Option>
                </Select>

                <DatePicker
                  value={tradeDate ? dayjs(tradeDate) : undefined}
                  onChange={(date) => setTradeDate(date?.format('YYYY-MM-DD'))}
                  placeholder="选择日期（默认最新）"
                  style={{ width: 150 }}
                />

                <Select
                  value={accumulateDays}
                  onChange={setAccumulateDays}
                  style={{ width: 130 }}
                >
                  <Option value={1}>单日</Option>
                  <Option value={3}>3日累计</Option>
                  <Option value={5}>5日累计</Option>
                  <Option value={10}>10日累计</Option>
                  <Option value={20}>20日累计</Option>
                </Select>

                <Button
                  type="primary"
                  icon={<SearchOutlined aria-hidden="true" />}
                  onClick={handleQueryTopIndustries}
                  loading={loading}
                >
                  查询
                </Button>

                <Button
                  icon={<ReloadOutlined aria-hidden="true" />}
                  onClick={handleRefresh}
                  loading={loading}
                >
                  刷新
                </Button>
              </Space>

              {/* 统计信息 */}
              {industryData.length > 0 && (
                <Row gutter={16}>
                  <Col span={6}>
                    <Statistic
                      title="统计数量"
                      value={industryStats.total}
                      suffix="个"
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="净流入"
                      value={industryStats.netInflow}
                      precision={2}
                      suffix="万元"
                      valueStyle={{ color: industryStats.netInflow > 0 ? '#f5222d' : '#52c41a' }}
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="净流入"
                      value={industryStats.upCount}
                      suffix={`/ ${industryStats.total} 个`}
                      prefix={<RiseOutlined aria-hidden="true" />}
                      valueStyle={{ color: '#f5222d' }}
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="净流出"
                      value={industryStats.downCount}
                      suffix={`/ ${industryStats.total} 个`}
                      prefix={<FallOutlined aria-hidden="true" />}
                      valueStyle={{ color: '#52c41a' }}
                    />
                  </Col>
                </Row>
              )}

              {/* 数据表格 */}
              <Table
                columns={industryColumns}
                dataSource={industryData}
                rowKey={(record) => `${record.trade_date}-${record.sw_l1}-${record.sw_l2}-${record.sw_l3}`}
                loading={loading}
                pagination={false}
                size="small"
                expandable={{
                  expandedRowRender: (record) => {
                    // 生成唯一的 rowKey，处理 null 值
                    const rowKey = `${record.trade_date}-${record.sw_l1 || ''}-${record.sw_l2 || ''}-${record.sw_l3 || ''}`;
                    const expandedRow = expandedRows[rowKey];

                    if (!expandedRow || expandedRow.loading) {
                      return <div style={{ padding: '16px', textAlign: 'center' }}>加载中...</div>;
                    }

                    if (expandedRow.stocks.length === 0) {
                      return <div style={{ padding: '16px', textAlign: 'center' }}>暂无股票数据</div>;
                    }

                    return (
                      <Table
                        columns={[
                          {
                            title: '股票代码',
                            dataIndex: 'ts_code',
                            key: 'ts_code',
                            width: 100,
                          },
                          {
                            title: '股票名称',
                            dataIndex: 'stock_name',
                            key: 'stock_name',
                            width: 100,
                            render: (name: string) => name || '-',
                          },
                          {
                            title: '净流入(万元)',
                            dataIndex: 'net_mf_amount',
                            key: 'net_mf_amount',
                            render: (val: number, record: StockMoneyflow) => {
                              const ratio = expandedRow.industryNetMfAmount && expandedRow.industryNetMfAmount !== 0
                                ? (val / expandedRow.industryNetMfAmount * 100)
                                : 0;
                              return (
                                <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
                                  {val ? val.toFixed(2) : '-'}
                                  {ratio !== 0 && (
                                    <span style={{ fontSize: '12px', marginLeft: '8px', opacity: 0.7 }}>
                                      ({ratio > 0 ? '+' : ''}{ratio.toFixed(2)}%)
                                    </span>
                                  )}
                                </span>
                              );
                            },
                            width: 180,
                            align: 'right' as const,
                            sorter: (a: StockMoneyflow, b: StockMoneyflow) => (a.net_mf_amount || 0) - (b.net_mf_amount || 0),
                            defaultSortOrder: 'descend' as const,
                          },
                          {
                            title: '特大单净流入(万元)',
                            dataIndex: 'net_elg_amount',
                            key: 'net_elg_amount',
                            render: (val: number) => (
                              <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
                                {val ? val.toFixed(2) : '-'}
                              </span>
                            ),
                            width: 140,
                            align: 'right' as const,
                          },
                          {
                            title: '大单净流入(万元)',
                            dataIndex: 'net_lg_amount',
                            key: 'net_lg_amount',
                            render: (val: number) => (
                              <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
                                {val ? val.toFixed(2) : '-'}
                              </span>
                            ),
                            width: 120,
                            align: 'right' as const,
                          },
                        ]}
                        dataSource={expandedRow.stocks}
                        rowKey={(stock) => stock.ts_code}
                        pagination={false}
                        size="small"
                        summary={() => {
                          const stocksTotal = expandedRow.stocks.reduce((sum, s) => sum + (s.net_mf_amount || 0), 0);
                          const diff = (expandedRow.industryNetMfAmount || 0) - stocksTotal;
                          return (
                            <Table.Summary fixed>
                              <Table.Summary.Row>
                                <Table.Summary.Cell index={0} colSpan={2} align="right">
                                  <strong>汇总（共 {expandedRow.stocks.length} 只）</strong>
                                </Table.Summary.Cell>
                                <Table.Summary.Cell index={1} align="right">
                                  <strong style={{
                                    color: stocksTotal > 0 ? '#f5222d' : stocksTotal < 0 ? '#52c41a' : undefined
                                  }}>
                                    {stocksTotal.toFixed(2)}
                                  </strong>
                                </Table.Summary.Cell>
                                <Table.Summary.Cell index={2} colSpan={2}>
                                  <span style={{ fontSize: '12px', color: Math.abs(diff) < 0.01 ? '#52c41a' : '#faad14' }}>
                                    {Math.abs(diff) < 0.01 ? '✓ 与行业汇总一致' : `⚠️ 差异: ${diff.toFixed(2)} 万元`}
                                  </span>
                                </Table.Summary.Cell>
                              </Table.Summary.Row>
                            </Table.Summary>
                          );
                        }}
                        style={{ margin: '-16px -24px -16px -24px' }}
                      />
                    );
                  },
                  onExpand: (expanded, record) => {
                    if (expanded) {
                      handleExpand(record);
                    }
                  },
                }}
              />
            </Space>
          </Tabs.TabPane>

          <Tabs.TabPane tab="个股查询" key="stock">
            <Space direction="vertical" style={{ width: '100%' }} size="large">
              {/* 查询条件 */}
              <Space wrap>
                <Input
                  placeholder="股票代码 (如: 600382)"
                  value={stockCode}
                  onChange={(e) => setStockCode(e.target.value)}
                  onPressEnter={handleQueryStock}
                  style={{ width: 150 }}
                />

                <Select
                  value={stockLimit}
                  onChange={setStockLimit}
                  style={{ width: 120 }}
                >
                  <Option value={20}>最近20天</Option>
                  <Option value={50}>最近50天</Option>
                  <Option value={100}>最近100天</Option>
                  <Option value={200}>最近200天</Option>
                </Select>

                <Button
                  type="primary"
                  icon={<SearchOutlined aria-hidden="true" />}
                  onClick={handleQueryStock}
                  loading={loading}
                >
                  查询
                </Button>

                <Button
                  icon={<ReloadOutlined aria-hidden="true" />}
                  onClick={handleRefresh}
                  loading={loading}
                >
                  刷新
                </Button>
              </Space>

              {/* 统计信息 */}
              {stockData.length > 0 && (
                <Row gutter={16}>
                  <Col span={6}>
                    <Statistic
                      title="记录数"
                      value={stockStats.total}
                      suffix="条"
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="净流入"
                      value={stockStats.netInflow}
                      precision={2}
                      suffix="万元"
                      valueStyle={{ color: stockStats.netInflow > 0 ? '#f5222d' : '#52c41a' }}
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="净流入天数"
                      value={stockStats.upCount}
                      suffix={`/ ${stockStats.total} 天`}
                      prefix={<RiseOutlined aria-hidden="true" />}
                      valueStyle={{ color: '#f5222d' }}
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="净流出天数"
                      value={stockStats.downCount}
                      suffix={`/ ${stockStats.total} 天`}
                      prefix={<FallOutlined aria-hidden="true" />}
                      valueStyle={{ color: '#52c41a' }}
                    />
                  </Col>
                </Row>
              )}

              {/* 数据表格 */}
              <Table
                columns={stockColumns}
                dataSource={stockData}
                rowKey={(record) => `${record.ts_code}-${record.trade_date}`}
                loading={loading}
                pagination={{
                  current: stockPagination.current,
                  pageSize: stockPagination.pageSize,
                  pageSizeOptions: ['10', '20', '50', '100'],
                  showSizeChanger: true,
                  showTotal: (total) => `共 ${total} 条记录`,
                  hideOnSinglePage: false,
                  onChange: (page, pageSize) => {
                    setStockPagination({ current: page, pageSize });
                  },
                }}
                size="small"
              />
            </Space>
          </Tabs.TabPane>
        </Tabs>

        {error && <Alert message={error} type="error" showIcon style={{ marginTop: 16 }} />}
      </Card>
    </div>
  );
}

export default MoneyFlowPage;
