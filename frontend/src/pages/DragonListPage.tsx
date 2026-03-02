import { useState, useEffect } from 'react';
import {
  Card,
  Tabs,
  Table,
  DatePicker,
  Button,
  Space,
  Typography,
  Statistic,
  Row,
  Col,
  Tag,
  message,
  Select
} from 'antd';
import {
  TrophyOutlined,
  RiseOutlined,
  FallOutlined,
  ReloadOutlined
} from '@ant-design/icons';
import { dragonlistService } from '@/services';
import type { DragonList, DragonListStats, DragonListSortBy } from '@/types/dragonlist.types';
import dayjs from 'dayjs';

const { Title } = Typography;
const { RangePicker } = DatePicker;

function DragonListPage() {
  const [activeTab, setActiveTab] = useState('daily');
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<DragonList[]>([]);
  const [stats, setStats] = useState<DragonListStats | null>(null);
  const [selectedDate, setSelectedDate] = useState<string | undefined>(undefined);
  const [sortBy, setSortBy] = useState<DragonListSortBy>('net_amount');
  const [topN, setTopN] = useState(10);

  // 获取每日龙虎榜数据
  const fetchDailyData = async (date?: string) => {
    setLoading(true);
    try {
      const response = await dragonlistService.query({
        tradeDate: date,
        limit: 200
      });

      if (response.success) {
        setData(response.data);
      }
    } catch (error) {
      message.error('获取数据失败');
    } finally {
      setLoading(false);
    }
  };

  // 获取龙虎榜排名
  const fetchTopData = async () => {
    setLoading(true);
    try {
      const response = await dragonlistService.getTop(undefined, topN, sortBy);

      if (response.success) {
        setData(response.data);
        setSelectedDate(response.trade_date);
      }
    } catch (error) {
      message.error('获取排名失败');
    } finally {
      setLoading(false);
    }
  };

  // 获取统计数据
  const fetchStats = async () => {
    try {
      const response = await dragonlistService.getStats(selectedDate);

      if (response.success && response.data) {
        setStats(response.data);
      }
    } catch (error) {
      console.error('获取统计失败', error);
    }
  };

  // 刷新数据
  const handleRefresh = () => {
    if (activeTab === 'daily') {
      fetchDailyData(selectedDate);
    } else if (activeTab === 'top') {
      fetchTopData();
    }
  };

  // 表格列定义
  const columns = [
    {
      title: '股票代码',
      dataIndex: 'ts_code',
      key: 'ts_code',
      width: 120,
      fixed: 'left' as const
    },
    {
      title: '股票名称',
      dataIndex: 'name',
      key: 'name',
      width: 120
    },
    {
      title: '收盘价',
      dataIndex: 'close',
      key: 'close',
      render: (val: number) => val?.toFixed(2) || '-',
      width: 100
    },
    {
      title: '涨跌幅(%)',
      dataIndex: 'pct_change',
      key: 'pct_change',
      render: (val: number) => (
        <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
          {val?.toFixed(2)}%
        </span>
      ),
      width: 100
    },
    {
      title: '龙虎榜净买入(万元)',
      dataIndex: 'net_amount',
      key: 'net_amount',
      render: (val: number) => (
        <span style={{ color: val > 0 ? '#f5222d' : val < 0 ? '#52c41a' : undefined }}>
          {val?.toFixed(2) || '-'}
        </span>
      ),
      sorter: (a: DragonList, b: DragonList) => (a.net_amount || 0) - (b.net_amount || 0),
      width: 140
    },
    {
      title: '龙虎榜成交额(万元)',
      dataIndex: 'l_amount',
      key: 'l_amount',
      render: (val: number) => val?.toFixed(2) || '-',
      width: 140
    },
    {
      title: '上榜理由',
      dataIndex: 'reason',
      key: 'reason',
      ellipsis: true,
      width: 250
    }
  ];

  useEffect(() => {
    if (activeTab === 'daily') {
      fetchDailyData(selectedDate);
    } else if (activeTab === 'top') {
      fetchTopData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, sortBy, topN, selectedDate]);

  useEffect(() => {
    if (selectedDate && activeTab === 'daily') {
      fetchStats();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedDate, activeTab]);

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>
        <TrophyOutlined /> 龙虎榜数据
      </Title>

      <Space direction="vertical" style={{ width: '100%' }} size="large">
        {/* 操作栏 */}
        <Space wrap>
          <DatePicker
            value={selectedDate ? dayjs(selectedDate) : undefined}
            onChange={(date) => {
              const dateStr = date?.format('YYYY-MM-DD');
              setSelectedDate(dateStr);
              if (activeTab === 'daily') {
                fetchDailyData(dateStr);
              }
            }}
            placeholder="选择日期"
            allowClear
          />
          <Button type="primary" icon={<ReloadOutlined />} onClick={handleRefresh}>
            刷新
          </Button>
        </Space>

        {/* 排名Tab的额外选项 */}
        {activeTab === 'top' && (
          <Space>
            <Select
              value={sortBy}
              onChange={setSortBy}
              style={{ width: 150 }}
              options={[
                { label: '净买入额', value: 'net_amount' },
                { label: '龙虎榜成交额', value: 'l_amount' },
                { label: '总成交额', value: 'amount' },
                { label: '净买额占比', value: 'net_rate' }
              ]}
            />
            <Select
              value={topN}
              onChange={setTopN}
              style={{ width: 100 }}
              options={[
                { label: '前10名', value: 10 },
                { label: '前20名', value: 20 },
                { label: '前30名', value: 30 },
                { label: '前50名', value: 50 }
              ]}
            />
          </Space>
        )}

        {/* 统计卡片 */}
        {stats && (
          <Row gutter={16}>
            <Col span={4}>
              <Statistic title="上榜总数" value={stats.data?.summary?.total_count || 0} />
            </Col>
            <Col span={4}>
              <Statistic
                title="净买入家数"
                value={stats.data?.summary?.net_buy_count || 0}
                valueStyle={{ color: '#cf1322' }}
              />
            </Col>
            <Col span={4}>
              <Statistic
                title="净卖出家数"
                value={stats.data?.summary?.net_sell_count || 0}
                valueStyle={{ color: '#3f8600' }}
              />
            </Col>
            <Col span={4}>
              <Statistic
                title="净买入总额(万元)"
                value={stats.data?.summary?.total_net_amount || 0}
                precision={2}
              />
            </Col>
            <Col span={4}>
              <Statistic
                title="龙虎榜成交额(万元)"
                value={stats.data?.summary?.total_l_amount || 0}
                precision={2}
              />
            </Col>
            <Col span={4}>
              <Statistic
                title="平均净买额占比(%)"
                value={stats.data?.summary?.avg_net_rate || 0}
                precision={2}
              />
            </Col>
          </Row>
        )}

        {/* 数据表格 */}
        <Card>
          <Table
            columns={columns}
            dataSource={data}
            rowKey={(record) => `${record.ts_code}_${record.trade_date}`}
            loading={loading}
            pagination={{ pageSize: 20 }}
            scroll={{ x: 1200 }}
          />
        </Card>
      </Space>
    </div>
  );
}

export default DragonListPage;
