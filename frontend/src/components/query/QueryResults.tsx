import { useMemo, useState, useEffect } from 'react';
import { Table, Card, Tabs, Empty, Spin, Tag, Radio } from 'antd';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ComposedChart, Area, Bar, ReferenceLine } from 'recharts';
import type { ColumnsType } from 'antd/es/table';
import type { StockData } from '@/types';
import { formatNumber, formatPercent, formatVolume, formatDate } from '@/utils';
import api from '@/services/api';

interface QueryResultsProps {
  results: Record<string, StockData[]>;
  loading?: boolean;
}

interface RegimeData {
  date: string;
  close: number;
  regime: 'bull' | 'bear' | 'neutral';
  total_score: number;
  trend_score: number;
  momentum_score: number;
  position_score: number;
  volume_score: number;
  volatility_score: number;
}

export function QueryResults({ results, loading }: QueryResultsProps) {
  const stockCodes = Object.keys(results);
  const [pagination, setPagination] = useState({ current: 1, pageSize: 50 });
  const [regimeData, setRegimeData] = useState<Record<string, RegimeData[]>>({});
  const [regimeLoading, setRegimeLoading] = useState(false);
  const [selectedCycle, setSelectedCycle] = useState<'short' | 'medium' | 'long'>('medium');

  // 将多只股票的数据合并为表格数据
  const tableData = useMemo(() => {
    if (stockCodes.length === 0) return [];

    // 获取所有日期
    const allDates = new Set<string>();
    stockCodes.forEach((code) => {
      results[code].forEach((item) => {
        // 后端返回 datetime 字段，前端期望 date 字段
        allDates.add(item.date || item.datetime);
      });
    });

    // 按日期降序排列
    const sortedDates = Array.from(allDates).sort((a, b) => b.localeCompare(a));

    // 构建表格数据
    return sortedDates.map((date) => {
      const row: any = {
        key: date,
        date,
      };

      stockCodes.forEach((code) => {
        const data = results[code].find((d) => d.date === date);
        if (data) {
          row[`${code}_open`] = data.open;
          row[`${code}_high`] = data.high;
          row[`${code}_low`] = data.low;
          row[`${code}_close`] = data.close;
          // 后端返回 pct_chg 字段，转换为 changePercent
          row[`${code}_change`] = data.changePercent || data.pct_chg || 0;
          row[`${code}_volume`] = data.volume;
          row[`${code}_amount`] = data.amount || data.turnover;
          // 换手率：优先使用 turnover_rate_f（基于自由流通股的换手率），这是最常用的换手率指标
          // 注意：turnover 字段在数据库中存储的是成交额，不是换手率
          row[`${code}_turnoverRate`] = data.turnover_rate_f ?? data.turnoverRate;
        }
      });

      return row;
    });
  }, [results, stockCodes]);

  // 生成表格列
  const columns = useMemo(() => {
    const cols: ColumnsType<any> = [
      {
        title: '日期',
        dataIndex: 'date',
        key: 'date',
        fixed: 'left',
        width: 120,
        sorter: (a, b) => a.date.localeCompare(b.date),
      },
    ];

    stockCodes.forEach((code) => {
      cols.push(
        {
          title: `${code} 开盘价`,
          dataIndex: `${code}_open`,
          key: `${code}_open`,
          width: 100,
          render: (value: number) => (value != null ? formatNumber(value, 2) : '-'),
          sorter: (a, b) => (a[`${code}_open`] || 0) - (b[`${code}_open`] || 0),
        },
        {
          title: `${code} 最高价`,
          dataIndex: `${code}_high`,
          key: `${code}_high`,
          width: 100,
          render: (value: number) => (value != null ? formatNumber(value, 2) : '-'),
          sorter: (a, b) => (a[`${code}_high`] || 0) - (b[`${code}_high`] || 0),
        },
        {
          title: `${code} 最低价`,
          dataIndex: `${code}_low`,
          key: `${code}_low`,
          width: 100,
          render: (value: number) => (value != null ? formatNumber(value, 2) : '-'),
          sorter: (a, b) => (a[`${code}_low`] || 0) - (b[`${code}_low`] || 0),
        },
        {
          title: `${code} 收盘价`,
          dataIndex: `${code}_close`,
          key: `${code}_close`,
          width: 100,
          render: (value: number) => (value != null ? formatNumber(value, 2) : '-'),
          sorter: (a, b) => (a[`${code}_close`] || 0) - (b[`${code}_close`] || 0),
        },
        {
          title: `${code} 涨跌幅`,
          dataIndex: `${code}_change`,
          key: `${code}_change`,
          width: 100,
          render: (value: number) => {
            if (value == null) return '-';
            const color = value >= 0 ? '#f5222d' : '#52c41a';
            // 后端返回的 pct_chg 已经是百分比值（如 -0.7483 表示 -0.7483%）
            // 直接添加 % 符号，不需要再转换
            return <span style={{ color }}>{`${value.toFixed(2)}%`}</span>;
          },
          sorter: (a, b) => (a[`${code}_change`] || 0) - (b[`${code}_change`] || 0),
        },
        {
          title: `${code} 成交量`,
          dataIndex: `${code}_volume`,
          key: `${code}_volume`,
          width: 120,
          render: (value: number) => (value != null ? formatVolume(value) : '-'),
          sorter: (a, b) => (a[`${code}_volume`] || 0) - (b[`${code}_volume`] || 0),
        },
        {
          title: `${code} 成交额`,
          dataIndex: `${code}_amount`,
          key: `${code}_amount`,
          width: 120,
          render: (value: number) => (value != null ? formatVolume(value) : '-'),
          sorter: (a, b) => (a[`${code}_amount`] || 0) - (b[`${code}_amount`] || 0),
        },
        {
          title: `${code} 换手率`,
          dataIndex: `${code}_turnoverRate`,
          key: `${code}_turnoverRate`,
          width: 90,
          render: (value: number) => (value != null ? `${value.toFixed(2)}%` : '-'),
          sorter: (a, b) => (a[`${code}_turnoverRate`] || 0) - (b[`${code}_turnoverRate`] || 0),
        }
      );
    });

    return cols;
  }, [stockCodes]);

  // 准备图表数据（按日期升序）
  const chartData = useMemo(() => {
    if (stockCodes.length === 0) return [];

    // 获取所有日期并升序排列
    const allDates = new Set<string>();
    stockCodes.forEach((code) => {
      results[code].forEach((item) => {
        // 后端返回 datetime 字段，前端期望 date 字段
        allDates.add(item.date || item.datetime);
      });
    });

    const sortedDates = Array.from(allDates).sort((a, b) => a.localeCompare(b));

    return sortedDates.map((date) => {
      const point: any = { date };
      stockCodes.forEach((code) => {
        // 后端返回 datetime 字段，前端期望 date 字段
        const data = results[code].find((d) => (d.date || d.datetime) === date);
        if (data) {
          point[code] = data.close;
        }
      });
      return point;
    });
  }, [results, stockCodes]);

  // 计算日期范围 key，用于防止重复请求（包含周期）
  const dateRangeKey = useMemo(() => {
    if (stockCodes.length === 0) return '';
    const parts: string[] = [];
    stockCodes.forEach((code) => {
      const data = results[code];
      if (data && data.length > 0) {
        const dates = data.map(d => d.date || d.datetime).sort();
        parts.push(`${code}:${dates[0]}-${dates[dates.length - 1]}`);
      }
    });
    const key = `${selectedCycle}|${parts.join('|')}`;
    console.log('[牛熊市] dateRangeKey:', key);
    return key;
  }, [results, stockCodes, selectedCycle]);

  // 获取牛熊市数据
  useEffect(() => {
    console.log('[牛熊市] useEffect 触发, dateRangeKey:', dateRangeKey, 'stockCodes:', stockCodes);
    if (stockCodes.length === 0 || !dateRangeKey) return;

    const fetchRegimeData = async () => {
      setRegimeLoading(true);
      setRegimeData({}); // 清空旧数据
      const newRegimeData: Record<string, RegimeData[]> = {};

      for (const code of stockCodes) {
        try {
          // 获取数据的日期范围
          const data = results[code];
          if (!data || data.length === 0) continue;

          const dates = data.map(d => d.date || d.datetime).sort();
          const startDate = dates[0];
          const endDate = dates[dates.length - 1];

          console.log(`[牛熊市] 请求 ${code}: ${startDate} ~ ${endDate}`);

          const response = await api.post('/stock/regime', {
            symbol: code,
            start_date: startDate,
            end_date: endDate,
            cycle: selectedCycle,
          });

          console.log(`[牛熊市] ${code} 响应:`, response);

          // axios 拦截器已经解包，response 就是后端响应体 { symbol: ..., data: [...] }
          const regimeResponseData = response.data;
          if (regimeResponseData && Array.isArray(regimeResponseData) && regimeResponseData.length > 0) {
            newRegimeData[code] = regimeResponseData;
            console.log(`[牛熊市] ${code} 成功: ${regimeResponseData.length} 条`);
          }
        } catch (error) {
          console.error(`获取 ${code} 牛熊市数据失败:`, error);
        }
      }

      console.log('[牛熊市] 最终数据:', newRegimeData);
      setRegimeData(newRegimeData);
      setRegimeLoading(false);
    };

    fetchRegimeData();
  }, [dateRangeKey]);

  // 准备牛熊走势图数据
  const regimeChartData = useMemo(() => {
    if (stockCodes.length === 0 || Object.keys(regimeData).length === 0) return {};

    const chartDataMap: Record<string, any[]> = {};

    stockCodes.forEach((code) => {
      const data = regimeData[code];
      if (!data || data.length === 0) return;  // 确保数据不为空

      chartDataMap[code] = data.map((item) => ({
        date: item.date,
        close: item.close,
        total_score: item.total_score,
        regime: item.regime,
        // 背景色区域数据
        bull_area: item.regime === 'bull' ? 100 : null,
        bear_area: item.regime === 'bear' ? 100 : null,
        neutral_area: item.regime === 'neutral' ? 100 : null,
      }));
    });

    return chartDataMap;
  }, [regimeData, stockCodes]);

  // 图表颜色
  const colors = ['#1890ff', '#52c41a', '#faad14', '#f5222d', '#722ed1', '#eb2f96', '#13c2c2', '#fa8c16'];

  if (stockCodes.length === 0) {
    return (
      <Card>
        <Empty description="请先选择股票并点击查询" />
      </Card>
    );
  }

  return (
    <Card>
      <Tabs
        defaultActiveKey="table"
        destroyInactiveTabPane
        items={[
          {
            key: 'table',
            label: '数据表格',
            children: (
              <Table
                columns={columns}
                dataSource={tableData}
                loading={loading}
                scroll={{ x: 'max-content' }}
                pagination={{
                  current: pagination.current,
                  pageSize: pagination.pageSize,
                  pageSizeOptions: ['10', '20', '50', '100', '200'],
                  showSizeChanger: true,
                  showTotal: (total) => `共 ${total} 条`,
                  hideOnSinglePage: false,
                  onChange: (page, pageSize) => {
                    setPagination({ current: page, pageSize });
                  },
                }}
              />
            ),
          },
          {
            key: 'chart',
            label: '价格走势图',
            children: (
              <div style={{ width: '100%', height: 400 }}>
                {loading ? (
                  <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100%' }}>
                    <Spin size="large" />
                  </div>
                ) : (
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={chartData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="date" />
                      <YAxis />
                      <Tooltip />
                      <Legend />
                      {stockCodes.map((code, index) => (
                        <Line
                          key={code}
                          type="monotone"
                          dataKey={code}
                          stroke={colors[index % colors.length]}
                          name={`${code} 收盘价`}
                          dot={false}
                        />
                      ))}
                    </LineChart>
                  </ResponsiveContainer>
                )}
              </div>
            ),
          },
          {
            key: 'regime',
            label: '牛熊走势图',
            children: regimeLoading ? (
              <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 400 }}>
                <Spin size="large" tip="计算牛熊市状态..." />
              </div>
            ) : Object.keys(regimeChartData).length === 0 ? (
              <Empty description={`暂无牛熊市数据（${selectedCycle === 'short' ? '短周期需要至少30天' : selectedCycle === 'medium' ? '中周期需要至少60天' : '长周期需要至少120天'}数据）`} style={{ padding: 100 }} />
            ) : (
              <div>
                {/* 周期选择器 */}
                <div style={{ marginBottom: 16 }}>
                  <span style={{ marginRight: 12, fontWeight: 'bold' }}>📈 周期选择：</span>
                  <Radio.Group value={selectedCycle} onChange={(e) => setSelectedCycle(e.target.value)} buttonStyle="solid">
                    <Radio.Button value="short">短周期 (3/5/10)</Radio.Button>
                    <Radio.Button value="medium">中周期 (5/10/20)</Radio.Button>
                    <Radio.Button value="long">长周期 (10/20/40)</Radio.Button>
                  </Radio.Group>
                  <span style={{ marginLeft: 12, color: '#8c8c8c', fontSize: 12 }}>
                    {selectedCycle === 'short' ? '适合短线交易，回看30天' : selectedCycle === 'medium' ? '适合波段交易，回看60天' : '适合中长线投资，回看120天'}
                  </span>
                </div>
                {/* 图例说明 */}
                <div style={{ marginBottom: 16, padding: 12, background: '#fafafa', borderRadius: 4, fontSize: 13 }}>
                  <div style={{ marginBottom: 8, fontWeight: 'bold' }}>📊 图表说明</div>
                  <div style={{ display: 'flex', flexWrap: 'wrap', gap: 16 }}>
                    <div>
                      <span style={{ color: '#1890ff', fontWeight: 'bold' }}>蓝色线</span>：牛熊得分（左Y轴，0-100分）
                    </div>
                    <div>
                      <span style={{ color: '#722ed1', fontWeight: 'bold' }}>紫色线</span>：收盘价（右Y轴）
                    </div>
                    <div>
                      <span style={{ color: '#f5222d' }}>红色虚线</span>：牛市线（70分）
                    </div>
                    <div>
                      <span style={{ color: '#52c41a' }}>绿色虚线</span>：熊市线（40分）
                    </div>
                  </div>
                  <div style={{ marginTop: 8, display: 'flex', gap: 16 }}>
                    <Tag color="red">牛市 ≥70分</Tag>
                    <Tag color="default">震荡 40-70分</Tag>
                    <Tag color="green">熊市 ≤40分</Tag>
                  </div>
                </div>
                {stockCodes.filter(code => regimeChartData[code] && regimeChartData[code].length > 0).map((code) => (
                  <div key={code} style={{ marginBottom: 32 }}>
                    <div style={{ marginBottom: 8, fontWeight: 'bold', fontSize: 16 }}>{code}</div>
                    <ResponsiveContainer width="100%" height={300}>
                      <ComposedChart data={regimeChartData[code]}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="date" tick={{ fontSize: 10 }} />
                        <YAxis yAxisId="left" domain={[-60, 100]} tick={{ fontSize: 10 }} label={{ value: '得分', angle: -90, position: 'insideLeft', fontSize: 11 }} />
                        <YAxis yAxisId="right" orientation="right" domain={['auto', 'auto']} tick={{ fontSize: 10 }} label={{ value: '股价', angle: 90, position: 'insideRight', fontSize: 11 }} />
                        <Tooltip
                          content={({ active, payload, label }) => {
                            if (!active || !payload || !payload.length) return null;
                            const data = payload[0]?.payload;
                            if (!data) return null;
                            const regimeColor = data.regime === 'bull' ? '#f5222d' : data.regime === 'bear' ? '#52c41a' : '#8c8c8c';
                            return (
                              <div style={{ background: 'white', border: '1px solid #ccc', padding: 8, borderRadius: 4 }}>
                                <div style={{ fontWeight: 'bold', marginBottom: 4 }}>{label}</div>
                                <div>收盘价: <span style={{ color: '#722ed1' }}>{data.close?.toFixed(2)}</span></div>
                                <div>牛熊得分: <span style={{ color: '#1890ff' }}>{data.total_score}</span></div>
                                <div>状态: <span style={{ color: regimeColor }}>{data.regime === 'bull' ? '🐂 牛市' : data.regime === 'bear' ? '🐻 熊市' : '📊 震荡'}</span></div>
                              </div>
                            );
                          }}
                        />
                        <Legend />
                        {/* 牛市区域背景 */}
                        <Area
                          yAxisId="left"
                          type="monotone"
                          dataKey="bull_area"
                          fill="rgba(245, 34, 45, 0.15)"
                          stroke="none"
                          name="牛市区域"
                        />
                        {/* 熊市区域背景 */}
                        <Area
                          yAxisId="left"
                          type="monotone"
                          dataKey="bear_area"
                          fill="rgba(82, 196, 26, 0.15)"
                          stroke="none"
                          name="熊市区域"
                        />
                        {/* 牛市线（左Y轴70分） */}
                        <ReferenceLine yAxisId="left" y={70} stroke="#f5222d" strokeDasharray="3 3" label={{ value: '牛市线 70', position: 'left', fill: '#f5222d', fontSize: 10 }} />
                        {/* 熊市线（左Y轴40分） */}
                        <ReferenceLine yAxisId="left" y={40} stroke="#52c41a" strokeDasharray="3 3" label={{ value: '熊市线 40', position: 'left', fill: '#52c41a', fontSize: 10 }} />
                        {/* 得分线 */}
                        <Line
                          yAxisId="left"
                          type="monotone"
                          dataKey="total_score"
                          stroke="#1890ff"
                          strokeWidth={2}
                          dot={false}
                          name="牛熊得分"
                        />
                        {/* 收盘价线 */}
                        <Line
                          yAxisId="right"
                          type="monotone"
                          dataKey="close"
                          stroke="#722ed1"
                          strokeWidth={1.5}
                          dot={false}
                          name="收盘价"
                        />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                ))}
              </div>
            ),
          },
        ]}
      />
    </Card>
  );
}
