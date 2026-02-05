import { useMemo } from 'react';
import { Table, Card, Tabs, Empty, Spin } from 'antd';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import type { ColumnsType } from 'antd/es/table';
import type { StockData } from '@/types';
import { formatNumber, formatPercent, formatVolume, formatDate } from '@/utils';

interface QueryResultsProps {
  results: Record<string, StockData[]>;
  loading?: boolean;
}

export function QueryResults({ results, loading }: QueryResultsProps) {
  const stockCodes = Object.keys(results);

  // 将多只股票的数据合并为表格数据
  const tableData = useMemo(() => {
    if (stockCodes.length === 0) return [];

    // 获取所有日期
    const allDates = new Set<string>();
    stockCodes.forEach((code) => {
      results[code].forEach((item) => {
        allDates.add(item.date);
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
          row[`${code}_change`] = data.changePercent || 0;
          row[`${code}_volume`] = data.volume;
          row[`${code}_amount`] = data.amount || data.turnover;
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
            return <span style={{ color }}>{formatPercent(value)}</span>;
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
        allDates.add(item.date);
      });
    });

    const sortedDates = Array.from(allDates).sort((a, b) => a.localeCompare(b));

    return sortedDates.map((date) => {
      const point: any = { date };
      stockCodes.forEach((code) => {
        const data = results[code].find((d) => d.date === date);
        if (data) {
          point[code] = data.close;
        }
      });
      return point;
    });
  }, [results, stockCodes]);

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
                  pageSize: 50,
                  showSizeChanger: true,
                  showTotal: (total) => `共 ${total} 条`,
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
        ]}
      />
    </Card>
  );
}
