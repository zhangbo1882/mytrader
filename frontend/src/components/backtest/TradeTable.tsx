import { Table } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { BacktestResult } from '@/types';

interface TradeTableProps {
  trades: BacktestResult['trades'];
}

export function TradeTable({ trades }: TradeTableProps) {
  const columns: ColumnsType<BacktestResult['trades'][number]> = [
    {
      title: '买入日期',
      dataIndex: 'buy_date',
      key: 'buy_date',
      width: 120,
      sorter: (a, b) => a.buy_date.localeCompare(b.buy_date),
    },
    {
      title: '卖出日期',
      dataIndex: 'sell_date',
      key: 'sell_date',
      width: 120,
      render: (val) => val || '-',
    },
    {
      title: '买入价',
      dataIndex: 'buy_price_original',
      key: 'buy_price_original',
      width: 100,
      render: (val) => val != null ? `¥${val.toFixed(2)}` : '-',
    },
    {
      title: '卖出价',
      dataIndex: 'sell_price_original',
      key: 'sell_price_original',
      width: 100,
      render: (val) => val != null ? `¥${val.toFixed(2)}` : '-',
    },
    {
      title: '持仓量',
      dataIndex: 'size',
      key: 'size',
      width: 100,
    },
    {
      title: '持仓天数',
      dataIndex: 'hold_days',
      key: 'hold_days',
      width: 100,
    },
    {
      title: '买入金额',
      dataIndex: 'buy_value',
      key: 'buy_value',
      width: 120,
      render: (val) => `¥${val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
    },
    {
      title: '卖出金额',
      dataIndex: 'sell_value',
      key: 'sell_value',
      width: 120,
      render: (val) => val != null ? `¥${val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '-',
    },
    {
      title: '毛利润',
      dataIndex: 'gross_pnl',
      key: 'gross_pnl',
      width: 120,
      render: (val) => (
        <span style={{ color: val >= 0 ? '#cf1322' : '#3f8600' }}>
          ¥{val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </span>
      ),
      sorter: (a, b) => a.gross_pnl - b.gross_pnl,
    },
    {
      title: '手续费',
      dataIndex: 'commission',
      key: 'commission',
      width: 100,
      render: (val) => `¥${val.toFixed(2)}`,
    },
    {
      title: '净利润',
      dataIndex: 'net_pnl',
      key: 'net_pnl',
      width: 120,
      render: (val) => (
        <span style={{ color: val >= 0 ? '#cf1322' : '#3f8600', fontWeight: 'bold' }}>
          ¥{val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </span>
      ),
      sorter: (a, b) => a.net_pnl - b.net_pnl,
    },
    {
      title: '收益率',
      dataIndex: 'pnl_pct',
      key: 'pnl_pct',
      width: 100,
      render: (val) => (
        <span style={{ color: val >= 0 ? '#cf1322' : '#3f8600' }}>
          {val.toFixed(2)}%
        </span>
      ),
      sorter: (a, b) => a.pnl_pct - b.pnl_pct,
    },
  ];

  return (
    <Table
      columns={columns}
      dataSource={trades}
      rowKey={(record, index) => `${record.buy_date}-${index}`}
      scroll={{ x: 1500 }}
      pagination={{
        pageSize: 20,
        showSizeChanger: true,
        showTotal: (total) => `共 ${total} 笔交易`,
      }}
    />
  );
}
