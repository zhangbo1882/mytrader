import { Card, Table, Typography, Tabs } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import type { FinancialReport } from '@/types';
import { formatNumber, formatCurrency, formatPercent } from '@/utils';

const { Text } = Typography;

interface FullReportViewProps {
  report: FinancialReport;
  loading?: boolean;
}

export function FullReportView({ report, loading }: FullReportViewProps) {
  // 利润表表格列
  const incomeColumns: ColumnsType<any> = [
    {
      title: '项目',
      dataIndex: 'item',
      key: 'item',
      fixed: 'left',
      width: 200,
    },
    {
      title: '金额（万元）',
      dataIndex: 'value',
      key: 'value',
      align: 'right',
      render: (value) => formatNumber(value, 2),
    },
    {
      title: '日期',
      dataIndex: 'date',
      key: 'date',
      width: 120,
    },
  ];

  // 资产负债表表格列
  const balanceColumns: ColumnsType<any> = [
    {
      title: '项目',
      dataIndex: 'item',
      key: 'item',
      fixed: 'left',
      width: 200,
    },
    {
      title: '金额（万元）',
      dataIndex: 'value',
      key: 'value',
      align: 'right',
      render: (value) => formatNumber(value, 2),
    },
    {
      title: '日期',
      dataIndex: 'date',
      key: 'date',
      width: 120,
    },
  ];

  // 现金流量表表格列
  const cashflowColumns: ColumnsType<any> = [
    {
      title: '项目',
      dataIndex: 'item',
      key: 'item',
      fixed: 'left',
      width: 200,
    },
    {
      title: '金额（万元）',
      dataIndex: 'value',
      key: 'value',
      align: 'right',
      render: (value) => formatNumber(value, 2),
    },
    {
      title: '日期',
      dataIndex: 'date',
      key: 'date',
      width: 120,
    },
  ];

  // 财务指标表格列
  const indicatorColumns: ColumnsType<any> = [
    {
      title: '指标名称',
      dataIndex: 'item',
      key: 'item',
      fixed: 'left',
      width: 200,
    },
    {
      title: '数值',
      dataIndex: 'value',
      key: 'value',
      align: 'right',
      render: (value, record) => {
        // 根据单位格式化
        if (record.unit === 'percent') {
          return formatPercent(Number(value));
        } else if (record.unit === 'currency') {
          return formatCurrency(Number(value));
        }
        return formatNumber(Number(value), 2);
      },
    },
    {
      title: '单位',
      dataIndex: 'unit',
      key: 'unit',
      width: 100,
      render: (unit) => {
        const unitMap: Record<string, string> = {
          percent: '%',
          currency: '元',
          number: '倍',
        };
        return unitMap[unit] || unit || '-';
      },
    },
    {
      title: '日期',
      dataIndex: 'date',
      key: 'date',
      width: 120,
    },
  ];

  const tableItems = [
    {
      key: '1',
      label: '利润表',
      children: (
        <Table
          columns={incomeColumns}
          dataSource={report.income || []}
          rowKey={(record, index) => `${record.item}-${index}`}
          loading={loading}
          scroll={{ x: 600 }}
          pagination={false}
          size="small"
          bordered
        />
      ),
    },
    {
      key: '2',
      label: '资产负债表',
      children: (
        <Table
          columns={balanceColumns}
          dataSource={report.balance || []}
          rowKey={(record, index) => `${record.item}-${index}`}
          loading={loading}
          scroll={{ x: 600 }}
          pagination={false}
          size="small"
          bordered
        />
      ),
    },
    {
      key: '3',
      label: '现金流量表',
      children: (
        <Table
          columns={cashflowColumns}
          dataSource={report.cashflow || []}
          rowKey={(record, index) => `${record.item}-${index}`}
          loading={loading}
          scroll={{ x: 600 }}
          pagination={false}
          size="small"
          bordered
        />
      ),
    },
    {
      key: '4',
      label: '财务指标',
      children: (
        <Table
          columns={indicatorColumns}
          dataSource={report.indicators || []}
          rowKey={(record, index) => `${record.item}-${index}`}
          loading={loading}
          scroll={{ x: 700 }}
          pagination={false}
          size="small"
          bordered
        />
      ),
    },
  ];

  return (
    <Card title={<Text strong>完整财务报表</Text>}>
      <Tabs
        defaultActiveKey="1"
        items={tableItems}
        size="small"
      />
    </Card>
  );
}
