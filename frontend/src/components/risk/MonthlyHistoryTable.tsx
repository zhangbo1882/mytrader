/**
 * 月度历史表格组件
 */
import React from 'react';
import { Table, Tag, Button, Space, Modal, InputNumber, Input, message } from 'antd';
import { EditOutlined, RiseOutlined, FallOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { MonthlySnapshot } from '../../types/risk.types';

interface MonthlyHistoryTableProps {
  snapshots: MonthlySnapshot[];
  onEditCapitalChange?: (yearMonth: string, amount: number, reason: string) => void;
  loading?: boolean;
}

const MonthlyHistoryTable: React.FC<MonthlyHistoryTableProps> = ({
  snapshots,
  onEditCapitalChange,
  loading
}) => {
  const [editModalVisible, setEditModalVisible] = React.useState(false);
  const [editingSnapshot, setEditingSnapshot] = React.useState<MonthlySnapshot | null>(null);
  const [editAmount, setEditAmount] = React.useState<number>(0);
  const [editReason, setEditReason] = React.useState<string>('');

  const handleEdit = (record: MonthlySnapshot) => {
    setEditingSnapshot(record);
    setEditAmount(record.capital_change || 0);
    setEditReason(record.capital_change_reason || '');
    setEditModalVisible(true);
  };

  const handleSaveEdit = () => {
    if (editingSnapshot && onEditCapitalChange) {
      onEditCapitalChange(editingSnapshot.year_month, editAmount, editReason);
      message.success('资金变动已更新');
    }
    setEditModalVisible(false);
    setEditingSnapshot(null);
  };

  const columns: ColumnsType<MonthlySnapshot> = [
    {
      title: '月份',
      dataIndex: 'year_month',
      key: 'year_month',
      width: 90,
      fixed: 'left',
    },
    {
      title: '月初资金',
      dataIndex: 'month_start_capital',
      key: 'month_start_capital',
      width: 100,
      align: 'right',
      render: (val: number) => `¥${val.toLocaleString()}`,
    },
    {
      title: '月末资金',
      dataIndex: 'month_end_capital',
      key: 'month_end_capital',
      width: 100,
      align: 'right',
      render: (val: number) => `¥${val.toLocaleString()}`,
    },
    {
      title: '本月盈亏',
      dataIndex: 'month_pnl',
      key: 'month_pnl',
      width: 100,
      align: 'right',
      render: (val: number) => {
        const color = val >= 0 ? '#cf1322' : '#3f8600';
        const icon = val >= 0 ? <RiseOutlined /> : <FallOutlined />;
        return (
          <span style={{ color }}>
            {icon} {val >= 0 ? '+' : ''}¥{val.toLocaleString()}
          </span>
        );
      },
    },
    {
      title: '盈亏比例',
      dataIndex: 'month_pnl_percent',
      key: 'month_pnl_percent',
      width: 80,
      align: 'right',
      render: (val: number) => (
        <Tag color={val >= 0 ? 'red' : 'green'}>
          {val >= 0 ? '+' : ''}{val.toFixed(2)}%
        </Tag>
      ),
    },
    {
      title: '资金变动',
      dataIndex: 'capital_change',
      key: 'capital_change',
      width: 90,
      align: 'right',
      render: (val: number) => {
        if (!val || val === 0) return <Tag>无</Tag>;
        return (
          <Tag color={val > 0 ? 'blue' : 'orange'}>
            {val > 0 ? '+' : ''}¥{val.toLocaleString()}
          </Tag>
        );
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 60,
      fixed: 'right',
      render: (_, record) => (
        <Button
          type="link"
          size="small"
          icon={<EditOutlined />}
          onClick={() => handleEdit(record)}
        />
      ),
    },
  ];

  return (
    <>
      <Table
        columns={columns}
        dataSource={snapshots}
        rowKey="id"
        size="small"
        loading={loading}
        pagination={false}
        scroll={{ x: 600 }}
        locale={{ emptyText: '暂无历史记录' }}
      />

      <Modal
        title="编辑资金变动"
        open={editModalVisible}
        onOk={handleSaveEdit}
        onCancel={() => setEditModalVisible(false)}
        okText="保存"
        cancelText="取消"
      >
        {editingSnapshot && (
          <Space direction="vertical" style={{ width: '100%' }}>
            <div>月份: <strong>{editingSnapshot.year_month}</strong></div>
            <div>
              资金变动金额:
              <InputNumber
                style={{ width: '100%', marginTop: 8 }}
                value={editAmount}
                onChange={(v) => setEditAmount(v || 0)}
                formatter={(v) => `¥ ${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
                parser={(v) => v?.replace(/¥\s?|(,*)/g, '') as unknown as number}
              />
            </div>
            <div>
              变动原因:
              <Input
                style={{ marginTop: 8 }}
                value={editReason}
                onChange={(e) => setEditReason(e.target.value)}
                placeholder="请输入变动原因"
              />
            </div>
          </Space>
        )}
      </Modal>
    </>
  );
};

export default MonthlyHistoryTable;
