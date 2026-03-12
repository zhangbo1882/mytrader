/**
 * 持仓列表组件
 */
import React from 'react';
import { Table, Button, Space, Tag, Tooltip } from 'antd';
import {
  EditOutlined,
  DeleteOutlined,
  RiseOutlined,
  FallOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { PositionDetail } from '../../types/risk.types';

interface PositionListProps {
  positions: PositionDetail[];
  onAdjustStopLoss: (position: PositionDetail) => void;
  onSell: (position: PositionDetail) => void;
}

const PositionList: React.FC<PositionListProps> = ({
  positions,
  onAdjustStopLoss,
  onSell
}) => {
  // 计算汇总
  const totalMarketValue = positions.reduce((sum, p) => sum + (p.market_value || 0), 0);
  const totalRisk = positions.reduce((sum, p) => sum + (p.total_risk || 0), 0);
  const totalProfit = positions.reduce((sum, p) => sum + (p.total_profit || 0), 0);

  const columns: ColumnsType<PositionDetail> = [
    {
      title: '代码',
      dataIndex: 'symbol',
      key: 'symbol',
      width: 80,
      fixed: 'left',
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      width: 80,
    },
    {
      title: '股数',
      dataIndex: 'shares',
      key: 'shares',
      width: 80,
      align: 'right',
      render: (val: number) => val.toLocaleString(),
    },
    {
      title: '成本',
      dataIndex: 'cost_price',
      key: 'cost_price',
      width: 70,
      align: 'right',
      render: (val: number) => val.toFixed(2),
    },
    {
      title: '现价',
      dataIndex: 'current_price',
      key: 'current_price',
      width: 80,
      align: 'right',
      render: (val: number, record) => (
        <div>
          <div>{val.toFixed(2)}</div>
          {record.price_date && (
            <div style={{ fontSize: '11px', color: '#999' }}>{record.price_date}</div>
          )}
        </div>
      ),
    },
    {
      title: '市值',
      dataIndex: 'market_value',
      key: 'market_value',
      width: 100,
      align: 'right',
      render: (val: number) => `¥${val.toLocaleString()}`,
    },
    {
      title: '止损价',
      dataIndex: 'stop_loss_price',
      key: 'stop_loss_price',
      width: 70,
      align: 'right',
      render: (val: number) => val.toFixed(2),
    },
    {
      title: '止损比例',
      dataIndex: 'stop_loss_percent',
      key: 'stop_loss_percent',
      width: 60,
      align: 'right',
      render: (val: number) => `${val.toFixed(1)}%`,
    },
    {
      title: '风险占用',
      dataIndex: 'total_risk',
      key: 'total_risk',
      width: 100,
      align: 'right',
      render: (val: number) => (
        <Tooltip title="从成本价到止损价的风险">
          <span>¥{(val || 0).toLocaleString()}</span>
        </Tooltip>
      ),
    },
    {
      title: '盈亏',
      dataIndex: 'total_profit',
      key: 'total_profit',
      width: 110,
      align: 'right',
      render: (val: number, record) => {
        const color = val >= 0 ? '#cf1322' : '#3f8600';
        const icon = val >= 0 ? <RiseOutlined /> : <FallOutlined />;
        // 计算盈亏比例
        const profitPercent = record.cost_price > 0
          ? ((record.current_price - record.cost_price) / record.cost_price * 100)
          : 0;
        const percentColor = profitPercent >= 0 ? '#cf1322' : '#3f8600';
        return (
          <div>
            <span style={{ color }}>
              {icon} ¥{Math.abs(val).toLocaleString()}
            </span>
            <br />
            <span style={{ color: percentColor, fontSize: '12px' }}>
              {profitPercent >= 0 ? '+' : ''}{profitPercent.toFixed(2)}%
            </span>
          </div>
        );
      },
    },
    {
      title: '锁定利润',
      dataIndex: 'locked_profit',
      key: 'locked_profit',
      width: 100,
      align: 'right',
      render: (val: number) => {
        if (val >= 0) {
          // 正数：红色，表示锁定利润
          return <Tag color="red">+¥{val.toLocaleString()}</Tag>;
        }
        // 负数：绿色，表示亏损
        return <Tag color="green">-¥{Math.abs(val).toLocaleString()}</Tag>;
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 120,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Tooltip title="调整止损">
            <Button
              type="link"
              size="small"
              icon={<EditOutlined />}
              onClick={() => onAdjustStopLoss(record)}
            />
          </Tooltip>
          <Tooltip title="卖出">
            <Button
              type="link"
              size="small"
              danger
              icon={<DeleteOutlined />}
              onClick={() => onSell(record)}
            />
          </Tooltip>
        </Space>
      ),
    },
  ];

  return (
    <Table
      columns={columns}
      dataSource={positions}
      rowKey="symbol"
      size="small"
      pagination={false}
      locale={{ emptyText: '暂无持仓' }}
      summary={() => {
        if (positions.length === 0) return null;
        const profitColor = totalProfit >= 0 ? '#cf1322' : '#3f8600';
        return (
          <Table.Summary fixed>
            <Table.Summary.Row>
              <Table.Summary.Cell index={0} colSpan={5}>
                <strong>合计</strong>
              </Table.Summary.Cell>
              <Table.Summary.Cell index={5} align="right">
                <strong>¥{totalMarketValue.toLocaleString()}</strong>
              </Table.Summary.Cell>
              <Table.Summary.Cell index={6} colSpan={2} />
              <Table.Summary.Cell index={8} align="right">
                <Tag color="orange">¥{totalRisk.toLocaleString()}</Tag>
              </Table.Summary.Cell>
              <Table.Summary.Cell index={9} align="right">
                <span style={{ color: profitColor, fontWeight: 'bold' }}>
                  ¥{totalProfit.toLocaleString()}
                </span>
              </Table.Summary.Cell>
              <Table.Summary.Cell index={10} colSpan={2} />
            </Table.Summary.Row>
          </Table.Summary>
        );
      }}
    />
  );
};

export default PositionList;
