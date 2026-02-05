import { useState, memo } from 'react';
import { Modal, Table, Tag, Button, Space, message, Typography, Progress, Card, Row, Col, Statistic } from 'antd';
import {
  EyeOutlined,
  SearchOutlined,
  FilterOutlined,
  RiseOutlined,
  FallOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { BoardDetail, Stock } from '@/types';
import { formatPercent, formatCurrency, formatNumber } from '@/utils';

const { Text } = Typography;

interface ConstituentsModalProps {
  visible: boolean;
  board: BoardDetail | null;
  onClose: () => void;
  onViewStock: (stockCode: string) => void;
  onBatchQuery?: (stockCodes: string[]) => void;
}

export const ConstituentsModal = memo(function ConstituentsModal({
  visible,
  board,
  onClose,
  onViewStock,
  onBatchQuery,
}: ConstituentsModalProps) {
  if (!board) return null;

  const [selectedCodes, setSelectedCodes] = useState<string[]>([]);

  // 查看股票详情（跳转到查询页面）
  const handleViewStock = (code: string) => {
    onViewStock(code);
    onClose();
  };

  // 批量查询
  const handleBatchQuery = () => {
    if (selectedCodes.length === 0) {
      message.warning('请先选择要查询的股票');
      return;
    }

    if (onBatchQuery) {
      onBatchQuery(selectedCodes);
      onClose();
    }
  };

  const columns: ColumnsType<Stock> = [
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
      sorter: (a, b) => a.name.localeCompare(b.name),
    },
    {
      title: '市值（亿元）',
      key: 'marketCap',
      width: 120,
      render: (_, record) => {
        const cap = (record as any).marketCap;
        return cap != null ? formatCurrency(Number(cap) / 100000000) : '-';
      },
    },
    {
      title: '涨跌幅',
      key: 'changePercent',
      width: 100,
      render: (_, record) => {
        const change = (record as any).changePercent;
        if (change == null) return '-';
        return (
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            {change > 0 ? <RiseOutlined style={{ color: '#cf1322' }} aria-hidden="true" /> : <FallOutlined style={{ color: '#3f8600' }} aria-hidden="true" />}
            <Text style={{ color: change > 0 ? '#cf1322' : '#3f8600' }}>
              {formatPercent(change)}
            </Text>
          </div>
        );
      },
    },
    {
      title: '操作',
      key: 'action',
      width: 150,
      fixed: 'right',
      render: (_, record) => (
        <Space size="small">
          <Button
            size="small"
            icon={<EyeOutlined aria-hidden="true" />}
            onClick={() => handleViewStock(record.code)}
          >
            查看详情
          </Button>
          <Button
            size="small"
            type={selectedCodes.includes(record.code) ? 'default' : 'primary'}
            onClick={() => {
              if (selectedCodes.includes(record.code)) {
                setSelectedCodes(selectedCodes.filter((c) => c !== record.code));
              } else {
                setSelectedCodes([...selectedCodes, record.code]);
              }
            }}
          >
            {selectedCodes.includes(record.code) ? '已选择' : '选择'}
          </Button>
        </Space>
      ),
    },
  ];

  // 行选择配置
  const rowSelection = {
    selectedRowKeys: selectedCodes,
    onChange: (selectedRowKeys: React.Key[]) => {
      setSelectedCodes(selectedRowKeys as string[]);
    },
  };

  return (
    <Modal
      title={
        <Space>
          <span>{board.name}</span>
          <Tag color="blue">{board.category}</Tag>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      width={900}
      footer={
        onBatchQuery ? (
          <Space>
            <span style={{ color: '#999', fontSize: 12 }}>
              已选 {selectedCodes.length} 只
            </span>
            <Button onClick={() => setSelectedCodes([])}>清空选择</Button>
            <Button type="primary" onClick={handleBatchQuery} disabled={selectedCodes.length === 0}>
              查询选中股票
            </Button>
          </Space>
        ) : null
      }
    >
      {/* 板块信息 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Row gutter={16}>
          <Col span={6}>
            <Statistic
              title="成分股数量"
              value={board.stockCount}
              suffix="只"
              valueStyle={{ fontSize: 16 }}
            />
          </Col>
          {board.valuation?.pe && (
            <Col span={6}>
              <Statistic
                title="平均市盈率"
                value={board.valuation.pe}
                valueStyle={{ fontSize: 16 }}
              />
            </Col>
          )}
          {board.valuation?.pb && (
            <Col span={6}>
              <Statistic
                title="平均市净率"
                value={board.valuation.pb}
                valueStyle={{ fontSize: 16 }}
              />
            </Col>
          )}
          {board.valuation?.marketCap && (
            <Col span={6}>
              <Statistic
                title="总市值"
                value={formatCurrency(board.valuation.marketCap)}
                valueStyle={{ fontSize: 16 }}
              />
            </Col>
          )}
        </Row>
      </Card>

      {/* 成分股列表 */}
      {board.stocks && board.stocks.length > 0 && (
        <Table
          columns={columns}
          dataSource={board.stocks}
          rowKey="code"
          rowSelection={onBatchQuery ? rowSelection : undefined}
          scroll={{ x: 700 }}
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showTotal: (total) => `共 ${total} 只`,
          }}
          size="small"
        />
      )}
    </Modal>
  );
});
