import { Card, Table, Typography, Statistic, Row, Col, Tag, Button, Space } from 'antd';
import { CheckCircleOutlined, StockOutlined, SaveOutlined } from '@ant-design/icons';
import type { ScreeningResult } from '@/types';

const { Title } = Typography;

interface ScreeningResultsProps {
  result: ScreeningResult;
  onSave?: () => void;
}

function ScreeningResults({ result, onSave }: ScreeningResultsProps) {
  const columns = [
    {
      title: '代码',
      dataIndex: 'code',
      key: 'code',
      width: 100,
      fixed: 'left' as const,
      render: (code: string) => <Tag color="blue">{code}</Tag>
    },
    {
      title: '名称',
      dataIndex: 'name',
      key: 'name',
      width: 120
    },
    {
      title: '最新价',
      dataIndex: 'latest_close',
      key: 'latest_close',
      width: 100,
      render: (price: number) => `¥${price?.toFixed(2) || '-'}`
    },
    {
      title: 'PE(TTM)',
      dataIndex: 'pe_ttm',
      key: 'pe_ttm',
      width: 100,
      render: (pe: number | null) => pe !== null ? pe.toFixed(2) : '-'
    },
    {
      title: 'PB',
      dataIndex: 'pb',
      key: 'pb',
      width: 100,
      render: (pb: number | null) => pb !== null ? pb.toFixed(2) : '-'
    },
    {
      title: '总市值(亿)',
      dataIndex: 'total_mv_yi',
      key: 'total_mv_yi',
      width: 120,
      render: (mv: number | null) => mv !== null ? mv.toFixed(2) : '-'
    }
  ];

  return (
    <Card style={{ marginTop: 16 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={4} style={{ marginBottom: 0 }}>
          <CheckCircleOutlined style={{ color: '#52c41a', marginRight: 8 }} />
          筛选结果
        </Title>
        {onSave && (
          <Button
            type="primary"
            icon={<SaveOutlined />}
            onClick={onSave}
          >
            保存到历史
          </Button>
        )}
      </div>

      {/* Statistics */}
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={8}>
          <Statistic
            title="筛选结果数量"
            value={result.count}
            prefix={<StockOutlined />}
            valueStyle={{ color: '#1890ff' }}
          />
        </Col>
        <Col span={8}>
          <Statistic
            title="筛选状态"
            value={result.success ? "成功" : "失败"}
            valueStyle={{ color: result.success ? '#52c41a' : '#ff4d4f' }}
          />
        </Col>
        <Col span={8}>
          <Statistic
            title="数据完整性"
            value={result.stocks?.length || 0}
            suffix={`/ ${result.count}`}
            valueStyle={{ color: '#1890ff' }}
          />
        </Col>
      </Row>

      {/* Results table */}
      <Table
        columns={columns}
        dataSource={result.stocks}
        rowKey="code"
        pagination={{
          pageSize: 50,
          showSizeChanger: true,
          pageSizeOptions: ['20', '50', '100', '200'],
          showTotal: (total) => `共 ${total} 条结果`
        }}
        scroll={{ x: 600 }}
        size="small"
      />
    </Card>
  );
}

export default ScreeningResults;
