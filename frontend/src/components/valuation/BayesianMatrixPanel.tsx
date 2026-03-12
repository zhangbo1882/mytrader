import { useState, useEffect, useCallback } from 'react';
import {
  Table, Button, Tag, Space, Alert, Typography, Tooltip,
  Modal, Select, InputNumber, Popover, Progress, Badge
} from 'antd';
import {
  ReloadOutlined, InfoCircleOutlined,
  ClockCircleOutlined, ExperimentOutlined
} from '@ant-design/icons';
import { valuationService } from '@/services/valuationService';
import api from '@/services/api';

const { Text } = Typography;

interface PriorRow {
  industry_code: string;
  industry_name: string;
  level: string;
  method: string;
  accuracy: number;
  sample_count: number;
  stock_count: number;
  updated_at: string;
}

interface TableRow {
  key: string;
  industry_code: string;
  industry_name: string;
  level: string;
  pe?: number;
  pb?: number;
  ps?: number;
  dcf?: number;
  sample_count?: number;
  updated_at?: string;
}

function accuracyColor(val?: number): string {
  if (val === undefined) return '#d9d9d9';
  if (val >= 0.60) return '#f5222d';
  if (val >= 0.52) return '#faad14';
  return '#52c41a';
}

function AccuracyCell({ value }: { value?: number }) {
  if (value === undefined) return <Text type="secondary">—</Text>;
  return (
    <span style={{ color: accuracyColor(value), fontWeight: 500 }}>
      {(value * 100).toFixed(1)}%
    </span>
  );
}

function buildTableRows(rows: PriorRow[]): TableRow[] {
  const map = new Map<string, TableRow>();
  for (const r of rows) {
    const key = `${r.level}:${r.industry_code}`;
    if (!map.has(key)) {
      map.set(key, {
        key,
        industry_code: r.industry_code,
        industry_name: r.industry_name,
        level: r.level,
        sample_count: r.sample_count,
        updated_at: r.updated_at,
      });
    }
    const row = map.get(key)!;
    if (r.method === 'pe') row.pe = r.accuracy;
    if (r.method === 'pb') row.pb = r.accuracy;
    if (r.method === 'ps') row.ps = r.accuracy;
    if (r.method === 'dcf') row.dcf = r.accuracy;
    // keep latest updated_at
    if (!row.updated_at || r.updated_at > row.updated_at) {
      row.updated_at = r.updated_at;
    }
  }
  return Array.from(map.values()).sort((a, b) => {
    if (a.level !== b.level) return a.level.localeCompare(b.level);
    return a.industry_code.localeCompare(b.industry_code);
  });
}

const columns = [
  {
    title: '行业',
    dataIndex: 'industry_name',
    key: 'industry_name',
    width: 120,
    render: (name: string, row: TableRow) => (
      <Space size={4}>
        <Tag color={row.level === 'L1' ? 'blue' : 'purple'} style={{ fontSize: 10, padding: '0 4px' }}>
          {row.level}
        </Tag>
        <span>{name}</span>
      </Space>
    ),
  },
  {
    title: <Tooltip title="PE估值（市盈率）历史方向准确率">PE</Tooltip>,
    dataIndex: 'pe',
    key: 'pe',
    width: 80,
    align: 'center' as const,
    render: (v?: number) => <AccuracyCell value={v} />,
    sorter: (a: TableRow, b: TableRow) => (a.pe ?? 0) - (b.pe ?? 0),
  },
  {
    title: <Tooltip title="PB估值（市净率）历史方向准确率">PB</Tooltip>,
    dataIndex: 'pb',
    key: 'pb',
    width: 80,
    align: 'center' as const,
    render: (v?: number) => <AccuracyCell value={v} />,
    sorter: (a: TableRow, b: TableRow) => (a.pb ?? 0) - (b.pb ?? 0),
  },
  {
    title: <Tooltip title="PS估值（市销率）历史方向准确率">PS</Tooltip>,
    dataIndex: 'ps',
    key: 'ps',
    width: 80,
    align: 'center' as const,
    render: (v?: number) => <AccuracyCell value={v} />,
    sorter: (a: TableRow, b: TableRow) => (a.ps ?? 0) - (b.ps ?? 0),
  },
  {
    title: <Tooltip title="DCF估值（现金流折现）历史方向准确率">DCF</Tooltip>,
    dataIndex: 'dcf',
    key: 'dcf',
    width: 80,
    align: 'center' as const,
    render: (v?: number) => <AccuracyCell value={v} />,
    sorter: (a: TableRow, b: TableRow) => (a.dcf ?? 0) - (b.dcf ?? 0),
  },
  {
    title: '样本数',
    dataIndex: 'sample_count',
    key: 'sample_count',
    width: 80,
    align: 'center' as const,
    render: (v?: number) => <Text type="secondary">{v ?? '—'}</Text>,
  },
  {
    title: '更新时间',
    dataIndex: 'updated_at',
    key: 'updated_at',
    width: 140,
    render: (v?: string) => (
      <Text type="secondary" style={{ fontSize: 12 }}>
        {v ? v.slice(0, 16) : '—'}
      </Text>
    ),
  },
];

function RefreshModal({
  open,
  onClose,
  onSuccess,
}: {
  open: boolean;
  onClose: () => void;
  onSuccess: (taskId: string) => void;
}) {
  const [level, setLevel] = useState<'L1' | 'L2' | 'both'>('both');
  const [years, setYears] = useState(3);
  const [loading, setLoading] = useState(false);

  const handleOk = async () => {
    setLoading(true);
    try {
      const res: any = await valuationService.refreshPriorMatrix({ level, years });
      onSuccess(res.task_id);
      onClose();
    } catch {
      // error handled by axios interceptor
    } finally {
      setLoading(false);
    }
  };

  const durationHint =
    level === 'L1' ? '约 30 分钟' :
    level === 'L2' ? '约 1-2 小时' :
    '约 2-3 小时';

  return (
    <Modal
      title="刷新贝叶斯先验矩阵"
      open={open}
      onCancel={onClose}
      onOk={handleOk}
      confirmLoading={loading}
      okText="开始计算"
      cancelText="取消"
      width={420}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        <Alert
          message="建议更新时机"
          description={
            <ul style={{ margin: '4px 0 0', paddingLeft: 16 }}>
              <li>每季度季报发布后（3/6/9/12月）</li>
              <li>新增大量历史价格数据后</li>
              <li>估值准确率明显下降时</li>
            </ul>
          }
          type="info"
          showIcon
        />
        <div>
          <div style={{ marginBottom: 6, color: '#666' }}>回测粒度</div>
          <Select
            value={level}
            onChange={setLevel}
            style={{ width: '100%' }}
            options={[
              { label: 'L1 仅申万一级行业（31个，约30分钟）', value: 'L1' },
              { label: 'L2 仅申万二级行业（~100个，约1-2小时）', value: 'L2' },
              { label: 'L1+L2 全量（推荐，约2-3小时）', value: 'both' },
            ]}
          />
          <Text type="secondary" style={{ fontSize: 12 }}>预计耗时：{durationHint}（后台运行，不影响使用）</Text>
        </div>
        <div>
          <div style={{ marginBottom: 6, color: '#666' }}>回测年数</div>
          <InputNumber min={1} max={10} value={years} onChange={v => setYears(v ?? 3)} style={{ width: 80 }} />
        </div>
      </Space>
    </Modal>
  );
}

export function BayesianMatrixPanel() {
  const [rows, setRows] = useState<PriorRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [refreshOpen, setRefreshOpen] = useState(false);
  const [runningTaskId, setRunningTaskId] = useState<string | null>(null);
  const [taskProgress, setTaskProgress] = useState(0);
  const [taskMessage, setTaskMessage] = useState('');

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const res: any = await valuationService.getPriorMatrixStatus();
      setRows(res.data || []);
    } catch {
      /* ignore */
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchData(); }, [fetchData]);

  // Check for running tasks on mount
  useEffect(() => {
    const checkRunningTask = async () => {
      try {
        const res: any = await api.get('/tasks', { params: { task_type: 'update_valuation_prior', status: 'running' } });
        const tasks = res.tasks || [];
        if (tasks.length > 0) {
          const task = tasks[0];
          setRunningTaskId(task.task_id);
          setTaskProgress(task.progress ?? 0);
          setTaskMessage(task.message ?? '');
        }
      } catch { /* ignore */ }
    };
    checkRunningTask();
  }, []);

  // Poll task progress
  useEffect(() => {
    if (!runningTaskId) return;
    const timer = setInterval(async () => {
      try {
        const res: any = await api.get(`/tasks/${runningTaskId}`);
        const task = res.task || res;
        setTaskProgress(task.progress ?? 0);
        setTaskMessage(task.message ?? '');
        if (task.status === 'completed' || task.status === 'failed') {
          setRunningTaskId(null);
          if (task.status === 'completed') fetchData();
        }
      } catch { /* ignore */ }
    }, 5000);
    return () => clearInterval(timer);
  }, [runningTaskId, fetchData]);

  const tableRows = buildTableRows(rows);
  const l1Count = tableRows.filter(r => r.level === 'L1').length;
  const l2Count = tableRows.filter(r => r.level === 'L2').length;

  const legendInfo = (
    <Space size="large">
      <Space size={4}><span style={{ color: '#f5222d' }}>●</span> ≥60% 高准确率</Space>
      <Space size={4}><span style={{ color: '#faad14' }}>●</span> 52-60% 中等</Space>
      <Space size={4}><span style={{ color: '#52c41a' }}>●</span> &lt;52% 偏低</Space>
    </Space>
  );

  return (
    <div>
      {/* Header */}
      <Space style={{ marginBottom: 16, width: '100%', justifyContent: 'space-between' }} wrap>
        <Space>
          <ExperimentOutlined style={{ color: '#722ed1' }} />
          <Text strong>各行业×估值方法的历史方向准确率</Text>
          <Popover
            content={
              <div style={{ maxWidth: 300 }}>
                <p>对历史季报期运行估值，对比3个月后价格，计算各方法预测涨跌方向的准确率。</p>
                <p>贝叶斯组合方式会将此准确率作为先验权重（准确率高的方法权重更大），并与当前置信度相乘后归一化。</p>
                <p><strong>更新建议：</strong>每季度季报发布后更新一次（3/6/9/12月），可使权重更贴合最新市场特征。</p>
              </div>
            }
            title="什么是贝叶斯先验矩阵？"
          >
            <InfoCircleOutlined style={{ color: '#999', cursor: 'help' }} />
          </Popover>
          {rows.length > 0 && (
            <Space size={4} style={{ fontSize: 12, color: '#999' }}>
              <Tag color="blue">L1 {l1Count}个行业</Tag>
              <Tag color="purple">L2 {l2Count}个行业</Tag>
            </Space>
          )}
        </Space>

        <Space>
          <Button icon={<ReloadOutlined />} onClick={fetchData} loading={loading} size="small">
            刷新
          </Button>
          <Button
            type="primary"
            icon={<ClockCircleOutlined />}
            onClick={() => setRefreshOpen(true)}
            disabled={!!runningTaskId}
            size="small"
          >
            {runningTaskId ? '计算中...' : '更新矩阵'}
          </Button>
        </Space>
      </Space>

      {/* Running task progress */}
      {runningTaskId && (
        <Alert
          style={{ marginBottom: 12 }}
          message={
            <Space>
              <span>后台回测中</span>
              <Badge status="processing" />
            </Space>
          }
          description={
            <div>
              <div style={{ marginBottom: 4, fontSize: 12, color: '#666' }}>{taskMessage}</div>
              <Progress percent={taskProgress} size="small" status="active" />
            </div>
          }
          type="info"
        />
      )}

      {/* Empty state */}
      {rows.length === 0 && !loading && (
        <Alert
          message="尚无数据"
          description={
            <span>
              点击「更新矩阵」触发历史回测（约2-3小时），完成后贝叶斯加权估值将使用历史数据驱动的准确率权重。
              未更新前，贝叶斯方法会使用行业静态权重（等同于分层加权）。
            </span>
          }
          type="warning"
          showIcon
          action={
            <Button size="small" type="primary" onClick={() => setRefreshOpen(true)}>
              立即更新
            </Button>
          }
        />
      )}

      {/* Legend */}
      {rows.length > 0 && (
        <div style={{ marginBottom: 8, fontSize: 12 }}>{legendInfo}</div>
      )}

      {/* Matrix table */}
      {rows.length > 0 && (
        <Table
          dataSource={tableRows}
          columns={columns}
          loading={loading}
          size="small"
          pagination={false}
          scroll={{ x: 700 }}
          rowClassName={(row) => row.level === 'L1' ? 'matrix-row-l1' : 'matrix-row-l2'}
          summary={() => {
            const methods = ['pe', 'pb', 'ps', 'dcf'] as const;
            const avgs = methods.map(m => {
              const vals = tableRows.map(r => r[m]).filter((v): v is number => v !== undefined);
              return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : undefined;
            });
            return (
              <Table.Summary.Row>
                <Table.Summary.Cell index={0}>
                  <Text strong>整体均值</Text>
                </Table.Summary.Cell>
                {avgs.map((avg, i) => (
                  <Table.Summary.Cell key={i} index={i + 1} align="center">
                    <AccuracyCell value={avg} />
                  </Table.Summary.Cell>
                ))}
                <Table.Summary.Cell index={5} colSpan={2} />
              </Table.Summary.Row>
            );
          }}
        />
      )}

      <RefreshModal
        open={refreshOpen}
        onClose={() => setRefreshOpen(false)}
        onSuccess={(taskId) => {
          setRunningTaskId(taskId);
          setTaskProgress(0);
          setTaskMessage('任务已提交，等待Worker执行...');
        }}
      />
    </div>
  );
}
