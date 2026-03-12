import { useEffect, useState } from 'react';
import { Typography, Tabs, Button, Space, Alert } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { TaskProgress } from '@/components/tasks/TaskProgress';
import {
  BacktestParamsForm,
  BasicInfoCards,
  TradeTable,
  HealthMetrics,
  EquityCurve,
  BenchmarkComparison,
  BacktestHistory,
} from '@/components/backtest';
import { useBacktestPolling } from '@/hooks';
import { backtestService } from '@/services';
import type { BacktestRequest } from '@/types';

const { Title } = Typography;

type ViewState = 'params' | 'running' | 'result' | 'error';
type TabKey = 'backtest' | 'history';

function BacktestPage() {
  const [taskId, setTaskId] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [view, setView] = useState<ViewState>('params');
  const [errorMsg, setErrorMsg] = useState('');
  const [activeTab, setActiveTab] = useState<TabKey>('backtest');
  const [historyRefreshTrigger, setHistoryRefreshTrigger] = useState(0);

  const { status, result, error, isPolling } = useBacktestPolling(taskId);

  useEffect(() => {
    if (view === 'running' && result && !isPolling) {
      setView('result');
    }
  }, [view, result, isPolling]);

  useEffect(() => {
    if (view === 'running' && error && !isPolling) {
      setView('error');
      setErrorMsg(error);
    }
  }, [view, error, isPolling]);

  const handleSubmit = async (params: BacktestRequest) => {
    setSubmitting(true);
    try {
      const response = await backtestService.run(params);
      setTaskId(response.task_id);
      setView('running');
      setErrorMsg('');
    } catch (err) {
      setErrorMsg(err instanceof Error ? err.message : '提交失败');
      setView('error');
    } finally {
      setSubmitting(false);
    }
  };

  const handleReset = () => {
    setTaskId(null);
    setView('params');
    setErrorMsg('');
  };

  const handleTabChange = (key: string) => {
    setActiveTab(key as TabKey);
    if (key === 'history') {
      setHistoryRefreshTrigger((prev) => prev + 1);
    }
  };

  const renderBacktestContent = () => {
    if (view === 'params') {
      return <BacktestParamsForm onSubmit={handleSubmit} loading={submitting} />;
    }

    if (view === 'running' && status) {
      return (
        <div style={{ padding: '24px 0' }}>
          <TaskProgress
            status={status.status === 'completed' ? 'completed' : status.status === 'failed' ? 'failed' : 'running'}
            processed={status.progress || 0}
            total={100}
            failed={0}
            progress={status.progress}
          />
          <div style={{ marginTop: 16, textAlign: 'center', color: '#666' }}>
            {status.message || '正在执行回测...'}
          </div>
        </div>
      );
    }

    if (view === 'error') {
      return (
        <Alert
          message="回测失败"
          description={errorMsg}
          type="error"
          showIcon
          action={
            <Button size="small" onClick={handleReset}>
              重新配置
            </Button>
          }
          style={{ marginBottom: 24 }}
        />
      );
    }

    if (view === 'result' && result) {
      return (
        <div>
          <Space style={{ marginBottom: 16 }}>
            <Button icon={<ReloadOutlined />} onClick={handleReset}>
              重新运行
            </Button>
          </Space>

          <Tabs
            defaultActiveKey="basic"
            items={[
              {
                key: 'basic',
                label: '基础信息',
                children: <BasicInfoCards result={result} />,
              },
              {
                key: 'trades',
                label: `交易明细 (${result.trades.length})`,
                children: <TradeTable trades={result.trades} />,
              },
              {
                key: 'metrics',
                label: '健康指标',
                children: <HealthMetrics metrics={result.health_metrics} />,
              },
              {
                key: 'curve',
                label: '收益曲线',
                children: <EquityCurve result={result} />,
              },
              {
                key: 'benchmark',
                label: '基准对比',
                children: result.benchmark_comparison ? (
                  <BenchmarkComparison comparison={result.benchmark_comparison} />
                ) : (
                  <div style={{ padding: 24, textAlign: 'center', color: '#999' }}>
                    未设置基准指数
                  </div>
                ),
              },
            ]}
          />
        </div>
      );
    }

    return null;
  };

  return (
    <div>
      <Title level={2}>策略回测</Title>

      <Tabs
        activeKey={activeTab}
        onChange={handleTabChange}
        items={[
          {
            key: 'backtest',
            label: '执行回测',
            children: renderBacktestContent(),
          },
          {
            key: 'history',
            label: '历史记录',
            children: <BacktestHistory refreshTrigger={historyRefreshTrigger} />,
          },
        ]}
      />
    </div>
  );
}

export default BacktestPage;
