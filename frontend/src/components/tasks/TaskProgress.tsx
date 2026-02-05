import { Progress, Typography, Space } from 'antd';
import { SyncOutlined, CheckCircleOutlined, CloseCircleOutlined, PauseCircleOutlined } from '@ant-design/icons';
import type { TaskStatus } from '@/types';
import { TASK_STATUS_MAP } from '@/utils';

const { Text } = Typography;

interface TaskProgressProps {
  status: TaskStatus;
  processed: number;
  total: number;
  failed: number;
  showText?: boolean;
}

export function TaskProgress({ status, processed, total, failed, showText = true }: TaskProgressProps) {
  // Handle case where total is 0 or undefined (task initializing)
  const displayTotal = total || 0;
  const displayProcessed = processed || 0;
  const percent = displayTotal > 0 ? Math.floor((displayProcessed / displayTotal) * 100) : 0;

  // Show initializing message when total is 0 and status is pending/running
  const isInitializing = displayTotal === 0 && (status === 'pending' || status === 'running');
  const isCompleted = percent === 100 && displayProcessed > 0;

  const displayText = isInitializing
    ? '初始化中...'
    : isCompleted
      ? `已完成 ${displayProcessed}/${displayTotal}`
      : `${displayProcessed}/${displayTotal}`;

  const getStatusIcon = () => {
    if (isCompleted && status === 'running') {
      // Task is 100% complete but status still shows running
      return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
    }

    switch (status) {
      case 'running':
        return <SyncOutlined spin />;
      case 'completed':
        return <CheckCircleOutlined />;
      case 'failed':
      case 'stopped':
        return <CloseCircleOutlined />;
      case 'paused':
        return <PauseCircleOutlined />;
      default:
        return null;
    }
  };

  const getStatusColor = (): string => {
    if (isCompleted) {
      return '#52c41a'; // Green for completed
    }

    switch (status) {
      case 'running':
        return '#1890ff';
      case 'completed':
        return '#52c41a';
      case 'failed':
        return '#f5222d';
      case 'stopped':
        return '#d9d9d9';
      case 'paused':
        return '#faad14';
      default:
        return '#d9d9d9';
    }
  };

  const statusInfo = TASK_STATUS_MAP[status];

  return (
    <div>
      {showText && (
        <Space style={{ marginBottom: 8 }}>
          {getStatusIcon()}
          <Text>
            {statusInfo.text} · {displayProcessed} / {displayTotal}
            {displayTotal > 0 && !isInitializing && (
              <Text type="secondary"> ({percent}%)</Text>
            )}
          </Text>
          {failed > 0 && (
            <Text type="danger">
              · 失败 {failed} 个
            </Text>
          )}
        </Space>
      )}

      <Progress
        percent={isInitializing ? 0 : percent}
        status={isCompleted ? 'success' : status === 'failed' ? 'exception' : undefined}
        strokeColor={getStatusColor()}
      />

      {status === 'running' && !isInitializing && !isCompleted && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          正在处理中...
        </Text>
      )}

      {isInitializing && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          正在准备任务...
        </Text>
      )}
    </div>
  );
}
