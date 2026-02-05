import { memo } from 'react';
import { Card, Space, Button, Tag, Typography, Popconfirm, Tooltip } from 'antd';
import {
  PlayCircleOutlined,
  PauseCircleOutlined,
  StopOutlined,
  DeleteOutlined,
  ClockCircleOutlined,
  SyncOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
} from '@ant-design/icons';
import { TaskProgress } from './TaskProgress';
import type { Task, TaskStatus } from '@/types';
import { TASK_STATUS_MAP } from '@/utils';

const { Text, Paragraph } = Typography;

interface TaskCardProps {
  task: Task;
  onResume?: () => void;
  onPause?: () => void;
  onStop?: () => void;
  onDelete?: () => void;
  showActions?: boolean;
}

export const TaskCard = memo(function TaskCard({ task, onResume, onPause, onStop, onDelete, showActions = true }: TaskCardProps) {
  const statusInfo = TASK_STATUS_MAP[task.status];
  const isRunning = task.status === 'running';
  const isPaused = task.status === 'paused';
  const isPending = task.status === 'pending';
  const isCompleted = task.status === 'completed';
  const isFailed = task.status === 'failed';

  const getStatusIcon = () => {
    switch (task.status) {
      case 'pending':
        return <ClockCircleOutlined aria-hidden="true" />;
      case 'running':
        return <SyncOutlined spin aria-hidden="true" />;
      case 'paused':
        return <PauseCircleOutlined aria-hidden="true" />;
      case 'completed':
        return <CheckCircleOutlined aria-hidden="true" />;
      case 'failed':
        return <CloseCircleOutlined aria-hidden="true" />;
      case 'stopped':
        return <CloseCircleOutlined aria-hidden="true" />;
      default:
        return null;
    }
  };

  const getTaskTypeLabel = () => {
    switch (task.type) {
      case 'update':
        return '数据更新';
      case 'screen':
        return '股票筛选';
      case 'prediction':
        return 'AI预测';
      default:
        return task.type;
    }
  };

  const formatDuration = () => {
    if (!task.startTime) return '-';

    try {
      const start = new Date(task.startTime);
      // Check if date is valid
      if (isNaN(start.getTime())) return '-';

      const end = task.endTime ? new Date(task.endTime) : new Date();
      const duration = Math.floor((end.getTime() - start.getTime()) / 1000);

      if (duration < 0) return '0秒';
      if (duration < 60) return `${duration}秒`;
      if (duration < 3600) return `${Math.floor(duration / 60)}分${duration % 60}秒`;
      return `${Math.floor(duration / 3600)}时${Math.floor((duration % 3600) / 60)}分`;
    } catch {
      return '-';
    }
  };

  const formatStartTime = () => {
    if (!task.startTime) return '-';

    try {
      const date = new Date(task.startTime);
      if (isNaN(date.getTime())) return '-';
      return date.toLocaleString('zh-CN');
    } catch {
      return '-';
    }
  };

  return (
    <Card
      size="small"
      title={
        <Space>
          {getStatusIcon()}
          <span>{getTaskTypeLabel()}</span>
          <Tag color={statusInfo.color}>{statusInfo.text}</Tag>
        </Space>
      }
      extra={
        showActions && (
          <Space>
            {isPaused && (
              <Tooltip title="恢复任务">
                <Button
                  type="text"
                  icon={<PlayCircleOutlined />}
                  onClick={onResume}
                  aria-label="恢复任务"
                  style={{ color: '#52c41a' }}
                />
              </Tooltip>
            )}
            {isRunning && (
              <Tooltip title="暂停任务">
                <Button
                  type="text"
                  icon={<PauseCircleOutlined />}
                  onClick={onPause}
                  aria-label="暂停任务"
                  style={{ color: '#faad14' }}
                />
              </Tooltip>
            )}
            {(isRunning || isPaused || isPending) && (
              <Tooltip title="停止任务">
                <Popconfirm
                  title="确认停止"
                  description="确定要停止当前任务吗？"
                  onConfirm={onStop}
                  okText="确定"
                  cancelText="取消"
                >
                  <Button
                    type="text"
                    icon={<StopOutlined />}
                    aria-label="停止任务"
                    style={{ color: '#f5222d' }}
                  />
                </Popconfirm>
              </Tooltip>
            )}
            {(isCompleted || isFailed || task.status === 'stopped') && (
              <Tooltip title="删除任务">
                <Popconfirm
                  title="确认删除"
                  description="确定要删除此任务记录吗？"
                  onConfirm={onDelete}
                  okText="确定"
                  cancelText="取消"
                >
                  <Button
                    type="text"
                    icon={<DeleteOutlined />}
                    aria-label="删除任务"
                    style={{ color: '#999' }}
                  />
                </Popconfirm>
              </Tooltip>
            )}
          </Space>
        )
      }
    >
      <Space direction="vertical" style={{ width: '100%' }} size="small">
        {/* 任务参数 */}
        {task.params && Object.keys(task.params).length > 0 && (
          <Paragraph ellipsis={{ rows: 1, tooltip: true }} style={{ margin: 0, fontSize: 12 }}>
            <Text type="secondary">参数：</Text>
            <Text>{JSON.stringify(task.params)}</Text>
          </Paragraph>
        )}

        {/* 进度条 */}
        <TaskProgress status={task.status} processed={task.processed} total={task.total} failed={task.failed} />

        {/* 统计信息 */}
        <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
          <Space split={<span style={{ color: '#d9d9d9' }}>|</span>}>
            <Text type="secondary">开始时间：{formatStartTime()}</Text>
            <Text type="secondary">耗时：{formatDuration()}</Text>
          </Space>
          {task.error && (
            <Text type="danger" ellipsis={{ tooltip: task.error }}>
              错误：{task.error}
            </Text>
          )}
        </div>
      </Space>
    </Card>
  );
});
