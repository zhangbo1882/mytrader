import { useState, useEffect } from 'react';
import {
  Card,
  Typography,
  Divider,
  Table,
  Tag,
  Space,
  Button,
  Select,
  Popconfirm,
  message,
  Modal,
  InputNumber,
  Alert,
} from 'antd';
import {
  HistoryOutlined,
  DeleteOutlined,
  ClearOutlined,
  FilterOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import { TaskProgress } from '@/components/tasks/TaskProgress';
import type { Task, TaskStatus } from '@/types';
import { TASK_STATUS_MAP } from '@/utils';
import { taskService } from '@/services';

const { Title, Text } = Typography;
const { Option } = Select;

function TasksPage() {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(false);
  const [statusFilter, setStatusFilter] = useState<TaskStatus | 'all'>('all');
  const [cleanupDays, setCleanupDays] = useState<number>(30);
  const [cleanupModalVisible, setCleanupModalVisible] = useState(false);

  // 加载任务列表
  const loadTasks = async () => {
    setLoading(true);
    try {
      const result = await taskService.list();
      const taskList = result.tasks || result;
      setTasks(taskList);
    } catch (error) {
      message.error('加载任务列表失败');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadTasks();
  }, []);

  // 删除任务
  const handleDelete = async (id: string) => {
    try {
      await taskService.delete(id);
      message.success('任务已删除');
      loadTasks();
    } catch (error) {
      message.error('删除失败');
    }
  };

  // 清理旧任务
  const handleCleanup = async () => {
    try {
      const result = await taskService.cleanup(cleanupDays);
      message.success(`已清理 ${result.deleted} 个旧任务`);
      setCleanupModalVisible(false);
      loadTasks();
    } catch (error) {
      message.error('清理失败');
    }
  };

  // 获取任务类型标签
  const getTypeTag = (type: string) => {
    const typeMap: Record<string, { text: string; color: string }> = {
      update: { text: '数据更新', color: 'blue' },
      screen: { text: '股票筛选', color: 'green' },
      prediction: { text: 'AI预测', color: 'purple' },
    };
    const info = typeMap[type] || { text: type, color: 'default' };
    return <Tag color={info.color}>{info.text}</Tag>;
  };

  // 格式化时间
  const formatTime = (dateStr: string) => {
    return new Date(dateStr).toLocaleString('zh-CN');
  };

  // 计算耗时
  const getDuration = (task: Task) => {
    if (!task.startTime) return '-';
    const end = task.endTime ? new Date(task.endTime) : new Date();
    const start = new Date(task.startTime);
    const seconds = Math.floor((end.getTime() - start.getTime()) / 1000);
    if (seconds < 60) return `${seconds}秒`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}分${seconds % 60}秒`;
    return `${Math.floor(seconds / 3600)}时${Math.floor((seconds % 3600) / 60)}分`;
  };

  // 表格列定义
  const columns: ColumnsType<Task> = [
    {
      title: '任务ID',
      dataIndex: 'id',
      key: 'id',
      width: 80,
      ellipsis: true,
      render: (id) => (
        <Text style={{ fontSize: 12 }} copyable={{ text: id }}>
          {id.slice(-8)}
        </Text>
      ),
    },
    {
      title: '类型',
      dataIndex: 'type',
      key: 'type',
      width: 100,
      render: (type) => getTypeTag(type),
    },
    {
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      width: 100,
      render: (status: TaskStatus) => {
        const info = TASK_STATUS_MAP[status];
        return <Tag color={info.color}>{info.text}</Tag>;
      },
    },
    {
      title: '进度',
      key: 'progress',
      width: 200,
      render: (_, task) => (
        <TaskProgress
          status={task.status}
          processed={task.processed}
          total={task.total}
          failed={task.failed}
          showText={false}
        />
      ),
    },
    {
      title: '开始时间',
      dataIndex: 'startTime',
      key: 'startTime',
      width: 170,
      render: (time) => formatTime(time),
    },
    {
      title: '耗时',
      key: 'duration',
      width: 100,
      render: (_, task) => getDuration(task),
    },
    {
      title: '操作',
      key: 'action',
      width: 100,
      fixed: 'right',
      render: (_, task) => (
        <Popconfirm
          title="确认删除"
          description="确定要删除此任务记录吗？"
          onConfirm={() => handleDelete(task.id)}
          okText="确定"
          cancelText="取消"
        >
          <Button size="small" danger icon={<DeleteOutlined aria-hidden="true" />}>
            删除
          </Button>
        </Popconfirm>
      ),
    },
  ];

  // 过滤任务
  const filteredTasks = tasks.filter((task) => {
    if (statusFilter === 'all') return true;
    return task.status === statusFilter;
  });

  // 统计信息
  const stats = {
    total: tasks.length,
    running: tasks.filter((t) => t.status === 'running').length,
    completed: tasks.filter((t) => t.status === 'completed').length,
    failed: tasks.filter((t) => t.status === 'failed').length,
  };

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>
        <HistoryOutlined style={{ marginRight: 8 }} aria-hidden="true" />
        任务历史
      </Title>
      <Text type="secondary">查看和管理所有历史任务记录</Text>

      <Divider />

      {/* 统计信息 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space size="large">
          <div>
            <Text type="secondary">总任务数</Text>
            <div style={{ fontSize: 20, fontWeight: 'bold' }}>{stats.total}</div>
          </div>
          <div>
            <Text type="secondary">运行中</Text>
            <div style={{ fontSize: 20, fontWeight: 'bold', color: '#1890ff' }}>{stats.running}</div>
          </div>
          <div>
            <Text type="secondary">已完成</Text>
            <div style={{ fontSize: 20, fontWeight: 'bold', color: '#52c41a' }}>{stats.completed}</div>
          </div>
          <div>
            <Text type="secondary">失败</Text>
            <div style={{ fontSize: 20, fontWeight: 'bold', color: '#f5222d' }}>{stats.failed}</div>
          </div>
        </Space>
      </Card>

      {/* 操作栏 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          <Space>
            <FilterOutlined aria-hidden="true" />
            <Text>状态筛选：</Text>
            <Select
              value={statusFilter}
              onChange={setStatusFilter}
              style={{ width: 120 }}
            >
              <Option value="all">全部</Option>
              <Option value="running">运行中</Option>
              <Option value="completed">已完成</Option>
              <Option value="failed">失败</Option>
              <Option value="paused">已暂停</Option>
              <Option value="stopped">已停止</Option>
            </Select>
          </Space>

          <Button icon={<ReloadOutlined aria-hidden="true" />} onClick={loadTasks}>
            刷新
          </Button>

          <Button
            icon={<ClearOutlined aria-hidden="true" />}
            onClick={() => setCleanupModalVisible(true)}
          >
            清理旧任务
          </Button>
        </Space>

        {statusFilter !== 'all' && (
          <Alert
            message={`当前显示：${filteredTasks.length} 个任务`}
            type="info"
            style={{ marginTop: 12 }}
          />
        )}
      </Card>

      {/* 任务列表 */}
      <Table
        columns={columns}
        dataSource={filteredTasks}
        rowKey="id"
        loading={loading}
        scroll={{ x: 1200 }}
        pagination={{
          pageSize: 20,
          showSizeChanger: true,
          showTotal: (total) => `共 ${total} 条`,
        }}
        expandable={{
          expandedRowRender: (task) => (
            <div style={{ padding: 16 }}>
              <Space direction="vertical" style={{ width: '100%' }}>
                <div>
                  <Text strong>任务参数：</Text>
                  <pre style={{ marginTop: 8, background: '#f5f5f5', padding: 8 }}>
                    {JSON.stringify(task.params, null, 2)}
                  </pre>
                </div>
                {task.error && (
                  <div>
                    <Text strong type="danger">
                      错误信息：
                    </Text>
                    <Text type="danger">{task.error}</Text>
                  </div>
                )}
                {task.endTime && (
                  <Text type="secondary">
                    结束时间：{formatTime(task.endTime)}
                  </Text>
                )}
              </Space>
            </div>
          ),
          rowExpandable: (task) => !!task.params || !!task.error,
        }}
      />

      {/* 清理旧任务对话框 */}
      <Modal
        title="清理旧任务"
        open={cleanupModalVisible}
        onOk={handleCleanup}
        onCancel={() => setCleanupModalVisible(false)}
        okText="确定清理"
        cancelText="取消"
      >
        <Space direction="vertical" style={{ width: '100%' }}>
          <Alert
            message="提示"
            description="此操作将删除指定天数之前已完成或失败的任务记录，正在运行的任务不会被删除"
            type="warning"
            showIcon
          />
          <Space>
            <Text>删除</Text>
            <InputNumber
              min={1}
              max={365}
              value={cleanupDays}
              onChange={(value) => setCleanupDays(value || 30)}
              style={{ width: 100 }}
            />
            <Text>天之前的任务</Text>
          </Space>
        </Space>
      </Modal>
    </div>
  );
}

export default TasksPage;
