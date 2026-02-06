import { useEffect, useState, useMemo } from 'react';
import {
  Card,
  Typography,
  Divider,
  Space,
  Button,
  Alert,
  Empty,
  message,
  Tag,
  Modal,
  Form,
  Input,
  Radio,
  Checkbox,
  List,
  Popconfirm,
  Row,
  Col,
  Tooltip,
  Skeleton,
} from 'antd';
import {
  SyncOutlined,
  PlusOutlined,
  StarOutlined,
  ClockCircleOutlined,
  DeleteOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  InfoCircleOutlined,
  RocketOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import { TaskCard } from '@/components/tasks/TaskCard';
import { useTaskStore } from '@/stores';
import { useTaskPolling } from '@/hooks';
import { useNavigate } from 'react-router-dom';
import { taskService, scheduleService } from '@/services';
import { useFavoriteStore } from '@/stores';
import type { ScheduledJob } from '@/types';
import dayjs from 'dayjs';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;

// Task status color mapping
const getStatusColor = (status: string) => {
  const colors: Record<string, string> = {
    pending: 'warning',
    running: 'processing',
    paused: 'default',
    completed: 'success',
    failed: 'error',
    stopped: 'default',
  };
  return colors[status] || 'default';
};

// Human-readable cron expression parser
const getCronDescription = (cronExpr: string): string => {
  try {
    const parts = cronExpr.trim().split(/\s+/);
    if (parts.length !== 5) return cronExpr;

    const [minute, hour, day, month, dayOfWeek] = parts;

    // Day of week mapping
    const dowMap: Record<string, string> = {
      '0': '周日',
      '1': '周一',
      '2': '周二',
      '3': '周三',
      '4': '周四',
      '5': '周五',
      '6': '周六',
      '1-5': '周一至周五',
      '0-6': '每天',
      '*': '每天',
    };

    const dowDesc = dowMap[dayOfWeek] || dayOfWeek;

    if (day === '*' && month === '*') {
      if (hour === '*' && minute === '*') {
        return `每${dowDesc}`;
      }
      return `每${dowDesc} ${hour}:${minute.padStart(2, '0')}`;
    }

    return cronExpr;
  } catch {
    return cronExpr;
  }
};

// Task type to Chinese name mapping
const getTaskTypeName = (taskType: string): string => {
  const taskTypeMap: Record<string, string> = {
    'update_stock_prices': '股价更新',
    'update_financial_reports': '财务报表',
    'update_industry_classification': '行业分类',
    'update_index_data': '指数数据',
    'update_industry_statistics': '行业统计',
  };
  return taskTypeMap[taskType] || taskType;
};

function UpdatePage() {
  const navigate = useNavigate();
  const { tasks, runningTask, setTasks } = useTaskStore();
  const { favorites } = useFavoriteStore();
  const { poll } = useTaskPolling();

  const [taskModalVisible, setTaskModalVisible] = useState(false);
  const [scheduleModalVisible, setScheduleModalVisible] = useState(false);
  const [scheduledJobs, setScheduledJobs] = useState<ScheduledJob[]>([]);
  const [loading, setLoading] = useState(false);
  const [jobsLoading, setJobsLoading] = useState(true);
  const [form] = Form.useForm();
  const [scheduleForm] = Form.useForm();

  // 加载任务列表
  const loadTasks = async () => {
    try {
      const result = await taskService.list();
      const taskList = result.tasks || result;
      setTasks(taskList);
    } catch (error) {
      console.error('Failed to load tasks:', error);
      message.error({
        content: '加载任务失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    }
  };

  const loadScheduledJobs = async () => {
    try {
      setJobsLoading(true);
      const result = await scheduleService.list();
      setScheduledJobs(result.jobs || []);
    } catch (error) {
      console.error('Failed to load scheduled jobs:', error);
      message.error({
        content: '加载定时任务失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    } finally {
      setJobsLoading(false);
    }
  };

  // 加载任务列表
  useEffect(() => {
    loadTasks();
    loadScheduledJobs();
  }, []);

  // 定期轮询任务状态
  useEffect(() => {
    const interval = setInterval(() => {
      if (runningTask) {
        poll();
      }
    }, 10000);

    return () => clearInterval(interval);
  }, [runningTask, poll]);

  // 键盘快捷键
  useEffect(() => {
    const handleKeyPress = (e: KeyboardEvent) => {
      // Cmd/Ctrl + N: 创建新任务
      if ((e.metaKey || e.ctrlKey) && e.key === 'n') {
        e.preventDefault();
        setTaskModalVisible(true);
      }
      // Cmd/Ctrl + S: 创建定时任务
      if ((e.metaKey || e.ctrlKey) && e.key === 's') {
        e.preventDefault();
        setScheduleModalVisible(true);
      }
      // Cmd/Ctrl + R: 手动刷新
      if ((e.metaKey || e.ctrlKey) && e.key === 'r') {
        e.preventDefault();
        loadTasks();
        loadScheduledJobs();
        message.success('已刷新');
      }
    };

    window.addEventListener('keydown', handleKeyPress);
    return () => window.removeEventListener('keydown', handleKeyPress);
  }, []);

  // 创建自定义任务
  const handleCreateCustomTask = async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);

      const contentType = values.content_type || 'stock';
      const scope = values.scope;

      // Prepare request based on content type
      if (contentType === 'industry') {
        await taskService.create({
          type: 'update',
          params: {
            content_type: 'industry',
            src: values.industry_src || 'SW2021',
            force: values.industry_force || false,
          },
        });
      } else if (contentType === 'statistics') {
        await taskService.create({
          type: 'update',
          params: {
            content_type: 'statistics',
            metrics: values.metrics || ['pe_ttm', 'pb', 'ps_ttm', 'total_mv', 'circ_mv'],
          },
        });
      } else if (contentType === 'index') {
        await taskService.create({
          type: 'update',
          params: {
            content_type: 'index',
            markets: values.markets || ['SSE', 'SZSE'],
          },
        });
      } else if (contentType === 'financial') {
        let stockList: string[] = [];
        if (scope === 'custom' && values.custom_stocks) {
          stockList = values.custom_stocks.split('\n').map((s: string) => s.trim()).filter(Boolean);
        } else if (scope === 'favorites') {
          stockList = favorites.map((f) => f.code);
        }

        await taskService.create({
          type: 'update',
          params: {
            content_type: 'financial',
            stock_range: scope,
            custom_stocks: stockList,
            include_indicators: true,
          },
        });
      } else {
        let stockList: string[] = [];
        if (scope === 'custom' && values.custom_stocks) {
          stockList = values.custom_stocks.split('\n').map((s: string) => s.trim()).filter(Boolean);
        } else if (scope === 'favorites') {
          stockList = favorites.map((f) => f.code);
        }

        await taskService.create({
          type: 'update',
          params: {
            content_type: 'stock',
            mode: values.mode || 'incremental',
            stock_range: scope,
            custom_stocks: stockList,
          },
        });
      }

      message.success({
        content: '任务创建成功',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      setTaskModalVisible(false);
      form.resetFields();
      loadTasks();
    } catch (error) {
      if (error instanceof Error) {
        message.error({
          content: `创建失败：${error.message}`,
          icon: <CloseCircleOutlined />,
          duration: 5,
        });
      }
    } finally {
      setLoading(false);
    }
  };

  // 创建定时任务
  const handleCreateScheduledJob = async () => {
    try {
      const values = await scheduleForm.validateFields();
      setLoading(true);

      const taskTypeMap: Record<string, string> = {
        'stock': 'update_stock_prices',
        'index': 'update_index_data',
        'industry': 'update_industry_classification',
        'financial': 'update_financial_reports',
        'statistics': 'update_industry_statistics',
      };

      const task_type = taskTypeMap[values.content_type || 'stock'] || 'update_stock_prices';

      const params: any = {
        name: values.name,
        task_type: task_type,
        trigger: {
          cron_expression: values.cron_expression,
        },
        content_type: values.content_type || 'stock',
        mode: values.mode || 'incremental',
      };

      if (values.content_type === 'stock') {
        params.stock_range = values.stock_range || 'all';
      } else if (values.content_type === 'index') {
        params.markets = values.markets || ['SSE', 'SZSE'];
      }

      await scheduleService.create(params);
      message.success({
        content: '定时任务创建成功',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      setScheduleModalVisible(false);
      scheduleForm.resetFields();
      loadScheduledJobs();
    } catch (error) {
      if (error instanceof Error) {
        message.error({
          content: `创建失败：${error.message}`,
          icon: <CloseCircleOutlined />,
          duration: 5,
        });
      }
    } finally {
      setLoading(false);
    }
  };

  // 删除定时任务
  const handleDeleteScheduledJob = async (jobId: string) => {
    try {
      await scheduleService.delete(jobId);
      message.success({
        content: '定时任务已删除',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      loadScheduledJobs();
    } catch (error) {
      message.error({
        content: '删除失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    }
  };

  // 暂停定时任务
  const handlePauseScheduledJob = async (jobId: string) => {
    try {
      await scheduleService.pause(jobId);
      message.success({
        content: '定时任务已暂停',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      loadScheduledJobs();
    } catch (error) {
      message.error({
        content: '暂停失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    }
  };

  // 恢复定时任务
  const handleResumeScheduledJob = async (jobId: string) => {
    try {
      await scheduleService.resume(jobId);
      message.success({
        content: '定时任务已恢复',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      loadScheduledJobs();
    } catch (error) {
      message.error({
        content: '恢复失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    }
  };

  // 暂停任务
  const handlePauseTask = async (taskId: string) => {
    try {
      await taskService.pause(taskId);
      message.success({
        content: '任务暂停中...',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      await loadTasks();
    } catch (error) {
      message.error({
        content: '暂停任务失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    }
  };

  // 恢复任务
  const handleResumeTask = async (taskId: string) => {
    try {
      await taskService.resume(taskId);
      message.success({
        content: '任务已恢复',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      await loadTasks();
    } catch (error) {
      message.error({
        content: '恢复任务失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    }
  };

  // 停止任务
  const handleStopTask = async (taskId: string) => {
    try {
      await taskService.stop(taskId);
      message.success({
        content: '任务停止中...',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      await loadTasks();
    } catch (error) {
      message.error({
        content: '停止任务失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    }
  };

  // 删除任务
  const handleDeleteTask = async (taskId: string) => {
    try {
      await taskService.delete(taskId);
      message.success({
        content: '任务已删除',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      await loadTasks();
    } catch (error) {
      message.error({
        content: '删除任务失败',
        icon: <CloseCircleOutlined />,
        duration: 5,
      });
    }
  };

  // 正在运行的任务
  const activeTasks = tasks.filter((t) => ['running', 'paused', 'pending'].includes(t.status));

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>
        <SyncOutlined style={{ marginRight: 8 }} aria-hidden="true" />
        更新管理
      </Title>
      <Text type="secondary">创建和管理数据更新任务，实时查看进度</Text>

      <Divider />

      <Row gutter={[16, 16]}>
        {/* 左侧：操作面板 */}
        <Col xs={24} md={8}>
          <Card title="创建更新任务" size="small">
            <Space direction="vertical" style={{ width: '100%' }} size="middle">
              <Button
                type="primary"
                icon={<PlusOutlined aria-hidden="true" />}
                onClick={() => setTaskModalVisible(true)}
                block
                aria-label="创建更新任务"
                style={{ height: 44 }}
              >
                创建更新任务
              </Button>
              <Button
                icon={<ClockCircleOutlined aria-hidden="true" />}
                onClick={() => setScheduleModalVisible(true)}
                block
                aria-label="创建定时任务"
                style={{ height: 44 }}
              >
                创建定时任务
              </Button>
              <Button
                icon={<RocketOutlined aria-hidden="true" />}
                onClick={() => navigate('/tasks')}
                block
                aria-label="查看任务历史"
                style={{ height: 44 }}
              >
                查看任务历史
              </Button>
              <Tooltip title="快捷键: ⌘R / Ctrl+R">
                <Button
                  icon={<ReloadOutlined aria-hidden="true" />}
                  onClick={() => {
                    loadTasks();
                    loadScheduledJobs();
                    message.success('已刷新');
                  }}
                  block
                  style={{ height: 44 }}
                >
                  刷新
                </Button>
              </Tooltip>
            </Space>
          </Card>
        </Col>

        {/* 右侧：任务状态和定时任务 */}
        <Col xs={24} md={16}>
          {/* 运行中的任务 */}
          <Card
            title={
              <Space>
                <RocketOutlined aria-hidden="true" />
                <span>运行中的任务</span>
                <Tag color="processing">{activeTasks.length} 个</Tag>
              </Space>
            }
            size="small"
            style={{ marginBottom: 16 }}
            extra={
              <Tooltip title="快捷键: ⌘N / Ctrl+N">
                <Button
                  type="primary"
                  size="small"
                  icon={<PlusOutlined aria-hidden="true" />}
                  onClick={() => setTaskModalVisible(true)}
                  aria-label="快速创建任务"
                  style={{ height: 36, minWidth: 36 }}
                />
              </Tooltip>
            }
          >
            {activeTasks.length === 0 ? (
              <Empty description="暂无运行中的任务" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <Space direction="vertical" style={{ width: '100%' }} size="middle">
                {activeTasks.map((task) => (
                  <TaskCard
                    key={task.id}
                    task={task}
                    onResume={() => handleResumeTask(task.id)}
                    onPause={() => handlePauseTask(task.id)}
                    onStop={() => handleStopTask(task.id)}
                    onDelete={() => handleDeleteTask(task.id)}
                  />
                ))}
              </Space>
            )}
          </Card>

          {/* 定时任务 */}
          <Card
            title={
              <Space>
                <ClockCircleOutlined aria-hidden="true" />
                <span>定时任务</span>
                <Tag color="blue">{scheduledJobs.length} 个</Tag>
              </Space>
            }
            size="small"
            extra={
              <Tooltip title="快捷键: ⌘S / Ctrl+S">
                <Button
                  type="primary"
                  size="small"
                  icon={<PlusOutlined aria-hidden="true" />}
                  onClick={() => setScheduleModalVisible(true)}
                  aria-label="快速创建定时任务"
                  style={{ height: 36, minWidth: 36 }}
                />
              </Tooltip>
            }
          >
            {jobsLoading ? (
              <Skeleton active paragraph={{ rows: 3 }} />
            ) : scheduledJobs.length === 0 ? (
              <Empty description="暂无定时任务" image={Empty.PRESENTED_IMAGE_SIMPLE} />
            ) : (
              <List
                dataSource={scheduledJobs}
                renderItem={(job) => (
                  <List.Item
                    actions={[
                      job.enabled ? (
                        <Tooltip title="暂停任务">
                          <Button
                            type="text"
                            size="large"
                            icon={<PauseCircleOutlined aria-hidden="true" />}
                            onClick={() => handlePauseScheduledJob(job.id)}
                            aria-label="暂停定时任务"
                            style={{ height: 44, minWidth: 44 }}
                          >
                            暂停
                          </Button>
                        </Tooltip>
                      ) : (
                        <Tooltip title="恢复任务">
                          <Button
                            type="text"
                            size="large"
                            icon={<PlayCircleOutlined aria-hidden="true" />}
                            onClick={() => handleResumeScheduledJob(job.id)}
                            aria-label="恢复定时任务"
                            style={{ height: 44, minWidth: 44 }}
                          >
                            恢复
                          </Button>
                        </Tooltip>
                      ),
                      <Popconfirm
                        title="确定要删除这个定时任务吗？"
                        onConfirm={() => handleDeleteScheduledJob(job.id)}
                        okText="确定"
                        cancelText="取消"
                      >
                        <Tooltip title="删除任务">
                          <Button
                            type="text"
                            danger
                            size="large"
                            icon={<DeleteOutlined aria-hidden="true" />}
                            aria-label="删除定时任务"
                            style={{ height: 44, minWidth: 44 }}
                          >
                            删除
                          </Button>
                        </Tooltip>
                      </Popconfirm>,
                    ]}
                  >
                    <List.Item.Meta
                      title={
                        <Space>
                          <Text
                            strong
                            style={{
                              maxWidth: 300,
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                              whiteSpace: 'nowrap',
                            }}
                          >
                            {job.name}
                          </Text>
                          <Tag color={job.enabled ? 'green' : 'default'}>{job.enabled ? '启用' : '禁用'}</Tag>
                          {job.task_type && (
                            <Tag color="purple">{getTaskTypeName(job.task_type)}</Tag>
                          )}
                        </Space>
                      }
                      description={
                        <Space direction="vertical" size="small">
                          <Tooltip title={job.trigger || 'N/A'}>
                            <Text
                              type="secondary"
                              style={{
                                maxWidth: 400,
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                display: 'block',
                              }}
                            >
                              <ClockCircleOutlined style={{ marginRight: 4 }} aria-hidden="true" />
                              {getCronDescription(job.trigger || '')}
                            </Text>
                          </Tooltip>
                          {job.next_run_time && (
                            <Text type="secondary">
                              下次运行: {dayjs(job.next_run_time).format('YYYY-MM-DD HH:mm:ss')}
                            </Text>
                          )}
                        </Space>
                      }
                    />
                  </List.Item>
                )}
              />
            )}
          </Card>
        </Col>
      </Row>

      {/* 创建更新任务弹窗 */}
      <Modal
        title="创建更新任务"
        open={taskModalVisible}
        onOk={handleCreateCustomTask}
        onCancel={() => {
          setTaskModalVisible(false);
          form.resetFields();
        }}
        confirmLoading={loading}
        width={600}
      >
        <Form form={form} layout="vertical" initialValues={{ content_type: 'stock', mode: 'incremental', scope: 'all' }}>
          <Form.Item
            label="更新内容"
            name="content_type"
            rules={[{ required: true, message: '请选择更新内容' }]}
            extra="选择要更新的数据类型"
          >
            <Radio.Group>
              <Radio value="stock">股价数据</Radio>
              <Radio value="financial">财务数据</Radio>
              <Radio value="index">指数数据</Radio>
              <Radio value="industry">申万行业分类</Radio>
              <Radio value="statistics">行业统计</Radio>
            </Radio.Group>
          </Form.Item>

          <Form.Item noStyle shouldUpdate={(prev, curr) => prev.content_type !== curr.content_type}>
            {({ getFieldValue }) =>
              getFieldValue('content_type') !== 'financial' ? (
                <Form.Item
                  label="更新模式"
                  name="mode"
                  rules={[{ required: true, message: '请选择更新模式' }]}
                  extra={
                    <Tooltip title="增量更新更快，适合日常使用；全量更新较慢，适合首次或补全数据">
                      <InfoCircleOutlined style={{ color: '#1890ff', cursor: 'help' }} aria-hidden="true" />
                    </Tooltip>
                  }
                >
                  <Radio.Group>
                    <Radio value="incremental">增量更新（快速，日常使用）</Radio>
                    <Radio value="full">全量更新（较慢，首次或补全数据）</Radio>
                  </Radio.Group>
                </Form.Item>
              ) : null
            }
          </Form.Item>

          <Form.Item noStyle shouldUpdate={(prev, curr) => prev.content_type !== curr.content_type}>
            {({ getFieldValue }) =>
              getFieldValue('content_type') === 'index' ? (
                <Form.Item
                  label="市场选择"
                  name="markets"
                  rules={[{ required: true, message: '请选择市场' }]}
                  initialValue={['SSE', 'SZSE']}
                  extra="选择要更新的交易所市场"
                >
                  <Checkbox.Group>
                    <Checkbox value="SSE">上海证券交易所 (SSE)</Checkbox>
                    <Checkbox value="SZSE">深圳证券交易所 (SZSE)</Checkbox>
                  </Checkbox.Group>
                </Form.Item>
              ) : getFieldValue('content_type') === 'industry' ? (
                <>
                  <Form.Item
                    label="行业分类版本"
                    name="industry_src"
                    initialValue="SW2021"
                    extra="选择行业分类标准"
                  >
                    <Radio.Group>
                      <Radio value="SW2021">申万2021行业分类</Radio>
                      <Radio value="SW2014">申万2014行业分类</Radio>
                    </Radio.Group>
                  </Form.Item>
                  <Form.Item
                    name="industry_force"
                    valuePropName="checked"
                    initialValue={false}
                  >
                    <Checkbox>强制重新获取（忽略已有数据）</Checkbox>
                  </Form.Item>
                </>
              ) : getFieldValue('content_type') === 'statistics' ? (
                <Form.Item
                  label="更新指标"
                  name="metrics"
                  initialValue={['pe_ttm', 'pb', 'ps_ttm', 'total_mv', 'circ_mv']}
                  extra="选择要更新的行业统计指标"
                  rules={[{ required: true, message: '请至少选择一个指标' }]}
                >
                  <Checkbox.Group style={{ width: '100%' }}>
                    <Space direction="vertical">
                      <Checkbox value="pe_ttm">市盈率TTM (pe_ttm)</Checkbox>
                      <Checkbox value="pb">市净率 (pb)</Checkbox>
                      <Checkbox value="ps_ttm">市销率TTM (ps_ttm)</Checkbox>
                      <Checkbox value="total_mv">总市值 (total_mv)</Checkbox>
                      <Checkbox value="circ_mv">流通市值 (circ_mv)</Checkbox>
                    </Space>
                  </Checkbox.Group>
                </Form.Item>
              ) : (
                <Form.Item
                  label="股票范围"
                  name="scope"
                  rules={[{ required: true, message: '请选择股票范围' }]}
                  extra="选择要更新的股票范围"
                >
                  <Radio.Group>
                    <Radio value="all">全部A股</Radio>
                    <Radio value="favorites">收藏列表</Radio>
                    <Radio value="custom">自定义</Radio>
                  </Radio.Group>
                </Form.Item>
              )
            }
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prev, curr) => prev.scope !== curr.scope || prev.content_type !== curr.content_type}
          >
            {({ getFieldValue }) =>
              getFieldValue('content_type') !== 'index' &&
              getFieldValue('content_type') !== 'industry' &&
              getFieldValue('scope') === 'custom' ? (
                <Form.Item
                  label="自定义股票代码"
                  name="custom_stocks"
                  rules={[{ required: true, message: '请输入股票代码' }]}
                  extra="每行一个股票代码，如：000001、600000、600519"
                >
                  <TextArea
                    placeholder="000001&#10;600000&#10;600519"
                    rows={5}
                    spellCheck={false}
                    aria-label="股票代码列表"
                  />
                </Form.Item>
              ) : null
            }
          </Form.Item>
        </Form>
      </Modal>

      {/* 创建定时任务弹窗 */}
      <Modal
        title="创建定时任务"
        open={scheduleModalVisible}
        onOk={handleCreateScheduledJob}
        onCancel={() => {
          setScheduleModalVisible(false);
          scheduleForm.resetFields();
        }}
        confirmLoading={loading}
        width={600}
      >
        <Form form={scheduleForm} layout="vertical" initialValues={{ content_type: 'stock', mode: 'incremental', stock_range: 'all' }}>
          <Form.Item
            label="任务名称"
            name="name"
            rules={[{ required: true, message: '请输入任务名称' }]}
            extra="如：工作日收盘更新"
          >
            <Input placeholder="输入任务名称" autoComplete="off" aria-label="任务名称" />
          </Form.Item>

          <Form.Item
            label={
              <Space>
                Cron表达式
                <Tooltip title="格式: 分 时 日 月 周&#10;示例: 0 18 * * 1-5 = 工作日18点&#10;0 0 * * * = 每天00:00">
                  <InfoCircleOutlined style={{ color: '#1890ff' }} aria-hidden="true" />
                </Tooltip>
              </Space>
            }
            name="cron_expression"
            rules={[{ required: true, message: '请输入Cron表达式' }]}
            extra="使用快捷选择或输入自定义表达式"
          >
            <Input
              placeholder="0 18 * * 1-5"
              autoComplete="off"
              spellCheck={false}
              aria-label="Cron表达式"
            />
          </Form.Item>

          <Paragraph>
            <Text strong>快捷选择：</Text>
            <Space wrap style={{ marginTop: 8 }}>
              <Tooltip title="工作日下午6点">
                <Button size="small" onClick={() => scheduleForm.setFieldsValue({ cron_expression: '0 18 * * 1-5' })}>
                  工作日18:00
                </Button>
              </Tooltip>
              <Tooltip title="工作日上午9点">
                <Button size="small" onClick={() => scheduleForm.setFieldsValue({ cron_expression: '0 9 * * 1-5' })}>
                  工作日09:00
                </Button>
              </Tooltip>
              <Tooltip title="每6小时一次">
                <Button size="small" onClick={() => scheduleForm.setFieldsValue({ cron_expression: '0 */6 * * *' })}>
                  每6小时
                </Button>
              </Tooltip>
              <Tooltip title="每天午夜">
                <Button size="small" onClick={() => scheduleForm.setFieldsValue({ cron_expression: '0 0 * * *' })}>
                  每天00:00
                </Button>
              </Tooltip>
            </Space>
          </Paragraph>

          <Form.Item
            label="更新内容"
            name="content_type"
            rules={[{ required: true, message: '请选择更新内容' }]}
            extra="选择定时任务要更新的数据类型"
          >
            <Radio.Group>
              <Radio value="stock">股价数据</Radio>
              <Radio value="index">指数数据</Radio>
              <Radio value="statistics">行业统计</Radio>
            </Radio.Group>
          </Form.Item>

          <Form.Item noStyle shouldUpdate={(prev, curr) => prev.content_type !== curr.content_type}>
            {({ getFieldValue }) =>
              getFieldValue('content_type') === 'stock' ? (
                <>
                  <Form.Item
                    label="更新模式"
                    name="mode"
                    rules={[{ required: true, message: '请选择更新模式' }]}
                    extra={
                      <Tooltip title="增量更新更快，适合日常使用">
                        <InfoCircleOutlined style={{ color: '#1890ff', cursor: 'help' }} aria-hidden="true" />
                      </Tooltip>
                    }
                  >
                    <Radio.Group>
                      <Radio value="incremental">增量更新（推荐）</Radio>
                      <Radio value="full">全量更新</Radio>
                    </Radio.Group>
                  </Form.Item>

                  <Form.Item
                    label="股票范围"
                    name="stock_range"
                    rules={[{ required: true, message: '请选择股票范围' }]}
                    extra="选择要更新的股票范围"
                  >
                    <Radio.Group>
                      <Radio value="all">全部A股</Radio>
                      <Radio value="favorites">收藏列表</Radio>
                    </Radio.Group>
                  </Form.Item>
                </>
              ) : (
                <Form.Item
                  label="市场选择"
                  name="markets"
                  rules={[{ required: true, message: '请选择市场' }]}
                  initialValue={['SSE', 'SZSE']}
                  extra="选择要更新的交易所市场"
                >
                  <Checkbox.Group>
                    <Checkbox value="SSE">上海证券交易所 (SSE)</Checkbox>
                    <Checkbox value="SZSE">深圳证券交易所 (SZSE)</Checkbox>
                  </Checkbox.Group>
                </Form.Item>
              )
            }
          </Form.Item>
        </Form>
      </Modal>

      {/* 轮询提示 */}
      {runningTask && (
        <Alert
          message={
            <Space>
              <SyncOutlined spin aria-hidden="true" />
              <span>实时监控中…</span>
            </Space>
          }
          description={
            <Space>
              <span>系统正在自动更新任务进度，每10秒刷新一次…</span>
              <Button
                type="link"
                size="small"
                icon={<ReloadOutlined aria-hidden="true" />}
                onClick={() => {
                  poll();
                  message.success('已刷新');
                }}
              >
                立即刷新
              </Button>
            </Space>
          }
          type="info"
          showIcon={false}
          style={{ marginTop: 16 }}
        />
      )}
    </div>
  );
}

export default UpdatePage;
