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
  EditOutlined,
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

// Compute start date from date range preset (e.g. '2y' → 2 years ago)
const computeStartDate = (dateRange: string, customStartDate?: string): string | undefined => {
  if (!dateRange || dateRange === 'incremental' || dateRange === 'custom') return customStartDate;
  const match = dateRange.match(/^(\d+)y$/);
  if (match) return dayjs().subtract(parseInt(match[1]), 'year').format('YYYY-MM-DD');
  return undefined;
};

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
    'update_a_share_batch': 'A股批量更新',
    'update_hk_batch': '港股批量更新',
    'update_hk_prices': '港股数据更新',
    'update_financial_reports': '财务报表',
    'update_industry_classification': '行业分类',
    'update_index_data': '指数数据',
    'update_industry_statistics': '行业统计',
    'update_moneyflow': '资金流向',
    'update_dragon_list': '龙虎榜',
    'backtest': '策略回测',
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
  const [editingJob, setEditingJob] = useState<ScheduledJob | null>(null);
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
      } else if (contentType === 'moneyflow') {
        let stockList: string[] = [];
        if (scope === 'custom' && values.custom_stocks) {
          stockList = values.custom_stocks.split('\n').map((s: string) => s.trim()).filter(Boolean);
        } else if (scope === 'favorites') {
          stockList = favorites.map((f) => f.code);
        }

        const mode = values.mode || 'incremental';
        const startDate = mode === 'full' ? computeStartDate(values.date_range, values.start_date) : undefined;

        // 创建资金流向数据更新任务
        // 全市场更新时，任务完成后会自动触发行业汇总计算
        await taskService.create({
          type: 'update',
          params: {
            content_type: 'moneyflow',
            mode,
            stock_range: scope,
            custom_stocks: stockList,
            exclude_st: true,
            start_date: startDate,
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
      } else if (contentType === 'dragon_list') {
        // 龙虎榜更新任务
        const startDate = computeStartDate(values.date_range, values.start_date);
        const endDate = values.end_date;
        // 如果有开始日期，则是批量模式，否则是增量模式
        const mode = startDate ? 'batch' : 'incremental';

        await taskService.create({
          type: 'update_dragon_list',
          params: {
            mode,
            start_date: startDate,
            end_date: endDate,
          },
        });
      } else if (contentType === 'hk') {
        // HK stock update logic
        let stockList: string[] = [];
        if (scope === 'custom' && values.custom_stocks) {
          stockList = values.custom_stocks.split('\n')
            .map((s: string) => s.trim())
            .filter(Boolean);
        } else if (scope === 'favorites') {
          // Filter favorites for HK stocks (ending with .HK or 4-5 digit codes)
          stockList = favorites
            .filter(f => f.code.endsWith('.HK') || /^\d{4,5}$/.test(f.code))
            .map(f => f.code.replace('.HK', ''));
        }

        const mode = values.mode || 'incremental';
        const startDate = mode === 'full' ? computeStartDate(values.date_range, values.start_date) : undefined;

        await taskService.create({
          type: 'update_hk_prices',
          params: {
            stock_range: scope,
            custom_stocks: stockList,
            mode,
            start_date: startDate,
          },
        });
      } else {
        let stockList: string[] = [];
        if (scope === 'custom' && values.custom_stocks) {
          stockList = values.custom_stocks.split('\n').map((s: string) => s.trim()).filter(Boolean);
        } else if (scope === 'favorites') {
          stockList = favorites.map((f) => f.code);
        }

        const mode = values.mode || 'incremental';
        const startDate = mode === 'full' ? computeStartDate(values.date_range, values.start_date) : undefined;

        // 增量更新且全市场/按市场选择时，使用高效的批量获取方式
        if (mode === 'incremental' && (scope === 'all' || scope === 'market')) {
          // 使用批量更新任务（高效模式：按日期获取所有股票）
          await taskService.create({
            type: 'update_a_share_batch',
            params: {
              days_back: 1,  // 增量更新只获取最近1天
            },
          });
        } else {
          // A股数据更新参数（逐股票模式）
          const params: any = {
            content_type: 'stock',
            mode,
            stock_range: scope,
            custom_stocks: stockList,
            start_date: startDate,
            exclude_st: values.exclude_st !== false, // 默认排除ST
          };

          // 如果按市场选择，添加市场参数
          if (scope === 'market') {
            params.markets = values.markets || ['main'];
          }

          await taskService.create({
            type: 'update',
            params,
          });
        }
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
        'hk': 'update_hk_prices',
        'index': 'update_index_data',
        'industry': 'update_industry_classification',
        'financial': 'update_financial_reports',
        'statistics': 'update_industry_statistics',
        'moneyflow': 'update_moneyflow',
        'dragon_list': 'update_dragon_list',
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

      // 根据任务类型设置不同参数
      if (values.content_type === 'stock' || values.content_type === 'moneyflow') {
        params.stock_range = values.stock_range || 'all';
        // 添加市场参数（按市场选择时）
        if (values.stock_range === 'market') {
          params.markets = values.schedule_markets || ['main'];
        }
        // 添加排除ST参数
        params.exclude_st = values.schedule_exclude_st !== false;
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

  // 编辑定时任务
  const handleEditScheduledJob = (job: ScheduledJob) => {
    setEditingJob(job);

    // 解析 cron 表达式
    let cronExpression = '';
    const triggerMatch = job.trigger?.match(/cron\[(.*)\]/);
    if (triggerMatch) {
      // 从 trigger 字符串解析 cron 表达式
      const parts: Record<string, string> = {};
      const matches = triggerMatch[1].matchAll(/(\w+)='([^']*)'/g);
      for (const match of matches) {
        parts[match[1]] = match[2];
      }
      if (parts.minute !== undefined && parts.hour !== undefined) {
        cronExpression = `${parts.minute} ${parts.hour} ${parts.day || '*'} ${parts.month || '*'} ${parts.day_of_week || '*'}`;
      }
    }

    // 从 job 中读取 content_type
    const contentType = job.task_type === 'update_hk_prices' ? 'hk' :
                        job.task_type === 'update_moneyflow' ? 'moneyflow' :
                        job.task_type === 'update_index_data' ? 'index' :
                        job.task_type === 'update_industry_statistics' ? 'statistics' :
                        job.task_type === 'update_dragon_list' ? 'dragon_list' : 'stock';

    // 设置表单初始值，从 job 中读取实际参数
    scheduleForm.setFieldsValue({
      name: job.name,
      cron_expression: cronExpression,
      content_type: contentType,
      mode: job.mode || 'incremental',
      stock_range: job.stock_range || 'all',
      schedule_markets: job.markets || ['main'],
      schedule_exclude_st: job.exclude_st !== false,
    });

    setScheduleModalVisible(true);
  };

  // 更新定时任务
  const handleUpdateScheduledJob = async () => {
    if (!editingJob) return;

    try {
      const values = await scheduleForm.validateFields();
      setLoading(true);

      const params: any = {
        name: values.name,
        trigger: {
          cron_expression: values.cron_expression,
        },
        content_type: values.content_type || 'stock',
        mode: values.mode || 'incremental',
      };

      // 根据任务类型设置不同参数
      if (values.content_type === 'stock' || values.content_type === 'moneyflow') {
        params.stock_range = values.stock_range || 'all';
        if (values.stock_range === 'market') {
          params.markets = values.schedule_markets || ['main'];
        }
        params.exclude_st = values.schedule_exclude_st !== false;
      } else if (values.content_type === 'index') {
        params.markets = values.markets || ['SSE', 'SZSE'];
      }

      await scheduleService.update(editingJob.id, params);
      message.success({
        content: '定时任务已更新',
        icon: <CheckCircleOutlined />,
        duration: 3,
      });
      setScheduleModalVisible(false);
      setEditingJob(null);
      scheduleForm.resetFields();
      loadScheduledJobs();
    } catch (error) {
      if (error instanceof Error) {
        message.error({
          content: `更新失败：${error.message}`,
          icon: <CloseCircleOutlined />,
          duration: 5,
        });
      }
    } finally {
      setLoading(false);
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
                      <Tooltip key="edit" title="编辑任务">
                        <Button
                          type="text"
                          size="large"
                          icon={<EditOutlined aria-hidden="true" />}
                          onClick={() => handleEditScheduledJob(job)}
                          aria-label="编辑定时任务"
                          style={{ height: 44, minWidth: 44 }}
                        >
                          编辑
                        </Button>
                      </Tooltip>,
                      job.enabled ? (
                        <Tooltip key="pause" title="暂停任务">
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
                        <Tooltip key="resume" title="恢复任务">
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
                        key="delete"
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
        <Form
          form={form}
          layout="vertical"
          initialValues={{ content_type: 'stock', mode: 'incremental', scope: 'all', date_range: '2y' }}
          onValuesChange={(changedValues) => {
            if ('content_type' in changedValues) {
              if (changedValues.content_type === 'dragon_list') {
                form.setFieldsValue({ date_range: 'incremental' });
              } else {
                form.setFieldsValue({ date_range: '2y' });
              }
            }
          }}
        >
          <Form.Item
            label="更新内容"
            name="content_type"
            rules={[{ required: true, message: '请选择更新内容' }]}
            extra="选择要更新的数据类型"
          >
            <Radio.Group>
              <Radio value="stock">A股数据</Radio>
              <Radio value="hk">港股数据</Radio>
              <Radio value="financial">财务数据</Radio>
              <Radio value="index">指数数据</Radio>
              <Radio value="industry">申万行业分类</Radio>
              <Radio value="statistics">行业统计</Radio>
              <Radio value="moneyflow">资金流向</Radio>
              <Radio value="dragon_list">龙虎榜</Radio>
            </Radio.Group>
          </Form.Item>

          <Form.Item noStyle shouldUpdate={(prev, curr) => prev.content_type !== curr.content_type}>
            {({ getFieldValue }) => {
              const contentType = getFieldValue('content_type');
              // 龙虎榜不需要更新模式（通过日期字段决定模式）
              // 财务数据也不需要更新模式
              if (contentType !== 'financial' && contentType !== 'dragon_list') {
                return (
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
                );
              }
              return null;
            }}
          </Form.Item>

          <Form.Item noStyle shouldUpdate={(prev, curr) => prev.content_type !== curr.content_type || prev.scope !== curr.scope}>
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
              ) : getFieldValue('content_type') === 'dragon_list' ? null : (
                <>
                  <Form.Item
                    label={
                      getFieldValue('content_type') === 'hk'
                        ? '港股范围'
                        : '股票范围'
                    }
                    name="scope"
                    rules={[{ required: true, message: '请选择股票范围' }]}
                    extra={
                      getFieldValue('content_type') === 'hk'
                        ? '选择要更新的港股范围'
                        : '选择要更新的股票范围'
                    }
                  >
                    <Radio.Group>
                      <Radio value="all">
                        {getFieldValue('content_type') === 'hk' ? '全部港股' : '全部A股'}
                      </Radio>
                      {getFieldValue('content_type') !== 'hk' && (
                        <Radio value="market">按市场选择</Radio>
                      )}
                      <Radio value="favorites">收藏列表</Radio>
                      <Radio value="custom">自定义</Radio>
                    </Radio.Group>
                  </Form.Item>
                  {getFieldValue('content_type') !== 'hk' && getFieldValue('scope') === 'market' && (
                    <Form.Item
                      label="选择市场"
                      name="markets"
                      initialValue={['main']}
                      rules={[{ required: true, message: '请至少选择一个市场' }]}
                      extra="可多选市场类型"
                    >
                      <Checkbox.Group>
                        <Space direction="vertical">
                          <Checkbox value="main">主板（沪深主板）</Checkbox>
                          <Checkbox value="gem">创业板</Checkbox>
                          <Checkbox value="star">科创板</Checkbox>
                          <Checkbox value="bse">北交所</Checkbox>
                        </Space>
                      </Checkbox.Group>
                    </Form.Item>
                  )}
                  {getFieldValue('content_type') !== 'hk' && (
                    <Form.Item
                      name="exclude_st"
                      valuePropName="checked"
                      initialValue={true}
                    >
                      <Checkbox>排除ST股票</Checkbox>
                    </Form.Item>
                  )}
                </>
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
                  label={
                    getFieldValue('content_type') === 'hk'
                      ? '自定义港股代码'
                      : '自定义股票代码'
                  }
                  name="custom_stocks"
                  rules={[{ required: true, message: '请输入股票代码' }]}
                  extra={
                    getFieldValue('content_type') === 'hk'
                      ? '每行一个港股代码，如：00700、00941、02318'
                      : '每行一个股票代码，如：000001、600000、600519'
                  }
                >
                  <TextArea
                    placeholder={
                      getFieldValue('content_type') === 'hk'
                        ? '00700&#10;00941&#10;02318'
                        : '000001&#10;600000&#10;600519'
                    }
                    rows={5}
                    spellCheck={false}
                    aria-label={
                      getFieldValue('content_type') === 'hk'
                        ? '港股代码列表'
                        : '股票代码列表'
                    }
                  />
                </Form.Item>
              ) : null
            }
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prev, curr) =>
              prev.content_type !== curr.content_type ||
              prev.mode !== curr.mode ||
              prev.date_range !== curr.date_range
            }
          >
            {({ getFieldValue }) => {
              const contentType = getFieldValue('content_type');
              const mode = getFieldValue('mode');
              const dateRange = getFieldValue('date_range');
              const showForFullMode =
                (contentType === 'stock' || contentType === 'hk' || contentType === 'moneyflow') && mode === 'full';
              const showForDragonList = contentType === 'dragon_list';
              if (!showForFullMode && !showForDragonList) return null;
              return (
                <>
                  <Form.Item
                    label="数据日期范围"
                    name="date_range"
                    extra={
                      showForDragonList
                        ? '选择要获取的龙虎榜历史数据范围'
                        : '选择全量更新的历史数据起始时间'
                    }
                  >
                    <Radio.Group>
                      {showForDragonList && <Radio value="incremental">增量更新（近期）</Radio>}
                      <Radio value="1y">过去1年</Radio>
                      <Radio value="2y">过去2年</Radio>
                      <Radio value="5y">过去5年</Radio>
                      <Radio value="10y">过去10年</Radio>
                      <Radio value="custom">自定义日期</Radio>
                    </Radio.Group>
                  </Form.Item>
                  {dateRange === 'custom' && (
                    <>
                      <Form.Item
                        label="开始日期"
                        name="start_date"
                        extra="格式: YYYY-MM-DD"
                      >
                        <Input placeholder="例如: 2023-01-01" autoComplete="off" aria-label="开始日期" />
                      </Form.Item>
                      <Form.Item
                        label="结束日期"
                        name="end_date"
                        extra="可选，留空表示今天，格式: YYYY-MM-DD"
                      >
                        <Input placeholder="例如: 2025-12-31" autoComplete="off" aria-label="结束日期" />
                      </Form.Item>
                    </>
                  )}
                </>
              );
            }}
          </Form.Item>
        </Form>
      </Modal>
      <Modal
        title={editingJob ? '编辑定时任务' : '创建定时任务'}
        open={scheduleModalVisible}
        onOk={editingJob ? handleUpdateScheduledJob : handleCreateScheduledJob}
        onCancel={() => {
          setScheduleModalVisible(false);
          setEditingJob(null);
          scheduleForm.resetFields();
        }}
        confirmLoading={loading}
        width={600}
      >
        <Form
          form={scheduleForm}
          layout="vertical"
          initialValues={{ content_type: 'stock', mode: 'incremental', stock_range: 'all', schedule_markets: ['main'], schedule_exclude_st: true }}
          onValuesChange={(changedValues) => {
            // 当切换到"按市场选择"时，确保设置默认市场值
            if (changedValues.stock_range === 'market') {
              const currentMarkets = scheduleForm.getFieldValue('schedule_markets');
              if (!currentMarkets || currentMarkets.length === 0) {
                scheduleForm.setFieldsValue({ schedule_markets: ['main'] });
              }
            }
          }}
        >
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
              <Tooltip title="工作日15:30（收盘后）">
                <Button size="small" onClick={() => scheduleForm.setFieldsValue({ cron_expression: '30 15 * * 1-5' })}>
                  工作日15:30
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
              <Radio value="stock">A股数据</Radio>
              <Radio value="hk">港股数据</Radio>
              <Radio value="index">指数数据</Radio>
              <Radio value="statistics">行业统计</Radio>
              <Radio value="moneyflow">资金流向</Radio>
              <Radio value="dragon_list">龙虎榜</Radio>
            </Radio.Group>
          </Form.Item>

          <Form.Item noStyle shouldUpdate={(prev, curr) => prev.content_type !== curr.content_type || prev.stock_range !== curr.stock_range}>
            {({ getFieldValue }) => {
              const contentType = getFieldValue('content_type');
              const stockRange = getFieldValue('stock_range');
              if (contentType === 'stock' || contentType === 'hk' || contentType === 'moneyflow') {
                return (
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
                      extra={
                        contentType === 'hk'
                          ? '选择要更新的港股范围'
                          : '选择要更新的股票范围'
                      }
                    >
                      <Radio.Group>
                        <Radio value="all">
                          {contentType === 'hk' ? '全部港股' : '全部A股'}
                        </Radio>
                        {contentType !== 'hk' && (
                          <Radio value="market">按市场选择</Radio>
                        )}
                        <Radio value="favorites">收藏列表</Radio>
                      </Radio.Group>
                    </Form.Item>

                    {contentType !== 'hk' && stockRange === 'market' && (
                      <Form.Item
                        label="选择市场"
                        name="schedule_markets"
                        initialValue={['main']}
                        rules={[{ required: true, message: '请至少选择一个市场' }]}
                        extra="可多选市场类型"
                      >
                        <Checkbox.Group>
                          <Space direction="vertical">
                            <Checkbox value="main">主板（沪深主板）</Checkbox>
                            <Checkbox value="gem">创业板</Checkbox>
                            <Checkbox value="star">科创板</Checkbox>
                            <Checkbox value="bse">北交所</Checkbox>
                          </Space>
                        </Checkbox.Group>
                      </Form.Item>
                    )}

                    {contentType !== 'hk' && (
                      <Form.Item
                        name="schedule_exclude_st"
                        valuePropName="checked"
                        initialValue={true}
                      >
                        <Checkbox>排除ST股票</Checkbox>
                      </Form.Item>
                    )}
                  </>
                );
              } else if (contentType === 'dragon_list') {
                // 龙虎榜定时任务：不需要额外参数
                return null;
              } else {
                // 其他类型（如index）：显示市场选择
                return (
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
                );
              }
            }}
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
