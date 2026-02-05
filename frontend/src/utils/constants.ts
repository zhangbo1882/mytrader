// Application constants

export const PRICE_TYPES = [
  { label: '前复权', value: 'qfq' },
  { label: '后复权', value: 'hfq' },
  { label: '不复权', value: 'none' },
] as const;

export const DATE_RANGE_OPTIONS = [
  { label: '1个月', value: '1M' },
  { label: '3个月', value: '3M' },
  { label: '6个月', value: '6M' },
  { label: '1年', value: '1Y' },
  { label: '年初至今', value: 'YTD' },
] as const;

export const TASK_STATUS_MAP = {
  pending: { text: '等待中', color: 'default' },
  running: { text: '运行中', color: 'processing' },
  paused: { text: '已暂停', color: 'warning' },
  completed: { text: '已完成', color: 'success' },
  failed: { text: '失败', color: 'error' },
  stopped: { text: '已停止', color: 'default' },
} as const;

export const POLLING_INTERVAL = 10000; // 10 seconds

export const API_TIMEOUT = 60000; // 60 seconds
