import { TaskStatus } from './common.types';

export interface Task {
  id: string;
  type: 'update' | 'screen' | 'prediction';
  status: TaskStatus;
  total: number;
  processed: number;
  failed: number;
  startTime: string;
  endTime?: string;
  params?: Record<string, any>;
  result?: any;
  error?: string;
  progress: number;
}

export interface TaskListResponse {
  tasks: Task[];
}

export interface CreateTaskParams {
  type: 'update' | 'screen' | 'prediction';
  params: Record<string, any>;
}

// Scheduled Job (Cron Job) types
export interface ScheduledJob {
  id: string;
  name: string;
  trigger?: string; // From API: cron[month='*', day='*', ...]
  task_type?: string; // Task type from backend: update_stock_prices, update_index_data, etc.
  content_type?: 'stock' | 'index';
  mode?: 'incremental' | 'full'; // Made optional
  stock_range?: 'all' | 'favorites' | 'custom';
  custom_stocks?: string[];
  markets?: ('SSE' | 'SZSE')[];
  next_run_time?: string;
  enabled: boolean;
}

export interface CreateScheduledJobParams {
  name: string;
  cron_expression: string;
  content_type?: 'stock' | 'index';
  mode?: 'incremental' | 'full';
  stock_range?: 'all' | 'favorites' | 'custom';
  custom_stocks?: string[];
  markets?: ('SSE' | 'SZSE')[];
}
