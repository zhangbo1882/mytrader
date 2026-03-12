import api from './api';
import type { Task, CreateTaskParams } from '@/types';

// Helper function to transform backend task format to frontend format
function transformTask(task: any): Task {
  const stats = task.stats || { success: 0, failed: 0, skipped: 0 };
  const totalStocks = task.total_stocks || stats.total || 0;

  // Use current_stock_index as processed count (more accurate than stats.success)
  // For tasks like index updates, stats.success might be 0 while current_stock_index shows real progress
  const processedCount = task.current_stock_index ?? stats.success ?? 0;

  // Calculate progress
  let progress: number;
  if (task.status === 'completed' || task.status === 'completed_with_errors') {
    // For completed tasks, always show 100%
    progress = 100;
  } else if (totalStocks > 0) {
    progress = Math.floor((processedCount / totalStocks) * 100);
  } else {
    // For tasks without stock counts (like industry_statistics), use backend progress
    progress = task.progress || 0;
  }

  // Map backend task_type to frontend type
  const taskTypeMap: Record<string, 'update' | 'screen' | 'prediction' | 'backtest'> = {
    // Update tasks
    'update_stock_prices': 'update',
    'update_hk_prices': 'update',
    'update_financial_reports': 'update',
    'update_index_data': 'update',
    'update_industry_classification': 'update',
    'update_industry_statistics': 'update',
    'update_moneyflow': 'update',
    'calculate_industry_moneyflow': 'update',
    'update_dragon_list': 'update',

    // Screen tasks
    'screen_stocks': 'screen',

    // Prediction/AI tasks
    'price_breakout': 'prediction',
    'sma_cross': 'prediction',

    // Backtest tasks
    'backtest': 'backtest',
  };

  // Determine type: use metadata type first, then mapped task_type, default to 'update'
  const backendType = task.metadata?.type || task.task_type;
  const mappedType = backendType ? taskTypeMap[backendType] : undefined;
  const type: 'update' | 'screen' | 'prediction' | 'backtest' = mappedType || 'update';

  return {
    id: task.task_id,
    type,
    status: task.status,
    total: totalStocks,
    processed: processedCount,
    failed: stats.failed,
    startTime: task.start_time || task.created_at,
    endTime: task.end_time || task.completed_at,  // Use completed_at if end_time is not available
    params: task.params,
    result: task.result,
    error: task.error,
    progress,
  };
}

export const taskService = {
  // List all tasks
  list: async () => {
    try {
      // Use fetch instead of axios to handle Infinity values in JSON
      const response = await fetch('/api/tasks');
      const text = await response.text();

      // Parse JSON with Infinity handling - replace Infinity with null
      const parseJSONWithInfinity = (jsonString: string) => {
        // Simple approach: replace Infinity with 1e308 (a very large number)
        const cleaned = jsonString
          .replace(/:\s*Infinity\b/g, ': 1e308')
          .replace(/:\s*-Infinity\b/g, ': -1e308')
          .replace(/:\s*NaN\b/g, ': 0');

        return JSON.parse(cleaned);
      };

      const data = parseJSONWithInfinity(text);
      const tasks = data.tasks || [];
      return {
        success: data.success,
        tasks: tasks.map(transformTask),
      };
    } catch (error) {
      console.error('Failed to fetch tasks:', error);
      return {
        success: false,
        tasks: [],
      };
    }
  },

  // Get task by ID
  get: async (id: string) => {
    const task = await api.get<any>(`/tasks/${id}`);
    return transformTask(task.task || task);
  },

  // Create a new task
  create: (params: CreateTaskParams) => {
    // If type is a specific backend task_type (not a frontend category), use it directly
    const frontendTypes = ['update', 'screen', 'prediction', 'backtest'];
    if (params.type && !frontendTypes.includes(params.type)) {
      return api.post<Task>('/tasks/create', {
        task_type: params.type,
        params: params.params
      });
    }

    // Otherwise, determine the task_type based on content_type
    const contentType = params.params?.content_type || 'stock';
    let taskType: string;

    switch (contentType) {
      case 'hk':
        taskType = 'update_hk_prices';
        break;
      case 'financial':
        taskType = 'update_financial_reports';
        break;
      case 'index':
        taskType = 'update_index_data';
        break;
      case 'industry':
        taskType = 'update_industry_classification';
        break;
      case 'statistics':
        taskType = 'update_industry_statistics';
        break;
      case 'moneyflow':
        taskType = 'update_moneyflow';
        break;
      case 'industry_moneyflow_summary':
        taskType = 'calculate_industry_moneyflow';
        break;
      case 'dragon_list':
        taskType = 'update_dragon_list';
        break;
      case 'stock':
      default:
        taskType = 'update_stock_prices';
        break;
    }

    // Use the new unified task creation API
    return api.post<Task>('/tasks/create', {
      task_type: taskType,
      params: params.params
    });
  },

  // Pause a task
  pause: (id: string) => {
    return api.post<{ success: boolean }>(`/tasks/${id}/pause`);
  },

  // Resume a paused task
  resume: (id: string) => {
    return api.post<{ success: boolean }>(`/tasks/${id}/resume`);
  },

  // Stop a task
  stop: (id: string) => {
    return api.post<{ success: boolean }>(`/tasks/${id}/stop`);
  },

  // Delete a task
  delete: (id: string) => {
    return api.delete<{ success: boolean }>(`/tasks/${id}`);
  },

  // Clean up old tasks
  cleanup: async (days?: number) => {
    const response = await api.post<{ success: boolean; deleted: number }>('/tasks/cleanup', { days });
    return response as { success: boolean; deleted: number };
  },

  // Get task result
  getResult: (id: string) => {
    return api.get<any>(`/tasks/${id}`);
  },
};
