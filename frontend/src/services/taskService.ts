import api from './api';
import type { Task, CreateTaskParams, TaskListResponse } from '@/types';

// Helper function to transform backend task format to frontend format
function transformTask(task: any): Task {
  const stats = task.stats || { success: 0, failed: 0, skipped: 0 };
  const totalStocks = task.total_stocks || stats.total || 0;

  // Use current_stock_index as processed count (more accurate than stats.success)
  // For tasks like index updates, stats.success might be 0 while current_stock_index shows real progress
  const processedCount = task.current_stock_index ?? stats.success ?? 0;

  return {
    id: task.task_id,
    type: task.metadata?.type || task.task_type || 'update',
    status: task.status,
    total: totalStocks,
    processed: processedCount,
    failed: stats.failed,
    startTime: task.start_time || task.created_at,
    endTime: task.end_time,
    params: task.params,
    result: task.result,
    error: task.error,
    progress: totalStocks > 0 ? Math.floor((processedCount / totalStocks) * 100) : task.progress || 0,
  };
}

export const taskService = {
  // List all tasks
  list: async () => {
    const response = await api.get<{ success: boolean; tasks: any[] }>('/tasks');
    const tasks = response.tasks || [];
    return {
      success: response.success,
      tasks: tasks.map(transformTask),
    };
  },

  // Get task by ID
  get: async (id: string) => {
    const task = await api.get<any>(`/tasks/${id}`);
    return transformTask(task.task || task);
  },

  // Create a new task
  create: (params: CreateTaskParams) => {
    // Determine the task_type based on content_type
    const contentType = params.params?.content_type || 'stock';
    let taskType: string;

    switch (contentType) {
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
  cleanup: (days?: number) => {
    return api.post<{ success: boolean; deleted: number }>('/tasks/cleanup', { days });
  },

  // Get task result
  getResult: (id: string) => {
    return api.get<any>(`/tasks/${id}/result`);
  },
};
