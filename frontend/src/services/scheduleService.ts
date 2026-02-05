import api from './api';
import type { ScheduledJob, CreateScheduledJobParams } from '@/types';

export const scheduleService = {
  // List all scheduled jobs
  list: (): Promise<{ jobs: ScheduledJob[] }> => {
    return api.get<{ jobs: ScheduledJob[] }>('/schedule/jobs') as Promise<{ jobs: ScheduledJob[] }>;
  },

  // Create a new scheduled job
  create: (params: CreateScheduledJobParams) => {
    return api.post<{ success: boolean; job_id: string; message?: string }>('/schedule/jobs', params);
  },

  // Delete a scheduled job
  delete: (id: string) => {
    return api.delete<{ success: boolean }>(`/schedule/jobs/${id}`);
  },

  // Pause a scheduled job
  pause: (id: string) => {
    return api.post<{ success: boolean }>(`/schedule/jobs/${id}/pause`);
  },

  // Resume a paused scheduled job
  resume: (id: string) => {
    return api.post<{ success: boolean }>(`/schedule/jobs/${id}/resume`);
  },
};
