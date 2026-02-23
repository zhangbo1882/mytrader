// Common types used across the application

export interface ApiResponse<T> {
  data: T;
  error?: string;
  message?: string;
}

export interface PaginationParams {
  page: number;
  pageSize: number;
}

export interface DateRange {
  start: string;
  end: string;
}

export type PriceType = 'qfq' | 'hfq' | 'bfq';

export type IntervalType = '5m' | '15m' | '30m' | '60m' | '1d';

export type TaskStatus = 'pending' | 'running' | 'paused' | 'completed' | 'failed' | 'stopped';
