import api from './api';
import type {
  BacktestBatchResponse,
  BacktestDeleteHistoryResponse,
  BacktestHasOptimizedParamsResponse,
  BacktestHistoryDetailResponse,
  BacktestHistoryFilters,
  BacktestHistoryListResponse,
  BacktestOptimizedParamsResponse,
  BacktestRequest,
  BacktestResultResponse,
  BacktestStrategiesResponse,
  BacktestTaskStatus,
  RunBacktestResponse,
} from '@/types';

export const backtestService = {
  run: (params: BacktestRequest) => {
    return api.post<RunBacktestResponse>('/backtest/run', params);
  },

  getResult: (taskId: string) => {
    return api.get<BacktestResultResponse>(`/backtest/result/${taskId}`);
  },

  getStatus: (taskId: string) => {
    return api.get<BacktestTaskStatus>(`/backtest/status/${taskId}`);
  },

  getStrategies: () => {
    return api.get<BacktestStrategiesResponse>('/backtest/strategies');
  },

  batch: (params: BacktestRequest[]) => {
    return api.post<BacktestBatchResponse>('/backtest/batch', { tasks: params });
  },

  getHistory: (filters?: BacktestHistoryFilters) => {
    const params = new URLSearchParams();
    if (filters?.page) params.append('page', filters.page.toString());
    if (filters?.page_size) params.append('page_size', filters.page_size.toString());
    if (filters?.stock) params.append('stock', filters.stock);
    if (filters?.strategy) params.append('strategy', filters.strategy);

    const queryString = params.toString();
    return api.get<BacktestHistoryListResponse>(
      `/backtest/history${queryString ? `?${queryString}` : ''}`
    );
  },

  getHistoryDetail: (taskId: string) => {
    return api.get<BacktestHistoryDetailResponse>(`/backtest/history/${taskId}`);
  },

  deleteHistory: (taskId: string) => {
    return api.delete<BacktestDeleteHistoryResponse>(`/backtest/history/${taskId}`);
  },

  getOptimizedParams: (stockCode: string) => {
    return api.get<BacktestOptimizedParamsResponse>(`/backtest/optimized-params/${stockCode}`);
  },

  hasOptimizedParams: (stockCode: string) => {
    return api.get<BacktestHasOptimizedParamsResponse>(`/backtest/has-optimized-params/${stockCode}`);
  },
};
