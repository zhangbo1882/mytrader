import api from './api';
import type {
  ScreeningConfig,
  ScreeningResult,
  StrategiesListResponse,
  CriteriaTypesResponse,
  PresetStrategy,
  SaveHistoryResponse,
  HistoryListResponse,
  HistoryDetailResponse,
  ReRunHistoryResponse,
  ScreeningStock
} from '@/types';

export interface Industry {
  code: string;
  name: string;
  parent_code: string;
}

export interface IndustriesResponse {
  success: boolean;
  level: number;
  industries: Industry[];
}

export const screeningService = {
  // Get all preset strategies
  listStrategies: () => {
    return api.get<StrategiesListResponse>('/screening/strategies');
  },

  // Apply preset strategy
  applyPresetStrategy: (strategyName: PresetStrategy, limit: number = 100) => {
    return api.get<ScreeningResult>(
      `/screening/strategies/${strategyName}?limit=${limit}`
    );
  },

  // Apply custom screening
  applyCustomStrategy: (config: ScreeningConfig, limit: number = 2000) => {
    return api.post<ScreeningResult>('/screening/custom', {
      config,
      limit
    });
  },

  // Get supported criteria types
  getCriteriaTypes: () => {
    return api.get<CriteriaTypesResponse>('/screening/criteria-types');
  },

  // Get industries by level (1=Level 1, 2=Level 2, 3=Level 3)
  getIndustries: (level: number = 1) => {
    return api.get<IndustriesResponse>(`/screening/industries?level=${level}`);
  },

  // Screening history methods
  saveHistory: (name: string, config: ScreeningConfig, stocks?: ScreeningStock[]) => {
    return api.post<SaveHistoryResponse>('/screening/history', {
      name,
      config,
      stocks
    });
  },

  getHistory: (userId?: string) => {
    const params = userId ? `?user_id=${userId}` : '';
    return api.get<HistoryListResponse>(`/screening/history${params}`);
  },

  getHistoryDetail: (id: number, userId?: string) => {
    const params = userId ? `?user_id=${userId}` : '';
    return api.get<HistoryDetailResponse>(`/screening/history/${id}${params}`);
  },

  reRunHistory: (id: number, limit: number = 2000, userId?: string) => {
    const params = new URLSearchParams();
    if (limit) params.append('limit', limit.toString());
    if (userId) params.append('user_id', userId);
    return api.post<ReRunHistoryResponse>(`/screening/history/${id}/re-run?${params.toString()}`);
  },

  deleteHistory: (id: number, userId?: string) => {
    const params = userId ? `?user_id=${userId}` : '';
    return api.delete<{success: boolean; message: string}>(`/screening/history/${id}${params}`);
  }
};
