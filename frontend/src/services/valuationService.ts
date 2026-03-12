import api from './api';
import type {
  ValuationResponse,
  BatchValuationRequest,
  BatchValuationResponse,
  CompareValuationRequest,
  CompareValuationResponse,
  ValuationMethod,
  CombineMethod,
  DCFConfig
} from '@/types';

export const valuationService = {
  // Get single stock valuation summary
  getSummary: (symbol: string, params?: {
    methods?: ValuationMethod[];
    date?: string;
    fiscal_date?: string;
    combine_method?: CombineMethod;
    dcf_config?: DCFConfig;
  }) => {
    const queryParams: Record<string, string> = {};
    if (params?.methods && params.methods.length > 0) {
      queryParams.methods = params.methods.join(',');
    }
    if (params?.date) {
      queryParams.date = params.date;
    }
    if (params?.fiscal_date) {
      queryParams.fiscal_date = params.fiscal_date;
    }
    if (params?.combine_method) {
      queryParams.combine_method = params.combine_method;
    }
    if (params?.dcf_config) {
      queryParams.dcf_config = JSON.stringify(params.dcf_config);
    }

    return api.get<ValuationResponse>(`/valuation/summary/${symbol}`, {
      params: queryParams
    });
  },

  // Batch valuation
  batch: (request: BatchValuationRequest) => {
    const body: any = {
      symbols: request.symbols.join(','),  // 后端期望逗号分隔的字符串
    };
    if (request.methods) {
      body.methods = request.methods.join(',');
    }
    if (request.date) {
      body.date = request.date;
    }
    if (request.combine_method) {
      body.combine_method = request.combine_method;
    }

    return api.post<BatchValuationResponse>('/valuation/batch', body);
  },

  // Valuation comparison
  compare: (request: CompareValuationRequest) => {
    const body: any = {
      symbols: request.symbols,
    };
    if (request.method) {
      body.method = request.method;
    }
    if (request.date) {
      body.date = request.date;
    }

    return api.post<CompareValuationResponse>('/valuation/compare', body);
  },

  // Get available models list
  getModels: () => {
    return api.get<{ success: boolean; models: string[] }>('/valuation/models');
  },

  // Bayesian prior matrix
  getPriorMatrixStatus: () => {
    return api.get<any>('/valuation/prior-matrix/status');
  },

  refreshPriorMatrix: (params: {
    years?: number;
    level?: 'L1' | 'L2' | 'both';
    min_stocks?: number;
  }) => {
    return api.post<any>('/valuation/prior-matrix/refresh', params);
  },
};
