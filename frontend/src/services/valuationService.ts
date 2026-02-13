import api from './api';
import type {
  ValuationRequest,
  ValuationResponse,
  BatchValuationRequest,
  BatchValuationResponse,
  CompareValuationRequest,
  CompareValuationResponse,
  ValuationMethod,
  DCFConfig
} from '@/types';

export const valuationService = {
  // Get single stock valuation summary
  getSummary: (symbol: string, params?: {
    methods?: ValuationMethod[];
    date?: string;
    combine_method?: 'weighted' | 'average' | 'median' | 'max_confidence';
    dcf_config?: DCFConfig;
  }) => {
    // Build query params as a plain object (not URLSearchParams)
    // This ensures axios correctly serializes complex values like dcf_config JSON
    const queryParams: Record<string, string> = {};
    if (params?.methods && params.methods.length > 0) {
      queryParams.methods = params.methods.join(',');
    }
    if (params?.date) {
      queryParams.date = params.date;
    }
    if (params?.combine_method) {
      queryParams.combine_method = params.combine_method;
    }
    if (params?.dcf_config) {
      // Convert dcf_config to JSON string for proper transmission
      queryParams.dcf_config = JSON.stringify(params.dcf_config);
    }

    return api.get<ValuationResponse>(`/valuation/summary/${symbol}`, {
      params: queryParams
    });
  },

  // Batch valuation
  batch: (request: BatchValuationRequest) => {
    const body: any = {
      symbols: request.symbols,
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
  }
};
