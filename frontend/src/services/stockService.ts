import api from './api';
import type {
  Stock,
  StockData,
  QueryParams,
  StockSearchResult,
  ScreenParams,
  ScreenResult,
} from '@/types';

export const stockService = {
  // Search stocks by code or name
  search: (q: string) => {
    return api.get<StockSearchResult>(`/stock/search?q=${encodeURIComponent(q)}`);
  },

  // Query stock data
  query: (params: QueryParams) => {
    // Transform camelCase to snake_case for backend compatibility
    const backendParams = {
      symbols: params.symbols,
      start_date: params.startDate,
      end_date: params.endDate,
      price_type: params.priceType,
    };
    return api.post<Record<string, StockData[]>>('/stock/query', backendParams);
  },

  // Get stock name by code
  getName: (code: string) => {
    return api.get<{ name: string }>(`/stock/name/${code}`);
  },

  // Get minimum date in database
  getMinDate: () => {
    return api.get<{ date: string }>('/stock/min-date');
  },

  // Export data to CSV
  exportCSV: (params: QueryParams) => {
    const backendParams = {
      symbols: params.symbols,
      start_date: params.startDate,
      end_date: params.endDate,
      price_type: params.priceType,
    };
    return api.post('/stock/export/csv', backendParams, { responseType: 'blob' });
  },

  // Export data to Excel
  exportExcel: (params: QueryParams) => {
    const backendParams = {
      symbols: params.symbols,
      start_date: params.startDate,
      end_date: params.endDate,
      price_type: params.priceType,
    };
    return api.post('/stock/export/excel', backendParams, { responseType: 'blob' });
  },

  // Traditional stock screening
  screen: (params: ScreenParams) => {
    return api.post<ScreenResult>('/stock/screen', params);
  },

  // AI-powered stock screening (REST API)
  aiScreen: (query: string) => {
    return api.post<{
      success: boolean;
      explanation: string;
      stocks: any[];
      params: any;
      count: number;
    }>('/stock/ai-screen', { query });
  },
};
