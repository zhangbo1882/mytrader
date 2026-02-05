import api from './api';
import type { FinancialSummary, FinancialReport } from '@/types';

export const financialService = {
  // Get financial summary for a stock
  getSummary: (stockCode: string) => {
    return api.get<FinancialSummary>(`/financial/summary/${stockCode}`);
  },

  // Get full financial report
  getReport: (stockCode: string) => {
    return api.get<FinancialReport>(`/financial/report/${stockCode}`);
  },

  // Export financial data
  export: (stockCode: string, format: 'csv' | 'excel') => {
    return api.post(`/financial/export/${format}`, { stockCode }, { responseType: 'blob' });
  },
};
