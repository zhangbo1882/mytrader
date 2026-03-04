import { api } from './index';
import type { StockMoneyflow, IndustryMoneyflow, IndustryLevel } from '@/types/moneyflow.types';

export const moneyflowService = {
  // 获取个股资金流向
  getStockMoneyflow: (tsCode: string, startDate?: string, endDate?: string, limit = 100) =>
    api.get<{ success: boolean; data: StockMoneyflow[]; count: number }>('/moneyflow/stock', {
      params: { ts_code: tsCode, start_date: startDate, end_date: endDate, limit }
    }),

  // 获取行业资金流向
  getIndustryMoneyflow: (
    level: IndustryLevel = 'L1',
    industryName?: string,
    startDate?: string,
    endDate?: string,
    limit = 100
  ) =>
    api.get<{ success: boolean; data: IndustryMoneyflow[]; count: number }>('/moneyflow/industry', {
      params: { level, industry_name: industryName, start_date: startDate, end_date: endDate, limit }
    }),

  // 获取行业排名
  getTopIndustries: (level: IndustryLevel = 'L1', tradeDate?: string, topN?: number, accumulateDays = 1) =>
    api.get<{ success: boolean; data: IndustryMoneyflow[]; count: number; trade_date: string }>(
      '/moneyflow/industry/top',
      {
        params: { level, trade_date: tradeDate, top_n: topN, accumulate_days: accumulateDays }
      }
    ),

  // 获取行业内个股资金流向
  getIndustryStocksMoneyflow: (
    industryName: string,
    level: IndustryLevel = 'L1',
    tradeDate?: string,
    accumulateDays = 1
  ) =>
    api.get<{
      success: boolean;
      data: StockMoneyflow[];
      count: number;
      trade_date: string;
      industry_name: string;
    }>('/moneyflow/industry/stocks', {
      params: { industry_name: industryName, level, trade_date: tradeDate, accumulate_days: accumulateDays }
    })
};
