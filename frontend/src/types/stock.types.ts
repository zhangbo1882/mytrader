import { PriceType, IntervalType } from './common.types';

export interface Stock {
  code: string;
  name: string;
  market?: string;
}

export interface StockData {
  date: string;
  datetime?: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  amount?: number;
  turnover?: number;
  pct_chg?: number;
  change?: number;
  changePercent?: number;
  turnoverRate?: number;  // 换手率
  turnover_rate_f?: number;  // 后端返回的换手率字段
}

export interface QueryParams {
  symbols: string[];
  startDate: string;
  endDate: string;
  interval: IntervalType;
  priceType: PriceType;
}

export interface ScreenParams {
  [key: string]: any;
}

export interface ScreenResult {
  stocks: Stock[];
  count: number;
  query?: string;
  explanation?: string;
}

export interface StockSearchResult {
  stocks: Stock[];
}
