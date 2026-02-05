import { DateRange, PriceType } from './common.types';

export interface Stock {
  code: string;
  name: string;
  market?: string;
}

export interface StockData {
  date: string;
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
}

export interface QueryParams {
  symbols: string[];
  startDate: string;
  endDate: string;
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
