import { Stock } from './stock.types';

export interface Board {
  code: string;
  name: string;
  category?: string;
  description?: string;
  stockCount?: number;
  pe?: number;
  pb?: number;
  marketCap?: number;
}

export interface BoardDetail extends Board {
  stocks: Stock[];
  valuation?: ValuationData;
}

export interface ValuationData {
  pe: number;
  pb: number;
  ps: number;
  marketCap: number;
  avgPrice?: number;
}
