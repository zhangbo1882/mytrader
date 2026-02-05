export interface FinancialSummary {
  stockCode: string;
  stockName: string;
  updateTime: string;
  indicators: FinancialIndicator[];
}

export interface FinancialIndicator {
  key: string;
  name: string;
  value: number | string;
  unit?: string;
  date?: string;
}

export interface FinancialReport {
  income: FinancialDataItem[];
  balance: FinancialDataItem[];
  cashflow: FinancialDataItem[];
  indicators: FinancialDataItem[];
}

export interface FinancialDataItem {
  item: string;
  value: number;
  date: string;
}

export interface MetricCard {
  key: string;
  title: string;
  value: number | string;
  unit?: string;
  trend?: 'up' | 'down' | 'neutral';
  description?: string;
}
