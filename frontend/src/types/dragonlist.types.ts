/**
 * Dragon List Types
 */

export interface DragonList {
  ts_code: string;
  trade_date: string;
  name: string;
  close: number;
  pct_change: number;
  turnover_rate: number;
  amount: number;
  l_sell: number;
  l_buy: number;
  l_amount: number;
  net_amount: number;
  net_rate: number;
  amount_rate: number;
  float_values: number;
  reason: string;
}

export interface DragonListStats {
  summary: {
    total_count: number;
    reason_count: number;
    net_buy_count: number;
    net_sell_count: number;
    total_net_amount: number;
    total_l_amount: number;
    avg_net_rate: number;
  };
  by_reason: Array<{
    reason: string;
    count: number;
    net_amount: number;
  }>;
  trade_date: string;
}

export type DragonListSortBy = 'net_amount' | 'l_amount' | 'amount' | 'net_rate';
export type UpdateMode = 'incremental' | 'batch';
