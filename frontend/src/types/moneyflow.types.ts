/**
 * Money Flow Types
 */

export interface StockMoneyflow {
  ts_code: string;
  trade_date: string;
  stock_name?: string;
  buy_sm_vol: number;
  buy_sm_amount: number;
  sell_sm_vol: number;
  sell_sm_amount: number;
  buy_md_vol: number;
  buy_md_amount: number;
  sell_md_vol: number;
  sell_md_amount: number;
  buy_lg_vol: number;
  buy_lg_amount: number;
  sell_lg_vol: number;
  sell_lg_amount: number;
  buy_elg_vol: number;
  buy_elg_amount: number;
  sell_elg_vol: number;
  sell_elg_amount: number;
  net_mf_vol: number;
  net_mf_amount: number;
  net_lg_amount: number;
  net_elg_amount: number;
}

export interface IndustryMoneyflow {
  trade_date: string;
  level: string;
  sw_l1: string;
  sw_l2: string;
  sw_l3: string;
  index_code: string;
  stock_count: number;
  up_count: number;
  down_count: number;
  limit_up_count: number;
  limit_down_count: number;
  net_mf_amount: number;
  net_lg_amount: number;
  net_elg_amount: number;
  buy_elg_amount: number;
  sell_elg_amount: number;
  buy_lg_amount: number;
  sell_lg_amount: number;
  avg_net_amount: number;
  avg_net_lg_amount: number;
  avg_net_elg_amount: number;
}

export type IndustryLevel = 'L1' | 'L2' | 'L3';
