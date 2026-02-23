import api from './api';
import type {
  BacktestRequest,
  BacktestResult,
  BacktestHistory,
  BacktestHistoryDetail,
  BacktestHistoryListResponse,
  BacktestHistoryDetailResponse,
  BacktestHistoryFilters
} from '@/types';

export interface BacktestRequest {
  stock: string;
  start_date: string;
  end_date: string;
  cash: number;
  commission: number;
  benchmark?: string;
  strategy: string;
  strategy_params: Record<string, any>;
}

export interface BacktestResult {
  basic_info: {
    stock: string;
    start_date: string;
    end_date: string;
    initial_cash: number;
    final_value: number;
    total_return: number;
    commission: number;
  };
  strategy_info: {
    strategy: string;
    strategy_params: Record<string, any>;
    strategy_name: string;
  };
  trade_stats: {
    total_trades: number;
    winning_trades: number;
    losing_trades: number;
    win_rate: number;
    total_profit: number;
    total_loss: number;
    profit_factor: number;
  };
  trades: Array<{
    buy_date: string;
    sell_date: string;
    buy_price: number;          // 复权买入价
    sell_price: number;         // 复权卖出价
    buy_price_original: number; // 实际买入价
    sell_price_original: number;// 实际卖出价
    size: number;
    hold_days: number;
    buy_value: number;
    sell_value: number;
    gross_pnl: number;
    commission: number;
    net_pnl: number;
    pnl_pct: number;
  }>;
  health_metrics: {
    annual_return: number;
    sharpe_ratio: number;
    sortino_ratio: number;
    calmar_ratio: number;
    max_drawdown: number;
    profit_factor: number;
    monthly_win_rate: number;
    volatility: number;
    total_return: number;
    avg_monthly_return: number;
  };
  benchmark_comparison: {
    benchmark: string;
    benchmark_metrics: any;
    excess_return: number;
    excess_sharpe: number;
  } | null;
}

export const backtestService = {
  // 运行回测
  run: (params: BacktestRequest) => {
    return api.post<{ success: boolean; task_id: string; status: string; message: string }>('/backtest/run', params);
  },

  // 获取回测结果
  getResult: (taskId: string) => {
    return api.get<{ success: boolean; task_id: string; status: string; result: BacktestResult }>(`/backtest/result/${taskId}`);
  },

  // 获取回测任务状态
  getStatus: (taskId: string) => {
    return api.get<any>(`/backtest/status/${taskId}`);
  },

  // 获取支持的策略列表
  getStrategies: () => {
    return api.get<{ success: boolean; strategies: any[] }>('/backtest/strategies');
  },

  // 批量回测
  batch: (params: BacktestRequest[]) => {
    return api.post<{ success: boolean; tasks: string[] }>('/backtest/batch', { tasks: params });
  },

  // 历史记录相关方法
  getHistory: (filters?: BacktestHistoryFilters) => {
    const params = new URLSearchParams();
    if (filters?.page) params.append('page', filters.page.toString());
    if (filters?.page_size) params.append('page_size', filters.page_size.toString());
    if (filters?.stock) params.append('stock', filters.stock);
    if (filters?.strategy) params.append('strategy', filters.strategy);

    const queryString = params.toString();
    return api.get<BacktestHistoryListResponse>(
      `/backtest/history${queryString ? '?' + queryString : ''}`
    );
  },

  getHistoryDetail: (taskId: string) => {
    return api.get<BacktestHistoryDetailResponse>(`/backtest/history/${taskId}`);
  },

  deleteHistory: (taskId: string) => {
    return api.delete<{success: boolean; message: string}>(`/backtest/history/${taskId}`);
  },

  // 获取股票的最优优化参数
  getOptimizedParams: (stockCode: string) => {
    return api.get<any>(`/backtest/optimized-params/${stockCode}`);
  },

  // 检查股票是否有优化参数
  hasOptimizedParams: (stockCode: string) => {
    return api.get<{ stock_code: string; has_params: boolean; updated_at?: string }>(`/backtest/has-optimized-params/${stockCode}`);
  }
};
