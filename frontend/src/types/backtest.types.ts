// Strategy parameter schema for dynamic form generation
export interface StrategyParamSchema {
  type: 'object';
  properties: Record<string, StrategyParam>;
  required?: string[];
}

// Task status for polling
export interface BacktestTaskStatus {
  task_id: string;
  task_type: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'paused';
  progress?: number;
  message?: string;
  created_at?: string;
  updated_at?: string;
}

// Backtest request
export interface BacktestRequest {
  stock: string;
  start_date: string;
  end_date?: string;
  cash?: number;
  commission?: number;
  benchmark?: string;
  strategy: string;
  strategy_params: Record<string, any>;
}

// Backtest result and metrics
export interface BacktestResult {
  basic_info: {
    stock: string;
    start_date: string;
    end_date: string;
    initial_cash: number;
    final_value: number;
    total_return: number;
  };
  trade_stats: {
    total_trades: number;
    winning_trades: number;
    losing_trades: number;
    win_rate: number;
  };
  trades: Array<{
    buy_date: string;
    sell_date: string;
    buy_price: number;
    sell_price: number;
    size: number;
  }>;
  health_metrics: {
    annual_return: number;
    sharpe_ratio: number;
    sortino_ratio: number;
    calmar_ratio: number;
    max_drawdown: number;
  };
}

// History list item (for table display)
export interface BacktestHistory {
  task_id: string;
  name: string;
  stock: string;
  stock_name: string;  // 股票名称
  strategy: string;
  strategy_name: string;
  total_return: number;
  sharpe_ratio: number;
  max_drawdown: number;
  created_at: string;
  completed_at: string;
}

// History list response
export interface BacktestHistoryListResponse {
  success: boolean;
  total: number;
  history: BacktestHistory[];
}

// History detail (extends BacktestHistory with result field)
export interface BacktestHistoryDetail extends BacktestHistory {
  result: BacktestResult;
}

// History list request with filters
export interface BacktestHistoryFilters {
  page?: number;
  page_size?: number;
  stock?: string;
  strategy?: string;
}

export interface BacktestHistoryListResponse {
  success: boolean;
  total: number;
  history: BacktestHistory[];
}