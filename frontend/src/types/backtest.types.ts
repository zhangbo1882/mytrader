export type StrategyParamValue = string | number | boolean | null | undefined;
export type StrategyParams = Record<string, StrategyParamValue>;

export interface StrategyParamField {
  type: 'integer' | 'number' | 'boolean' | 'string';
  title?: string;
  description?: string;
  default?: StrategyParamValue;
  minimum?: number;
  maximum?: number;
  enum?: string[];
}

export interface StrategyParamSchema {
  type: 'object';
  properties: Record<string, StrategyParamField>;
  required?: string[];
}

export interface StrategySchema {
  strategy_type: string;
  name: string;
  description?: string;
  params_schema: StrategyParamSchema;
}

export interface StrategySelectorValue {
  strategy: string;
  strategy_params: StrategyParams;
}

export interface BacktestTaskStatus {
  task_id: string;
  task_type: string;
  status: 'pending' | 'running' | 'completed' | 'failed' | 'paused';
  progress?: number;
  message?: string;
  created_at?: string;
  updated_at?: string;
}

export interface BacktestRequest {
  stock: string;
  start_date: string;
  end_date?: string;
  interval?: string;
  cash?: number;
  commission?: number;
  benchmark?: string;
  strategy: string;
  strategy_params: StrategyParams;
}

export interface BacktestTrade {
  buy_date: string;
  sell_date?: string | null;
  buy_price: number;
  sell_price?: number | null;
  buy_price_original?: number | null;
  sell_price_original?: number | null;
  size: number;
  hold_days?: number | null;
  buy_value: number;
  sell_value?: number | null;
  gross_pnl: number;
  commission: number;
  net_pnl: number;
  pnl_pct: number;
}

export interface BacktestBenchmarkMetrics {
  total_return?: number;
  volatility?: number;
  sharpe_ratio?: number;
  max_drawdown?: number;
}

export interface BacktestBenchmarkComparison {
  benchmark: string;
  benchmark_name?: string;
  benchmark_type?: string;
  benchmark_metrics?: BacktestBenchmarkMetrics | null;
  excess_return: number;
  excess_sharpe: number;
}

export interface BacktestResult {
  basic_info: {
    stock: string;
    actual_code?: string;
    start_date: string;
    end_date: string;
    initial_cash: number;
    final_value: number;
    total_return: number;
    commission: number;
  };
  strategy_info: {
    strategy: string;
    strategy_params: StrategyParams;
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
  trades: BacktestTrade[];
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
  benchmark_comparison: BacktestBenchmarkComparison | null;
  code_warning?: string;
  market_state_distribution?: Record<string, number>;
}

export interface BacktestHistory {
  task_id: string;
  name: string;
  stock: string;
  stock_name: string;
  strategy: string;
  strategy_name: string;
  total_return: number;
  sharpe_ratio: number;
  max_drawdown: number;
  created_at: string;
  completed_at?: string;
}

export interface BacktestHistoryDetail extends BacktestHistory {
  result: BacktestResult;
}

export interface BacktestHistoryFilters {
  page?: number;
  page_size?: number;
  stock?: string;
  strategy?: string;
}

export interface RunBacktestResponse {
  success: boolean;
  task_id: string;
  status: string;
  message: string;
}

export interface BacktestResultResponse {
  success: boolean;
  task_id: string;
  status: string;
  result: BacktestResult;
}

export interface BacktestHistoryListResponse {
  success: boolean;
  total: number;
  history: BacktestHistory[];
}

export interface BacktestHistoryDetailResponse {
  success: boolean;
  detail: BacktestHistoryDetail;
  message?: string;
}

export interface BacktestStrategiesResponse {
  success: boolean;
  strategies: StrategySchema[];
}

export interface BacktestBatchResponse {
  success: boolean;
  tasks: string[];
}

export interface BacktestDeleteHistoryResponse {
  success: boolean;
  message: string;
}

export interface BacktestOptimizedParamsResponse {
  stock_code: string;
  updated_at?: string;
  strategy: string;
  strategy_params: StrategyParams;
}

export interface BacktestHasOptimizedParamsResponse {
  stock_code: string;
  has_params: boolean;
  updated_at?: string;
}
