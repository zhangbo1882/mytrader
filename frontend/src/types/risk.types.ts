/**
 * 风险管理类型定义
 */

// 持仓信息（输入）
export interface Position {
  symbol: string;
  name?: string;  // 股票名称
  shares: number;
  cost_price: number;
  current_price: number;
  price_date?: string;  // 价格日期
  stop_loss_base: number;
  stop_loss_percent: number;
}

// 持仓详情（含计算属性）
export interface PositionDetail extends Position {
  name?: string;  // 股票名称
  stop_loss_price: number;
  risk_per_share: number;
  total_risk: number;
  market_value: number;
  profit_per_share: number;
  total_profit: number;
  locked_profit: number;
}

// 投资组合状态
export interface PortfolioState {
  total_capital: number;
  total_risk_budget: number;
  single_risk_budget: number;
  used_risk: number;
  available_risk: number;
  positions_value: number;
  remaining_cash: number;
  risk_usage_percent: number;
  positions: PositionDetail[];
}

// 投资组合输入
export interface PortfolioInput {
  total_capital: number;
  max_total_risk_percent?: number;
  max_single_risk_percent?: number;
  positions: Position[];
}

// 计算新股可买股数请求
export interface CalculateRequest {
  total_capital: number;
  max_total_risk_percent?: number;
  max_single_risk_percent?: number;
  positions: Position[];
  buy_price: number;
  stop_loss_percent: number;
}

// 计算结果
export interface CalculateResponse {
  success: boolean;
  error?: string;
  portfolio: PortfolioState;
  new_position: {
    buy_price: number;
    stop_loss_percent: number;
    loss_per_share: number;
    max_by_total_risk: number;
    max_by_single_risk: number;
    max_by_cash: number;
    max_shares: number;
    required_capital: number;
    max_loss: number;
    limiting_factors: string[];
    limiting_factor_names: string[];
    after_buy: {
      new_total_risk: number;
      new_risk_usage: number;
      new_positions_value: number;
      new_remaining_cash: number;
    };
  };
}

// 卖出请求
export interface SellRequest {
  total_capital: number;
  max_total_risk_percent?: number;
  max_single_risk_percent?: number;
  positions: Position[];
  symbol: string;
  sell_shares: number;
  sell_price: number;
}

// 卖出结果
export interface SellResponse {
  success: boolean;
  error?: string;
  sell_info: {
    symbol: string;
    sell_shares: number;
    sell_price: number;
    sell_value: number;
    realized_profit: number;
    released_risk: number;
  };
  remaining_shares: number;
  portfolio: PortfolioState;
}

// 调整止损请求
export interface AdjustStopLossRequest {
  position: Position;
  new_stop_loss_base: number;
  new_stop_loss_percent: number;
}

// 调整止损结果
export interface AdjustStopLossResponse {
  success: boolean;
  error?: string;
  adjustment: {
    old_risk: number;
    new_risk: number;
    released_risk: number;
    old_stop_loss_price: number;
    new_stop_loss_price: number;
    locked_profit: number;
  };
  position: PositionDetail;
}

// 添加持仓请求
export interface AddPositionRequest {
  total_capital: number;
  max_total_risk_percent?: number;
  max_single_risk_percent?: number;
  positions: Position[];
  symbol: string;
  shares: number;
  cost_price: number;
  current_price: number;
  stop_loss_percent: number;
}

// 添加持仓结果
export interface AddPositionResponse {
  success: boolean;
  error?: string;
  position: PositionDetail;
  portfolio: PortfolioState;
}

// ============================================================================
// 数据库持久化类型
// ============================================================================

// 投资组合设置
export interface PortfolioSettings {
  total_capital: number;
  max_total_risk_percent: number;
  max_single_risk_percent: number;
  updated_at?: string;
}

// 数据库持仓（输入，不含 current_price 和 name）
export interface StoredPositionInput {
  symbol: string;
  shares: number;
  cost_price: number;
  stop_loss_base: number;
  stop_loss_percent: number;
}

// 持仓输出（包含从行情表获取的 current_price 和 name）
export interface PositionOutput {
  symbol: string;
  name: string;           // 股票名称（从 stock_names 表获取）
  shares: number;
  cost_price: number;
  current_price: number;  // 当前价格（从行情表获取）
  price_date?: string;    // 价格日期
  stop_loss_base: number;
  stop_loss_percent: number;
  created_at?: string;
  updated_at?: string;
}

// 完整投资组合（从数据库加载）
export interface StoredPortfolio {
  total_capital: number;
  max_total_risk_percent: number;
  max_single_risk_percent: number;
  positions: PositionOutput[];  // 包含 current_price 和 name
  updated_at?: string;
}

// 持仓更新请求
export interface PositionUpdateRequest {
  shares?: number;
  cost_price?: number;
  stop_loss_base?: number;
  stop_loss_percent?: number;
}

// ============================================================================
// 资金管理类型
// ============================================================================

// 资金状态
export interface CapitalState {
  initial_capital: number;       // 初始总资金
  cumulative_pnl: number;        // 累计盈亏
  current_capital: number;       // 当前实际总资金
  positions_value: number;       // 持仓市值
  cash: number;                  // 剩余现金
  floating_pnl: number;          // 浮动盈亏
}

// 调整初始资金请求
export interface AdjustCapitalRequest {
  initial_capital: number;
  reason?: string;
}

// 月度快照
export interface MonthlySnapshot {
  id: number;
  year_month: string;
  month_start_capital: number;
  month_start_positions_value: number;
  month_end_capital: number;
  month_end_positions_value: number;
  month_end_cash: number;
  month_pnl: number;
  month_pnl_percent: number;
  capital_change: number;
  capital_change_reason?: string;
  created_at: string;
  updated_at: string;
}

// 月度资金变动请求
export interface MonthlyCapitalChangeRequest {
  year_month: string;
  amount: number;
  reason?: string;
}
