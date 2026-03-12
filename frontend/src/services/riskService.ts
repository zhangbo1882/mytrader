/**
 * 风险管理 API 服务
 */
import api from './api';
import type {
  PortfolioInput,
  PortfolioState,
  CalculateRequest,
  CalculateResponse,
  SellRequest,
  SellResponse,
  AdjustStopLossRequest,
  AdjustStopLossResponse,
  AddPositionRequest,
  AddPositionResponse,
  // 数据库持久化类型
  PortfolioSettings,
  StoredPositionInput,
  PositionOutput,
  StoredPortfolio,
  PositionUpdateRequest,
  // 资金管理类型
  CapitalState,
  AdjustCapitalRequest,
  MonthlySnapshot,
  MonthlyCapitalChangeRequest,
} from '../types/risk.types';

const BASE_URL = '/risk';

/**
 * 获取投资组合状态
 */
export async function getPortfolioState(data: PortfolioInput): Promise<PortfolioState> {
  return api.post<PortfolioState>(`${BASE_URL}/portfolio`, data);
}

/**
 * 计算新股可买股数
 */
export async function calculateNewPosition(data: CalculateRequest): Promise<CalculateResponse> {
  return api.post<CalculateResponse>(`${BASE_URL}/calculate`, data);
}

/**
 * 卖出股票
 */
export async function sellPosition(data: SellRequest): Promise<SellResponse> {
  return api.post<SellResponse>(`${BASE_URL}/sell`, data);
}

/**
 * 调整止损参数
 */
export async function adjustStopLoss(data: AdjustStopLossRequest): Promise<AdjustStopLossResponse> {
  return api.post<AdjustStopLossResponse>(`${BASE_URL}/adjust-stop-loss`, data);
}

/**
 * 添加新持仓
 */
export async function addPosition(data: AddPositionRequest): Promise<AddPositionResponse> {
  return api.post<AddPositionResponse>(`${BASE_URL}/add-position`, data);
}

// ============================================================================
// 数据库持久化 API
// ============================================================================

/**
 * 加载投资组合设置
 */
export async function loadPortfolioSettings(): Promise<PortfolioSettings> {
  return api.get<PortfolioSettings>(`${BASE_URL}/portfolio/settings`);
}

/**
 * 保存投资组合设置
 */
export async function savePortfolioSettings(data: PortfolioSettings): Promise<PortfolioSettings> {
  return api.put<PortfolioSettings>(`${BASE_URL}/portfolio/settings`, data);
}

/**
 * 加载完整投资组合（含实时价格和股票名称）
 */
export async function loadFullPortfolio(): Promise<StoredPortfolio> {
  return api.get<StoredPortfolio>(`${BASE_URL}/portfolio/full`);
}

/**
 * 加载所有持仓（含实时价格和股票名称）
 */
export async function loadPositions(): Promise<{ positions: PositionOutput[] }> {
  return api.get<{ positions: PositionOutput[] }>(`${BASE_URL}/positions`);
}

/**
 * 添加持仓到数据库
 */
export async function addPositionToDb(data: StoredPositionInput): Promise<{ success: boolean; position: PositionOutput; error?: string }> {
  return api.post<{ success: boolean; position: PositionOutput; error?: string }>(`${BASE_URL}/positions`, data);
}

/**
 * 更新持仓
 */
export async function updatePositionInDb(symbol: string, data: PositionUpdateRequest): Promise<{ success: boolean; updated_at?: string; error?: string }> {
  return api.put<{ success: boolean; updated_at?: string; error?: string }>(`${BASE_URL}/positions/${symbol}`, data);
}

/**
 * 删除持仓
 */
export async function deletePositionFromDb(symbol: string): Promise<{ success: boolean; error?: string }> {
  return api.delete<{ success: boolean; error?: string }>(`${BASE_URL}/positions/${symbol}`);
}


// ============================================================================
// 资金管理 API
// ============================================================================

/**
 * 获取资金状态
 */
export async function getCapitalState(): Promise<CapitalState> {
  return api.get<any, CapitalState>(`${BASE_URL}/capital/state`);
}

/**
 * 调整初始资金
 */
export async function adjustInitialCapital(data: AdjustCapitalRequest): Promise<{
  success: boolean;
  old_initial_capital: number;
  new_initial_capital: number;
  change: number;
  reason?: string;
  error?: string;
}> {
  return api.put<any, any>(`${BASE_URL}/capital/initial`, data);
}


// ============================================================================
// 月度快照 API
// ============================================================================

/**
 * 获取月度快照列表
 */
export async function getMonthlySnapshots(params?: {
  start_month?: string;
  end_month?: string;
  limit?: number;
}): Promise<MonthlySnapshot[]> {
  const query = new URLSearchParams();
  if (params?.start_month) query.append('start_month', params.start_month);
  if (params?.end_month) query.append('end_month', params.end_month);
  if (params?.limit) query.append('limit', params.limit.toString());

  const queryString = query.toString();
  return api.get<any, MonthlySnapshot[]>(`${BASE_URL}/monthly/snapshots${queryString ? '?' + queryString : ''}`);
}

/**
 * 创建月度快照
 */
export async function createMonthlySnapshot(yearMonth?: string): Promise<MonthlySnapshot> {
  const query = yearMonth ? `?year_month=${yearMonth}` : '';
  return api.post<any, MonthlySnapshot>(`${BASE_URL}/monthly/snapshots${query}`);
}

/**
 * 更新月度资金变动
 */
export async function updateMonthlyCapitalChange(data: MonthlyCapitalChangeRequest): Promise<MonthlySnapshot & { success?: boolean; error?: string }> {
  return api.put<any, any>(`${BASE_URL}/monthly/capital-change`, data);
}

/**
 * 添加已实现盈亏（卖出股票后调用）
 */
export async function addRealizedPnl(realizedProfit: number, sellValue: number, symbol: string): Promise<{
  success: boolean;
  realized_profit: number;
  sell_value: number;
  old_cumulative_pnl: number;
  new_cumulative_pnl: number;
  old_cash: number;
  new_cash: number;
  error?: string;
}> {
  return api.post<any, any>(`${BASE_URL}/capital/realized-pnl`, {
    realized_profit: realizedProfit,
    sell_value: sellValue,
    symbol: symbol,
  });
}

/**
 * 更新剩余现金（手动校准）
 */
export async function updateCash(cash: number): Promise<CapitalState> {
  return api.put<CapitalState>(`${BASE_URL}/capital/cash`, { cash });
}
