// Stock screening types

// Criteria type for filtering
export type CriteriaType =
  | 'Range'           // Range condition {min_val, max_val}
  | 'GreaterThan'     // Greater than {threshold}
  | 'LessThan'        // Less than {threshold}
  | 'Percentile'      // Percentile {percentile}
  | 'IndustryFilter'  // Industry filter {industries[], mode}
  | 'IndustryRelative' // Industry relative {percentile}
  | 'AmplitudeColumn' // Amplitude column {period, min_val/max_val/threshold}
  | 'TurnoverColumn'  // Turnover column {period, min_val/max_val/threshold}
  | 'PositiveDays'     // Positive days {period, threshold, min_positive_ratio}
  | 'MarketFilter'     // Market filter {markets[], mode}
  | 'BearToBull'       // Bear-to-bull transition {period, cycle}
  | 'ValuationUpside'; // Valuation upside {min_upside, max_upside, min_confidence, methods}

export interface Criteria {
  type: CriteriaType;
  column?: string;      // Field name: pe_ttm, pb, total_mv, etc. (optional for IndustryFilter)
  period?: number;      // For AmplitudeColumn/TurnoverColumn/PositiveDays: calculation period in days
  threshold?: number;   // For GreaterThan/LessThan/PositiveDays: threshold value
  min_positive_ratio?: number; // For PositiveDays: minimum positive day ratio (0-1)
  start_date?: string;  // Optional start date YYYYMMDD (alternative to period)
  end_date?: string;    // Optional end date YYYYMMDD (alternative to period)
  // Valuation screening parameters
  min_upside?: number;  // Minimum upside percentage for ValuationUpside
  max_upside?: number;  // Maximum upside percentage for ValuationUpside
  min_confidence?: number; // Minimum confidence threshold (0-1) for ValuationUpside
  methods?: string[];   // Valuation methods ['pe', 'pb', 'ps', 'peg'] for ValuationUpside
  [key: string]: any;
}

export interface ScreeningConfig {
  type: 'AND' | 'OR' | 'NOT';
  criteria: Criteria[];
}

// Preset strategies
export type PresetStrategy =
  | 'liquidity'        // Liquidity strategy
  | 'value'            // Value investing strategy
  | 'growth'           // Growth stock strategy
  | 'tech_growth'      // Tech growth strategy
  | 'quality'          // Quality strategy
  | 'dividend'         // Dividend strategy
  | 'low_volatility'   // Low volatility strategy
  | 'turnaround'       // Turnaround strategy
  | 'momentum_quality' // Momentum quality strategy
  | 'exclude_financials'; // Exclude financials strategy

// Screening result
export interface ScreeningStock {
  code: string;
  name: string;
  latest_close: number;
  pe_ttm: number | null;
  pb: number | null;
  total_mv_yi: number | null;
}

export interface ScreeningResult {
  success: boolean;
  count: number;
  stocks: ScreeningStock[];
}

// Preset strategy list response
export interface StrategiesListResponse {
  success: boolean;
  strategies: Array<{
    name: PresetStrategy;
    description: string;
    criteria_config?: ScreeningConfig;  // 新增：筛选条件配置
  }>;
}

// Criteria types list response
export interface CriteriaTypesResponse {
  success: boolean;
  types: CriteriaType[];
  criteria_details: Record<string, string>;
}

// Screening history types
export interface ScreeningHistory {
  id: number;
  name: string;
  result_count: number;
  stocks_count: number;
  created_at: string;
}

export interface ScreeningHistoryDetail extends ScreeningHistory {
  config: ScreeningConfig;
  stocks: ScreeningStock[];
}

export interface SaveHistoryResponse {
  success: boolean;
  history_id: number;
  message: string;
}

export interface HistoryListResponse {
  success: boolean;
  history: ScreeningHistory[];
}

export interface HistoryDetailResponse {
  success: boolean;
  detail: ScreeningHistoryDetail;
}

export interface ReRunHistoryResponse extends ScreeningResult {
  // Extends ScreeningResult which has success, count, stocks
}
