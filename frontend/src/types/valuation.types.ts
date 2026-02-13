// Valuation method types
export type ValuationMethod = 'pe' | 'pb' | 'ps' | 'peg' | 'dcf' | 'combined';

// Combination method types
export type CombineMethod = 'weighted' | 'average' | 'median' | 'max_confidence';

// Valuation rating types
export type ValuationRating = '强烈买入' | '买入' | '持有' | '卖出' | '强烈卖出';

// Single valuation result
export interface ValuationResult {
  symbol: string;
  date: string;
  model: string;
  fair_value: number;
  current_price: number;
  upside_downside: number;  // percentage
  rating: ValuationRating;
  confidence: number;       // 0-1
  metrics: {
    [key: string]: any;
    pe?: number;
    pb?: number;
    ps?: number;
    peg?: number;
    industry_pe?: number;
    industry_pb?: number;
    industry_ps?: number;
  };
  assumptions: {
    [key: string]: any;
  };
  warnings: string[];
  individual_results?: ValuationResult[];  // For combined model
}

// DCF configuration types
export type RiskProfile = 'conservative' | 'balanced' | 'aggressive';

export interface DCFConfig {
  risk_profile?: RiskProfile;
  forecast_years?: number;
  terminal_growth?: number;
  risk_free_rate?: number;
  market_return?: number;
  tax_rate?: number;
  credit_spread?: number;
  growth_rate_cap?: number;
  wacc_min?: number;
  wacc_max?: number;
  beta?: number;
}

// Valuation request parameters
export interface ValuationRequest {
  symbol: string;
  methods?: ValuationMethod[];
  date?: string;
  combine_method?: CombineMethod;
  dcf_config?: DCFConfig;
}

// Batch valuation request
export interface BatchValuationRequest {
  symbols: string[];
  methods?: ValuationMethod[];
  date?: string;
  combine_method?: CombineMethod;
}

// Valuation comparison request
export interface CompareValuationRequest {
  symbols: string[];
  method?: ValuationMethod;
  date?: string;
}

// API response types
export interface ValuationResponse {
  success: boolean;
  symbol: string;
  valuation: ValuationResult;
  error?: string;
}

export interface BatchValuationResponse {
  success: boolean;
  valuations: Array<ValuationResult | { symbol: string; error: string }>;
}

export interface CompareValuationResponse {
  success: boolean;
  comparison: {
    date: string;
    method: string;
    summary: {
      [key: string]: any;
    };
    stocks: ValuationResult[];
  };
}
