// ML and Prediction types

export type ModelType = 'lightgbm' | 'lstm' | 'xgboost' | 'random_forest';

export type PredictionTarget = '1d_return' | '5d_return' | 'trend' | 'trend_5d' | 'volatility';

export interface ModelTrainingParams {
  stockCode: string;
  startDate: string;
  endDate: string;
  target: PredictionTarget;
  modelType: ModelType;
  features?: string[];
  testSize?: number;
  validationSplit?: number;
  walkForward?: boolean;
  nSplits?: number;
}

export interface WalkForwardFoldResult {
  fold: number;
  trainSamples: number;
  testSamples: number;
  testDateRange?: [string, string];
  metrics: Record<string, number>;
}

export interface MLModel {
  id: string;
  stockCode: string;
  stockName: string;
  modelType: ModelType;
  target: PredictionTarget;
  startDate: string;
  endDate: string;
  features: string[];
  accuracy?: number;
  precision?: number;
  recall?: number;
  f1Score?: number;
  createdAt: string;
  status: 'training' | 'completed' | 'failed';
  error?: string;
  // 回归指标
  mae?: number;
  rmse?: number;
  r2?: number;
  mape?: number;
  // 特征重要性
  featureImportance?: { feature: string; importance: number }[];
  // Walk-forward 滚动窗口指标
  walkForward?: boolean;
  nSplits?: number;
  cvScore?: number;
  cvStd?: number;
  cvMetric?: string;
  foldResults?: WalkForwardFoldResult[];
}

export interface PredictionResult {
  stockCode: string;
  stockName: string;
  date: string;
  prediction: number;
  confidence: number;
  targetType?: string; // 预测目标类型，如 'return_1d', 'direction_1d' 等
  actual?: number;
  error?: number;
}

export interface ModelPerformance {
  accuracy: number;
  precision: number;
  recall: number;
  f1Score: number;
  confusionMatrix?: number[][];
  featureImportance?: { feature: string; importance: number }[];
}
