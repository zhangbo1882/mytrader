// ML and Prediction types

export type ModelType = 'lightgbm' | 'lstm' | 'xgboost' | 'random_forest';

export type PredictionTarget = '1d_return' | '3d_return' | '7d_return' | 'trend' | 'volatility';

export interface ModelTrainingParams {
  stockCode: string;
  startDate: string;
  endDate: string;
  target: PredictionTarget;
  modelType: ModelType;
  features?: string[];
  testSize?: number;
  validationSplit?: number;
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
}

export interface PredictionResult {
  stockCode: string;
  stockName: string;
  date: string;
  prediction: number;
  confidence: number;
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
