import api from './api';
import type { MLModel, ModelTrainingParams, PredictionResult, ModelPerformance } from '@/types';

export const mlService = {
  // Train a new model
  train: (params: ModelTrainingParams) => {
    return api.post<{ taskId: string }>('/ml/train', params);
  },

  // List all models
  listModels: () => {
    return api.get<{ models: MLModel[] }>('/ml/models');
  },

  // Get model by ID
  getModel: (id: string) => {
    return api.get<MLModel>(`/ml/models/${id}`);
  },

  // Delete model
  deleteModel: (id: string) => {
    return api.delete<{ success: boolean }>(`/ml/models/${id}`);
  },

  // Make prediction
  predict: (modelId: string, stockCode: string, days: number = 1) => {
    return api.post<PredictionResult[]>('/ml/predict', {
      modelId,
      stockCode,
      days,
    });
  },

  // Get model performance
  getPerformance: (modelId: string) => {
    return api.get<ModelPerformance>(`/ml/models/${modelId}/performance`);
  },

  // Get training status
  getTrainingStatus: (taskId: string) => {
    return api.get<{ status: string; progress: number; result?: any }>(`/ml/training/${taskId}`);
  },
};
