import api from './api';
import type { MLModel, ModelTrainingParams, PredictionResult, ModelPerformance } from '@/types';

// 目标类型映射：前端的 target 映射到后端的 target_type
const targetTypeMap: Record<string, string> = {
  '1d_return': 'return_1d',
  '5d_return': 'return_5d',
  'trend': 'direction_1d',
  'trend_5d': 'direction_5d',
  'volatility': 'high_low_5d',
};

export const mlService = {
  // Train a new model
  train: (params: ModelTrainingParams) => {
    // 转换字段名以匹配后端 API
    const requestData = {
      symbol: params.stockCode,
      start_date: params.startDate,
      end_date: params.endDate,
      target_type: targetTypeMap[params.target] || params.target,
      model_type: params.modelType,
      features: params.features,
      walk_forward: params.walkForward ?? false,
      n_splits: params.nSplits ?? 5,
    };
    return api.post<{ taskId: string }>('/ml/train', requestData);
  },

  // List all models
  listModels: async () => {
    const response = await api.get<{ models: any[] }>('/ml/models');
    // 转换后端数据格式到前端格式
    const models: MLModel[] = response.models.map((model: any) => {
      // 从 metrics 中提取性能指标（如果有的话）
      const metrics = model.metrics || {};

      return {
        id: model.model_id,
        stockCode: model.symbol,
        stockName: model.symbol, // 后端没有返回 stockName，暂时使用 symbol
        modelType: model.model_type === 'lgb' ? 'lightgbm' : model.model_type,
        target: model.target_type === 'return_1d' ? '1d_return' :
                model.target_type === 'return_5d' ? '5d_return' :
                model.target_type === 'direction_1d' ? 'trend' :
                model.target_type === 'direction_5d' ? 'trend_5d' :
                model.target_type === 'high_low_5d' ? 'volatility' : model.target_type,
        startDate: model.training_start,
        endDate: model.training_end,
        features: model.feature_importance ? Object.keys(model.feature_importance) : [],
        accuracy: metrics.accuracy,
        precision: metrics.precision,
        recall: metrics.recall,
        f1Score: metrics.f1_score,
        createdAt: model.created_at,
        status: model.status === 'active' ? 'completed' :
                model.status === 'training' ? 'training' : 'failed',
        // 存储原始指标用于显示
        mae: metrics.mae,
        rmse: metrics.rmse,
        r2: metrics.r2,
        mape: metrics.mape,
        // Walk-forward 指标
        walkForward: model.metadata?.walk_forward ?? false,
        nSplits: model.metadata?.n_splits,
        cvScore: model.metadata?.cv_score,
        cvStd: model.metadata?.cv_std,
        cvMetric: model.metadata?.cv_metric,
      };
    });
    return { models };
  },

  // Get model by ID
  getModel: async (id: string) => {
    const response = await api.get<any>(`/ml/models/${id}`);
    const model = response.model;
    const metrics = model?.metrics || {};

    return {
      id: model.model_id,
      stockCode: model.symbol,
      stockName: model.symbol,
      modelType: model.model_type === 'lgb' ? 'lightgbm' : model.model_type,
      target: model.target_type === 'return_1d' ? '1d_return' :
              model.target_type === 'return_5d' ? '3d_return' :
              model.target_type === 'direction_1d' ? 'trend' :
              model.target_type === 'high_low_5d' ? 'volatility' : model.target_type,
      startDate: model.training_start,
      endDate: model.training_end,
      features: model.feature_importance ? Object.keys(model.feature_importance) : [],
      accuracy: metrics.accuracy,
      precision: metrics.precision,
      recall: metrics.recall,
      f1Score: metrics.f1_score,
      createdAt: model.created_at,
      status: model.status === 'active' ? 'completed' :
              model.status === 'training' ? 'training' : 'failed',
      // 回归指标
      mae: metrics.mae,
      rmse: metrics.rmse,
      r2: metrics.r2,
      mape: metrics.mape,
      // 特征重要性
      featureImportance: model.feature_importance ?
        Object.entries(model.feature_importance)
          .map(([feature, importance]) => ({ feature, importance: importance as number }))
          .sort((a, b) => b.importance - a.importance)
          .slice(0, 20) : [],
    };
  },

  // Delete model
  deleteModel: (id: string) => {
    return api.delete<{ success: boolean }>(`/ml/models/${id}`);
  },

  // Make prediction
  predict: async (modelId: string, stockCode: string, days: number = 30) => {
    const response = await api.post<any>('/ml/predict', {
      model_id: modelId,
      symbol: stockCode,
      days: days, // 请求历史数据用于对比图
    });

    // 处理历史预测数据
    const history: PredictionResult[] = [];
    if (response.history && Array.isArray(response.history)) {
      const isDirection = response.target_type?.includes('direction');

      for (const item of response.history) {
        // 为历史数据估算置信度
        let estimatedConfidence: number;
        if (isDirection) {
          // 分类任务：基于预测概率的置信度
          const prob = Math.abs(item.prediction - 0.5) * 2; // 0~0.5 转为 0~1
          estimatedConfidence = 0.5 + prob * 0.45; // 0.5~0.95
        } else {
          // 回归任务：基于预测值大小
          const predAbs = Math.abs(item.prediction);
          estimatedConfidence = Math.max(0.4, Math.min(0.95, 0.5 + predAbs * 5));
        }

        // 计算误差
        let error: number | undefined;
        if (item.actual !== null) {
          error = item.prediction - item.actual;
        }

        history.push({
          stockCode: response.symbol,
          stockName: response.symbol,
          date: item.date,
          prediction: item.prediction,
          actual: item.actual,
          confidence: estimatedConfidence,
          targetType: response.target_type,
          error: error,
        });
      }
    }

    // 当前预测结果
    // 置信度计算：基于预测值大小（模型对自己的预测值越有信心，置信度越高）
    const predAbs = Math.abs(response.prediction);
    const intervalWidth = Math.abs(response.confidence_upper - response.confidence_lower);

    // 综合考虑预测值大小和区间宽度
    // 预测值大 → 置信度高；区间窄 → 置信度高
    const confidenceFromPred = Math.min(0.95, 0.5 + predAbs * 5);  // 0.5 ~ 0.95
    const confidenceFromInterval = intervalWidth > 0
      ? Math.max(0.4, 1 - intervalWidth * 10)  // 区间越窄置信度越高
      : 0.5;

    // 取两者的平均值
    const confidence = (confidenceFromPred + confidenceFromInterval) / 2;

    const current: PredictionResult = {
      stockCode: response.symbol,
      stockName: response.symbol,
      date: response.date,
      prediction: response.prediction,
      confidence: Math.max(0.3, Math.min(0.95, confidence)),
      targetType: response.target_type,
    };

    // 返回历史数据和当前预测
    return {
      history,
      current,
    };
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
