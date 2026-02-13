import { api } from './index';
import type { DragonList, DragonListStats, DragonListSortBy } from '@/types/dragonlist.types';

export const dragonlistService = {
  // 查询龙虎榜数据
  query: (params?: {
    tradeDate?: string;
    startDate?: string;
    endDate?: string;
    tsCode?: string;
    reason?: string;
    limit?: number;
  }) =>
    api.get<{ success: boolean; data: DragonList[]; count: number }>('/dragon-list/query', {
      params: {
        trade_date: params?.tradeDate,
        start_date: params?.startDate,
        end_date: params?.endDate,
        ts_code: params?.tsCode,
        reason: params?.reason,
        limit: params?.limit || 100
      }
    }),

  // 获取指定股票的龙虎榜历史
  getByStock: (tsCode: string, limit = 50) =>
    api.get<{ success: boolean; data: DragonList[]; count: number }>(`/dragon-list/stock/${tsCode}`, {
      params: { limit }
    }),

  // 获取龙虎榜排名
  getTop: (tradeDate?: string, topN = 10, by: DragonListSortBy = 'net_amount') =>
    api.get<{ success: boolean; data: DragonList[]; count: number; trade_date: string }>('/dragon-list/top', {
      params: { trade_date: tradeDate, top_n: topN, by }
    }),

  // 获取统计数据
  getStats: (tradeDate?: string) =>
    api.get<{ success: boolean; data: DragonListStats }>('/dragon-list/stats', {
      params: { trade_date: tradeDate }
    }),

  // 创建更新任务
  update: (mode: 'incremental' | 'batch', startDate?: string, endDate?: string) =>
    api.post<{ success: boolean; task_id: string; message: string }>('/tasks/create', {
      task_type: 'update_dragon_list',
      params: {
        mode,
        start_date: startDate,
        end_date: endDate
      }
    })
};
