import api from './api';

export interface FavoriteItem {
  id: number;
  stock_code: string;
  stock_name: string;
  added_at: string;
  notes: string | null;
}

export interface FavoriteListResponse {
  favorites: FavoriteItem[];
  total: number;
}

export interface FavoriteCheckResponse {
  is_favorite: boolean;
  favorite: FavoriteItem | null;
}

export interface BatchAddResult {
  stock_code: string;
  stock_name?: string;
  success: boolean;
  error?: string;
}

export interface BatchAddResponse {
  success: number;
  failed: number;
  total: number;
  results: BatchAddResult[];
}

export const favoriteService = {
  /**
   * 获取收藏列表
   */
  list: (userId = 'default') =>
    api.get<FavoriteListResponse>('/favorites', { params: { user_id: userId } }),

  /**
   * 添加收藏
   */
  add: (stockCode: string, notes = '') =>
    api.post<FavoriteItem>('/favorites', { stock_code: stockCode, notes }, {
      params: { user_id: 'default' }
    }),

  /**
   * 删除收藏
   */
  remove: (stockCode: string, userId = 'default') =>
    api.delete<{ message: string }>(`/favorites/${stockCode}`, {
      params: { user_id: userId }
    }),

  /**
   * 检查是否已收藏
   */
  check: (stockCode: string, userId = 'default') =>
    api.get<FavoriteCheckResponse>(`/favorites/check/${stockCode}`, {
      params: { user_id: userId }
    }),

  /**
   * 批量添加收藏
   */
  batchAdd: (stockCodes: string[], notes = '') =>
    api.post<BatchAddResponse>('/favorites/batch', { stock_codes: stockCodes, notes }, {
      params: { user_id: 'default' }
    }),

  /**
   * 清空收藏
   */
  clear: (userId = 'default') =>
    api.delete<{ message: string }>('/favorites/clear', {
      params: { user_id: userId }
    }),
};
