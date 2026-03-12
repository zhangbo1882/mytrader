import api from './api';

export interface FavoriteItem {
  id: number;
  stock_code: string;
  stock_name: string;
  added_at: string;
  notes: string | null;
  safety_rating: string | null;
  fundamental_rating: string | null;
  entry_price: number | null;
  urgency: number | null;
  sw_l1?: string;
  sw_l2?: string;
  sw_l3?: string;
  fair_value?: number | null;
  upside_downside?: number | null;
  valuation_date?: string | null;
  valuation_confidence?: number | null;
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
  skipped?: boolean;
  updated?: boolean;
}

export interface BatchAddResponse {
  success: number;
  updated?: number;
  failed: number;
  skipped?: number;
  total: number;
  results: BatchAddResult[];
}

export interface UpdateFavoriteData {
  safety_rating?: string;
  fundamental_rating?: string;
  entry_price?: number;
  urgency?: number;
  notes?: string;
  fair_value?: number | null;
  upside_downside?: number | null;
  valuation_date?: string | null;
  valuation_confidence?: number | null;
}

export interface StockImportData {
  code: string;
  safety_rating?: string;
  fundamental_rating?: string;
  entry_price?: number;
  urgency?: number;
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
  add: (stockCode: string, notes = '', data?: UpdateFavoriteData) =>
    api.post<FavoriteItem>('/favorites', {
      stock_code: stockCode,
      notes,
      ...data
    }, {
      params: { user_id: 'default' }
    }),

  /**
   * 更新收藏字段
   */
  update: (stockCode: string, data: UpdateFavoriteData) =>
    api.put<FavoriteItem>(`/favorites/${stockCode}`, data, {
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
   * 批量添加收藏（支持新格式，包含额外字段）
   */
  batchAdd: (stocks: StockImportData[] | string[], notes = '') => {
    // 兼容旧格式：如果传入的是字符串数组，转换为新格式
    const stocksData = typeof stocks[0] === 'string'
      ? (stocks as string[]).map(code => ({ code }))
      : stocks as StockImportData[];

    return api.post<BatchAddResponse>('/favorites/batch', {
      stocks_data: stocksData,
      notes
    }, {
      params: { user_id: 'default' }
    });
  },

  /**
   * 清空收藏
   */
  clear: (userId = 'default') =>
    api.delete<{ message: string }>('/favorites/clear', {
      params: { user_id: userId }
    }),

};
