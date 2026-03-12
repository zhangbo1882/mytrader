import { create } from 'zustand';
import type { Stock } from '@/types';
import { favoriteService, type FavoriteItem, type BatchAddResponse, type UpdateFavoriteData } from '@/services/favoriteService';

export interface FavoriteStock extends Stock {
  addedAt: string;
  notes: string | null;
  safetyRating: string | null;
  fundamentalRating: string | null;
  entryPrice: number | null;
  urgency: number | null;
  swL1?: string;
  swL2?: string;
  swL3?: string;
  fairValue?: number | null;
  upsideDownside?: number | null;
  valuationDate?: string | null;
  valuationConfidence?: number | null;
}

interface FavoriteState {
  favorites: FavoriteStock[];
  loading: boolean;
  error: string | null;

  loadFavorites: () => Promise<void>;
  addFavorite: (code: string, name: string) => Promise<void>;
  updateFavorite: (code: string, data: UpdateFavoriteData) => Promise<void>;
  removeFavorite: (code: string) => Promise<void>;
  isInFavorites: (code: string) => boolean;
  clearFavorites: () => Promise<void>;
  setFavorites: (favorites: FavoriteStock[]) => void;
  batchAddFavorites: (stockCodes: string[]) => Promise<BatchAddResponse>;
}

// Helper function to convert API format to store format
const toFavoriteStock = (item: FavoriteItem): FavoriteStock => ({
  code: item.stock_code,
  name: item.stock_name,
  addedAt: item.added_at,
  notes: item.notes,
  safetyRating: item.safety_rating,
  fundamentalRating: item.fundamental_rating,
  entryPrice: item.entry_price,
  urgency: item.urgency,
  swL1: item.sw_l1,
  swL2: item.sw_l2,
  swL3: item.sw_l3,
  fairValue: item.fair_value,
  upsideDownside: item.upside_downside,
  valuationDate: item.valuation_date,
  valuationConfidence: item.valuation_confidence,
});

export const useFavoriteStore = create<FavoriteState>()((set, get) => ({
  favorites: [],
  loading: false,
  error: null,

  loadFavorites: async () => {
    set({ loading: true, error: null });
    try {
      const response = await favoriteService.list();
      const favorites = response.favorites.map(toFavoriteStock);
      set({ favorites, loading: false });
    } catch (error) {
      set({
        loading: false,
        error: error instanceof Error ? error.message : '加载收藏失败'
      });
    }
  },

  addFavorite: async (code, _name) => {
    // Optimistic update
    const existing = get().favorites.some((f) => f.code === code);
    if (existing) {
      return; // Already exists
    }

    set({ loading: true, error: null });
    try {
      await favoriteService.add(code);
      // Reload the list from server to get the correct data
      const response = await favoriteService.list();
      const favorites = response.favorites.map(toFavoriteStock);
      set({ favorites, loading: false });
    } catch (error) {
      set({
        loading: false,
        error: error instanceof Error ? error.message : '添加收藏失败'
      });
      throw error;
    }
  },

  updateFavorite: async (code, data) => {
    set({ loading: true, error: null });
    try {
      await favoriteService.update(code, data);
      // Update local state
      set((state) => ({
        favorites: state.favorites.map((f) =>
          f.code === code
            ? {
                ...f,
                notes: data.notes ?? f.notes,
                safetyRating: data.safety_rating ?? f.safetyRating,
                fundamentalRating: data.fundamental_rating ?? f.fundamentalRating,
                entryPrice: data.entry_price ?? f.entryPrice,
                urgency: data.urgency ?? f.urgency,
              }
            : f
        ),
        loading: false
      }));
    } catch (error) {
      set({
        loading: false,
        error: error instanceof Error ? error.message : '更新收藏失败'
      });
      throw error;
    }
  },

  removeFavorite: async (code) => {
    set({ loading: true, error: null });
    try {
      await favoriteService.remove(code);
      // Optimistic removal from state
      set((state) => ({
        favorites: state.favorites.filter((f) => f.code !== code),
        loading: false
      }));
    } catch (error) {
      set({
        loading: false,
        error: error instanceof Error ? error.message : '删除收藏失败'
      });
      throw error;
    }
  },

  isInFavorites: (code) => {
    return get().favorites.some((f) => f.code === code);
  },

  clearFavorites: async () => {
    set({ loading: true, error: null });
    try {
      await favoriteService.clear();
      set({ favorites: [], loading: false });
    } catch (error) {
      set({
        loading: false,
        error: error instanceof Error ? error.message : '清空收藏失败'
      });
      throw error;
    }
  },

  batchAddFavorites: async (stockCodes) => {
    if (stockCodes.length === 0) {
      throw new Error('股票代码列表不能为空');
    }

    set({ loading: true, error: null });
    try {
      const response = await favoriteService.batchAdd(stockCodes);
      const listResponse = await favoriteService.list();
      const favorites = listResponse.favorites.map(toFavoriteStock);
      set({ favorites, loading: false });
      return response;
    } catch (error) {
      set({
        loading: false,
        error: error instanceof Error ? error.message : '批量添加收藏失败'
      });
      throw error;
    }
  },

  setFavorites: (favorites) => set({ favorites }),
}));
