import { create } from 'zustand';
import type { Stock } from '@/types';
import { favoriteService, type FavoriteItem } from '@/services/favoriteService';

export interface FavoriteStock extends Stock {
  addedAt: string;
}

interface FavoriteState {
  favorites: FavoriteStock[];
  loading: boolean;
  error: string | null;

  loadFavorites: () => Promise<void>;
  addFavorite: (code: string, name: string) => Promise<void>;
  removeFavorite: (code: string) => Promise<void>;
  isInFavorites: (code: string) => boolean;
  clearFavorites: () => Promise<void>;
  setFavorites: (favorites: FavoriteStock[]) => void;
}

// Helper function to convert API format to store format
const toFavoriteStock = (item: FavoriteItem): FavoriteStock => ({
  code: item.stock_code,
  name: item.stock_name,
  addedAt: item.added_at,
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

  addFavorite: async (code, name) => {
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

  setFavorites: (favorites) => set({ favorites }),
}));
