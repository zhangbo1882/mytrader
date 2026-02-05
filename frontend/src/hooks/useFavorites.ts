import { useCallback } from 'react';
import { useFavoriteStore } from '@/stores';
import { stockService } from '@/services';
import type { QueryParams } from '@/types';

export function useFavorites() {
  const { favorites, addFavorite, removeFavorite, isInFavorites } = useFavoriteStore();

  const addAndQuery = useCallback(
    async (code: string, name: string) => {
      // Get stock name if not provided
      if (!name) {
        try {
          const result = await stockService.getName(code);
          name = result.name;
        } catch (error) {
          console.error('Failed to get stock name:', error);
          name = code;
        }
      }

      addFavorite(code, name);
    },
    [addFavorite]
  );

  const batchAdd = useCallback(
    (stocks: Array<{ code: string; name: string }>) => {
      stocks.forEach((stock) => {
        if (!isInFavorites(stock.code)) {
          addFavorite(stock.code, stock.name);
        }
      });
    },
    [addFavorite, isInFavorites]
  );

  const getQueryParams = useCallback(
    (selectedCodes?: string[]): QueryParams => {
      const codes = selectedCodes || favorites.map((f) => f.code);
      return {
        symbols: codes,
        startDate: '',
        endDate: '',
        priceType: 'qfq',
      };
    },
    [favorites]
  );

  return {
    favorites,
    addFavorite: addAndQuery,
    removeFavorite,
    isInFavorites,
    batchAdd,
    getQueryParams,
  };
}
