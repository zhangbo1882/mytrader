import { create } from 'zustand';
import type { Stock, StockData, DateRange, PriceType } from '@/types';

interface QueryState {
  // Selected stocks
  symbols: Stock[];

  // Date range
  dateRange: DateRange;

  // Price type (qfq, hfq, none)
  priceType: PriceType;

  // Query results
  results: Record<string, StockData[]>;

  // Loading state
  loading: boolean;

  // Error state
  error: string | null;

  // Minimum available date
  minDate: string | null;

  // Actions
  setSymbols: (symbols: Stock[]) => void;
  setDateRange: (range: DateRange) => void;
  setPriceType: (type: PriceType) => void;
  setResults: (results: Record<string, StockData[]>) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setMinDate: (date: string) => void;
  clearResults: () => void;
}

export const useQueryStore = create<QueryState>((set) => ({
  // Initial state
  symbols: [],
  dateRange: { start: '', end: '' },
  priceType: 'bfq',
  results: {},
  loading: false,
  error: null,
  minDate: null,

  // Actions
  setSymbols: (symbols) => set({ symbols }),
  setDateRange: (dateRange) => set({ dateRange }),
  setPriceType: (priceType) => set({ priceType }),
  setResults: (results) => set({ results }),
  setLoading: (loading) => set({ loading }),
  setError: (error) => set({ error }),
  setMinDate: (minDate) => set({ minDate }),
  clearResults: () => set({ results: {}, error: null }),
}));
