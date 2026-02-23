import { create } from 'zustand';
import type { Stock, StockData, DateRange, PriceType, IntervalType } from '@/types';

interface QueryState {
  // Selected stocks
  symbols: Stock[];

  // Date range
  dateRange: DateRange;

  // Price type (qfq, hfq, bfq)
  priceType: PriceType;

  // Time interval (1d, 5m, 15m, 30m, 60m)
  interval: IntervalType;

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
  setInterval: (interval: IntervalType) => void;
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
  interval: '1d',
  results: {},
  loading: false,
  error: null,
  minDate: null,

  // Actions
  setSymbols: (symbols) => set({ symbols }),
  setDateRange: (dateRange) => set({ dateRange }),
  setPriceType: (priceType) => set({ priceType }),
  setInterval: (interval) => set({ interval }),
  setResults: (results) => set({ results }),
  setLoading: (loading) => set({ loading }),
  setError: (error) => set({ error }),
  setMinDate: (minDate) => set({ minDate }),
  clearResults: () => set({ results: {}, error: null }),
}));
