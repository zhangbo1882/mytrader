import { useCallback } from 'react';
import { useQueryStore } from '@/stores';
import { stockService } from '@/services';
import type { QueryParams, StockData } from '@/types';

// 将后端字段映射到前端字段
function mapBackendToFrontend(data: Record<string, any[]>): Record<string, StockData[]> {
  const mapped: Record<string, StockData[]> = {};

  for (const [symbol, records] of Object.entries(data)) {
    mapped[symbol] = records.map((record: any) => ({
      date: record.datetime || record.date,
      open: record.open,
      high: record.high,
      low: record.low,
      close: record.close,
      volume: record.volume,
      amount: record.amount || record.turnover,
      turnover: record.turnover,
      pct_chg: record.pct_chg,
      change: record.change,
      changePercent: record.pct_chg ?? record.changePercent,
    }));
  }

  return mapped;
}

export function useQuery() {
  const { loading, error, results, setLoading, setError, setResults, clearResults } = useQueryStore();

  const executeQuery = useCallback(async (params: QueryParams) => {
    setLoading(true);
    setError(null);
    clearResults();

    try {
      const data = await stockService.query(params);
      const mappedData = mapBackendToFrontend(data);
      setResults(mappedData);
      return mappedData;
    } catch (err) {
      const message = err instanceof Error ? err.message : '查询失败';
      setError(message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, [setLoading, setError, setResults, clearResults]);

  return {
    executeQuery,
    loading,
    error,
    results,
  };
}
