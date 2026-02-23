import { useCallback } from 'react';
import { useQueryStore } from '@/stores';
import { stockService } from '@/services';
import type { QueryParams, StockData } from '@/types';
import { message } from 'antd';

// 检查是否为港股代码
function isHKStock(symbol: string): boolean {
  return symbol.endsWith('.HK') || /^\d{4,5}$/.test(symbol);
}

// 将后端字段映射到前端字段
function mapBackendToFrontend(data: Record<string, any[]> | { error?: string; message?: string }, params?: { priceType?: string }): Record<string, StockData[]> {
  // 检查是否有错误
  if ('error' in data || 'message' in data) {
    console.error('Backend returned error:', data);
    return {};
  }

  const mapped: Record<string, StockData[]> = {};

  for (const [symbol, records] of Object.entries(data)) {
    // 确保 records 是数组
    if (!Array.isArray(records)) {
      console.error(`Invalid data for ${symbol}: records is not an array`, records);
      mapped[symbol] = [];
      continue;
    }

    // 检查港股前复权提示
    if (isHKStock(symbol) && params?.priceType === 'qfq' && records.length > 0) {
      // 检查前复权价格是否等于原始价格（说明没有真实复权数据）
      const firstRecord = records[0];
      if (firstRecord.open === firstRecord.open_qfq &&
          firstRecord.close === firstRecord.close_qfq) {
        message.warning('港股暂不支持前复权数据，将显示原始价格', 3);
      }
    }

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
      turnoverRate: record.turnover_rate_f ?? record.turnoverRate,
      turnover_rate_f: record.turnover_rate_f,
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
      const mappedData = mapBackendToFrontend(data, { priceType: params.priceType });
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
