import { useCallback, useState } from 'react';
import { financialService } from '@/services';
import type { FinancialSummary, FinancialReport } from '@/types';

export function useFinancialData() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [summary, setSummary] = useState<FinancialSummary | null>(null);
  const [report, setReport] = useState<FinancialReport | null>(null);

  const fetchSummary = useCallback(async (stockCode: string) => {
    setLoading(true);
    setError(null);

    try {
      const data = await financialService.getSummary(stockCode);
      setSummary(data);
      return data;
    } catch (err) {
      const message = err instanceof Error ? err.message : '获取财务摘要失败';
      setError(message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchReport = useCallback(async (stockCode: string) => {
    setLoading(true);
    setError(null);

    try {
      const data = await financialService.getReport(stockCode);
      setReport(data);
      return data;
    } catch (err) {
      const message = err instanceof Error ? err.message : '获取财务报表失败';
      setError(message);
      throw err;
    } finally {
      setLoading(false);
    }
  }, []);

  const exportData = useCallback(
    async (stockCode: string, format: 'csv' | 'excel') => {
      try {
        const blob = await financialService.export(stockCode, format);

        // Create download link
        const url = window.URL.createObjectURL(new Blob([blob]));
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', `${stockCode}_financial.${format === 'csv' ? 'csv' : 'xlsx'}`);
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.URL.revokeObjectURL(url);
      } catch (err) {
        const message = err instanceof Error ? err.message : '导出失败';
        setError(message);
        throw err;
      }
    },
    []
  );

  return {
    loading,
    error,
    summary,
    report,
    fetchSummary,
    fetchReport,
    exportData,
  };
}
