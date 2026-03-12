import { useState, useCallback } from 'react';
import { valuationService } from '@/services';
import type {
  ValuationRequest,
  ValuationResult
} from '@/types';

interface UseValuationResult {
  result: ValuationResult | null;
  loading: boolean;
  error: string | null;
  fetchValuation: (request: ValuationRequest) => Promise<void>;
  clear: () => void;
}

export function useValuation(): UseValuationResult {
  const [result, setResult] = useState<ValuationResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchValuation = useCallback(async (request: ValuationRequest) => {
    setLoading(true);
    setError(null);

    try {
      const response = await valuationService.getSummary(
        request.symbol,
        {
          methods: request.methods,
          date: request.date,
          fiscal_date: request.fiscal_date,
          combine_method: request.combine_method,
          dcf_config: request.dcf_config
        }
      ) as any;

      if (response.success) {
        setResult(response.valuation);
      } else {
        setError(response.error || '估值失败');
      }
    } catch (err: any) {
      setError(err.message || '网络错误');
    } finally {
      setLoading(false);
    }
  }, []);

  const clear = useCallback(() => {
    setResult(null);
    setError(null);
  }, []);

  return { result, loading, error, fetchValuation, clear };
}
