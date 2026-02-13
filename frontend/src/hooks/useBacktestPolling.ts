import { useState, useEffect, useCallback, useRef } from 'react';
import { backtestService } from '@/services';
import type { BacktestTaskStatus, BacktestResult } from '@/types';

interface UseBacktestPollingResult {
  status: BacktestTaskStatus | null;
  result: BacktestResult | null;
  error: string | null;
  isPolling: boolean;
  stopPolling: () => void;
}

const POLLING_INTERVAL = 3000; // 3 seconds
const MAX_ERRORS = 3;

export function useBacktestPolling(taskId: string | null): UseBacktestPollingResult {
  const [status, setStatus] = useState<BacktestTaskStatus | null>(null);
  const [result, setResult] = useState<BacktestResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPolling, setIsPolling] = useState(false);
  const errorCountRef = useRef(0);
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>();

  const stopPolling = useCallback(() => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = undefined;
    }
    setIsPolling(false);
  }, []);

  useEffect(() => {
    if (!taskId) return;

    setIsPolling(true);
    errorCountRef.current = 0;
    setError(null);

    const poll = async () => {
      try {
        const statusRes = await backtestService.getStatus(taskId) as any;
        setStatus(statusRes);

        if (statusRes.status === 'completed') {
          // Get full result
          const resultRes = await backtestService.getResult(taskId) as any;
          setResult(resultRes.result);
          setIsPolling(false);
          return;
        }

        if (statusRes.status === 'failed') {
          setError(statusRes.message || '回测失败');
          setIsPolling(false);
          return;
        }

        // Continue polling
        timeoutRef.current = setTimeout(poll, POLLING_INTERVAL);
      } catch (err) {
        errorCountRef.current++;
        if (errorCountRef.current >= MAX_ERRORS) {
          setError(err instanceof Error ? err.message : '轮询失败');
          setIsPolling(false);
        } else {
          timeoutRef.current = setTimeout(poll, POLLING_INTERVAL);
        }
      }
    };

    poll();

    return () => stopPolling();
  }, [taskId, stopPolling]);

  return { status, result, error, isPolling, stopPolling };
}
