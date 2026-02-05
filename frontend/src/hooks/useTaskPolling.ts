import { useEffect, useRef, useCallback } from 'react';
import { useTaskStore } from '@/stores';
import { taskService } from '@/services';
import { POLLING_INTERVAL } from '@/utils';

export function useTaskPolling(interval: number = POLLING_INTERVAL) {
  const { setTasks, runningTask } = useTaskStore();
  const intervalRef = useRef<number>();
  const isPollingRef = useRef(false);
  const errorCountRef = useRef(0);
  const MAX_ERRORS = 5;

  const poll = useCallback(async () => {
    try {
      const result = await taskService.list();
      const tasks = result.tasks || result;

      // Check if there are running tasks
      const hasRunningTasks = tasks.some((t: any) => t.status === 'running');

      if (hasRunningTasks || isPollingRef.current) {
        isPollingRef.current = hasRunningTasks;
        setTasks(tasks);
      }

      // Reset error count on success
      errorCountRef.current = 0;
    } catch (error) {
      errorCountRef.current += 1;
      console.error('Polling error:', error, `(${errorCountRef.current}/${MAX_ERRORS})`);

      // Stop polling after max errors
      if (errorCountRef.current >= MAX_ERRORS) {
        console.warn('Too many polling errors, stopping polling');
        if (intervalRef.current) {
          clearInterval(intervalRef.current);
          intervalRef.current = undefined;
        }
      }
    }
  }, [setTasks]);

  useEffect(() => {
    // Initial poll
    poll();

    // Start polling
    intervalRef.current = window.setInterval(poll, interval);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
    };
  }, [poll, interval]);

  const startPolling = useCallback(() => {
    if (!intervalRef.current) {
      errorCountRef.current = 0;
      poll();
      intervalRef.current = window.setInterval(poll, interval);
      isPollingRef.current = true;
    }
  }, [poll, interval]);

  const stopPolling = useCallback(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = undefined;
      isPollingRef.current = false;
    }
  }, []);

  return {
    poll,
    startPolling,
    stopPolling,
    isRunning: !!runningTask,
  };
}
