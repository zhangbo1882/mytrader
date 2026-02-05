import { useCallback } from 'react';
import { useBoardStore } from '@/stores';
import { boardService } from '@/services';

export function useBoardData() {
  const { boards, selectedBoard, loading, error, setBoards, setSelectedBoard, setLoading, setError } =
    useBoardStore();

  const fetchBoards = useCallback(async () => {
    setLoading(true);
    setError(null);

    try {
      const result = await boardService.list();
      const boardList = result.boards || result;
      setBoards(boardList);
    } catch (err) {
      const message = err instanceof Error ? err.message : '获取板块列表失败';
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [setLoading, setError, setBoards]);

  const fetchBoardDetail = useCallback(
    async (code: string) => {
      setLoading(true);
      setError(null);

      try {
        const detail = await boardService.getDetail(code);
        setSelectedBoard(detail);
      } catch (err) {
        const message = err instanceof Error ? err.message : '获取板块详情失败';
        setError(message);
      } finally {
        setLoading(false);
      }
    },
    [setLoading, setError, setSelectedBoard]
  );

  const searchBoards = useCallback(
    async (query: string) => {
      if (!query) {
        fetchBoards();
        return;
      }

      setLoading(true);
      setError(null);

      try {
        const result = await boardService.search(query);
        const boardList = result.boards || result;
        setBoards(boardList);
      } catch (err) {
        const message = err instanceof Error ? err.message : '搜索板块失败';
        setError(message);
      } finally {
        setLoading(false);
      }
    },
    [fetchBoards, setLoading, setError, setBoards]
  );

  return {
    boards,
    selectedBoard,
    loading,
    error,
    fetchBoards,
    fetchBoardDetail,
    searchBoards,
  };
}
