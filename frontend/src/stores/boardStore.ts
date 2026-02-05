import { create } from 'zustand';
import type { Board, BoardDetail } from '@/types';

interface BoardState {
  boards: Board[];
  selectedBoard: BoardDetail | null;
  loading: boolean;
  error: string | null;

  setBoards: (boards: Board[]) => void;
  setSelectedBoard: (board: BoardDetail | null) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
}

export const useBoardStore = create<BoardState>((set) => ({
  boards: [],
  selectedBoard: null,
  loading: false,
  error: null,

  setBoards: (boards) => set({ boards }),
  setSelectedBoard: (selectedBoard) => set({ selectedBoard }),
  setLoading: (loading) => set({ loading }),
  setError: (error) => set({ error }),
}));
