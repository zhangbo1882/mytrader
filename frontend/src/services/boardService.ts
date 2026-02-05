import api from './api';

export interface Board {
  code: string;
  name: string;
  category?: string;
  stockCount?: number;
  valuation?: {
    pe?: number;
    pb?: number;
    marketCap?: number;
  };
  description?: string;
}

export interface BoardDetail extends Board {
  stocks: BoardStock[];
}

export interface BoardStock {
  code: string;
  name: string;
  marketCap?: number;
  changePercent?: number;
}

export const boardService = {
  // 获取板块列表
  list: async () => {
    return api.get<{ boards: Board[] }>('/boards');
  },

  // 搜索板块
  search: async (query: string) => {
    const encoded = encodeURIComponent(query);
    return api.get<{ boards: Board[] }>(`/boards/search?q=${encoded}`);
  },

  // 获取板块详情
  getDetail: async (code: string) => {
    return api.get<{ board: BoardDetail }>(`/boards/${code}`);
  },

  // 获取板块成分股
  getConstituents: async (code: string) => {
    return api.get<{ board: BoardDetail }>(`/boards/${code}/constituents`);
  },
};
