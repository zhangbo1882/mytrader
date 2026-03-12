import { create } from 'zustand';
import { persist } from 'zustand/middleware';

export type TabKey =
  | 'query'
  | 'ai-screen'
  | 'prediction'
  | 'favorites'
  | 'data-import'
  | 'update'
  | 'tasks'
  | 'financial'
  | 'valuation'
  | 'backtest'
  | 'screening'
  | 'moneyflow'
  | 'dragon-list'
  | 'risk'
  | 'boards';

export interface UIState {
  activeTab: TabKey;
  sidebarCollapsed: boolean;

  setActiveTab: (tab: TabKey) => void;
  toggleSidebar: () => void;
  setSidebarCollapsed: (collapsed: boolean) => void;
}

export const useUIStore = create<UIState>()(
  persist(
    (set) => ({
      activeTab: 'query',
      sidebarCollapsed: false,

      setActiveTab: (activeTab) => set({ activeTab }),
      toggleSidebar: () => set((state) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
      setSidebarCollapsed: (sidebarCollapsed) => set({ sidebarCollapsed }),
    }),
    {
      name: 'mytrader-ui',
    }
  )
);
