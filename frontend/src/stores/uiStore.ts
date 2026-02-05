import { create } from 'zustand';
import { persist } from 'zustand/middleware';

type TabKey =
  | 'query'
  | 'ai-screen'
  | 'prediction'
  | 'favorites'
  | 'update'
  | 'tasks'
  | 'financial'
  | 'boards';

interface UIState {
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
