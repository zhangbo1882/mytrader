import { BrowserRouter, Routes, Route, Navigate, Outlet } from 'react-router-dom';
import { lazy, Suspense } from 'react';
import { Spin } from 'antd';
import {
  LineChartOutlined,
  RobotOutlined,
  BulbOutlined,
  StarOutlined,
  SyncOutlined,
  HistoryOutlined,
  DollarOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  ExperimentOutlined,
  FundOutlined,
  FilterOutlined,
  TransactionOutlined,
  TrophyOutlined,
  ImportOutlined,
  SafetyOutlined,
} from '@ant-design/icons';
import { Layout, Menu, theme } from 'antd';
import { useNavigate, useLocation } from 'react-router-dom';
import { create } from 'zustand';
import { persist } from 'zustand/middleware';

const { Header, Sider, Content } = Layout;

// Loading fallback for lazy-loaded components
const PageLoader = () => (
  <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: '400px' }}>
    <Spin size="large" tip="加载中..." />
  </div>
);

// Lazy load pages
const QueryPage = lazy(() => import('./pages/QueryPage'));
const AIScreenPage = lazy(() => import('./pages/AIScreenPage'));
const PredictionPage = lazy(() => import('./pages/PredictionPage'));
const FavoritesPage = lazy(() => import('./pages/FavoritesPage'));
const UpdatePage = lazy(() => import('./pages/UpdatePage'));
const TasksPage = lazy(() => import('./pages/TasksPage'));
const FinancialPage = lazy(() => import('./pages/FinancialPage'));
const BacktestPage = lazy(() => import('./pages/BacktestPage'));
const ValuationPage = lazy(() => import('./pages/ValuationPage'));
const ScreeningPage = lazy(() => import('./pages/ScreeningPage'));
const MoneyFlowPage = lazy(() => import('./pages/MoneyFlowPage'));
const DragonListPage = lazy(() => import('./pages/DragonListPage'));
const DataImportPage = lazy(() => import('./pages/DataImportPage'));
const RiskManagementPage = lazy(() => import('./pages/RiskManagementPage'));

type MenuItem = {
  key: string;
  icon: React.ReactNode;
  label: string;
};

// Simple UI store
const useUIStore = create()(
  persist(
    (set: any) => ({
      activeTab: 'query',
      sidebarCollapsed: false,
      setActiveTab: (tab: string) => set({ activeTab: tab }),
      toggleSidebar: () => set((state: any) => ({ sidebarCollapsed: !state.sidebarCollapsed })),
    }),
    { name: 'mytrader-ui' }
  )
);

function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const { sidebarCollapsed, toggleSidebar } = useUIStore();
  const { token } = theme.useToken();

  const menuItems: MenuItem[] = [
    { key: '/query', icon: <LineChartOutlined aria-hidden="true" />, label: '股票查询' },
    { key: '/ai-screen', icon: <RobotOutlined aria-hidden="true" />, label: 'AI智能筛选' },
    { key: '/prediction', icon: <BulbOutlined aria-hidden="true" />, label: 'AI预测' },
    { key: '/favorites', icon: <StarOutlined aria-hidden="true" />, label: '我的收藏' },
    { key: '/data-import', icon: <ImportOutlined aria-hidden="true" />, label: '数据导入' },
    { key: '/update', icon: <SyncOutlined aria-hidden="true" />, label: '更新管理' },
    { key: '/tasks', icon: <HistoryOutlined aria-hidden="true" />, label: '任务历史' },
    { key: '/financial', icon: <DollarOutlined aria-hidden="true" />, label: '财务数据' },
    { key: '/valuation', icon: <FundOutlined aria-hidden="true" />, label: '股票估值' },
    { key: '/backtest', icon: <ExperimentOutlined aria-hidden="true" />, label: '策略回测' },
    { key: '/screening', icon: <FilterOutlined aria-hidden="true" />, label: '股票筛选' },
    { key: '/moneyflow', icon: <TransactionOutlined aria-hidden="true" />, label: '资金流向' },
    { key: '/dragon-list', icon: <TrophyOutlined aria-hidden="true" />, label: '龙虎榜' },
    { key: '/risk', icon: <SafetyOutlined aria-hidden="true" />, label: '风险管理' },
  ];

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider trigger={null} collapsible collapsed={sidebarCollapsed}>
        <div style={{
          height: 32,
          margin: 16,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: '#fff',
          fontSize: '18px',
          fontWeight: 'bold',
        }}>
          {sidebarCollapsed ? 'MT' : 'MyTrader'}
        </div>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
        />
      </Sider>
      <Layout>
        <Header style={{
          padding: '0 24px',
          background: token.colorBgContainer,
          display: 'flex',
          alignItems: 'center',
        }}>
          <div
            onClick={toggleSidebar}
            style={{ fontSize: '16px', cursor: 'pointer' }}
          >
            {sidebarCollapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
          </div>
          <div style={{ fontSize: '14px', color: '#666', marginLeft: 16 }}>智能股票分析系统</div>
        </Header>
        <Content style={{
          margin: '24px 16px',
          padding: 24,
          minHeight: 280,
          background: token.colorBgContainer,
          borderRadius: token.borderRadiusLG,
        }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}

function App() {
  return (
    <BrowserRouter>
      <Suspense fallback={<PageLoader />}>
        <Routes>
          <Route path="/" element={<AppLayout />}>
            <Route index element={<Navigate to="/query" replace />} />
            <Route path="query" element={<QueryPage />} />
            <Route path="ai-screen" element={<AIScreenPage />} />
            <Route path="prediction" element={<PredictionPage />} />
            <Route path="favorites" element={<FavoritesPage />} />
            <Route path="data-import" element={<DataImportPage />} />
            <Route path="update" element={<UpdatePage />} />
            <Route path="tasks" element={<TasksPage />} />
            <Route path="financial" element={<FinancialPage />} />
            <Route path="valuation" element={<ValuationPage />} />
            <Route path="backtest" element={<BacktestPage />} />
            <Route path="screening" element={<ScreeningPage />} />
            <Route path="moneyflow" element={<MoneyFlowPage />} />
            <Route path="dragon-list" element={<DragonListPage />} />
            <Route path="risk" element={<RiskManagementPage />} />
          </Route>
        </Routes>
      </Suspense>
    </BrowserRouter>
  );
}

export default App;
