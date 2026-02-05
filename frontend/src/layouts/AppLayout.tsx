import { Outlet, useNavigate, useLocation } from 'react-router-dom';
import { Layout, Menu, theme } from 'antd';
import {
  LineChartOutlined,
  RobotOutlined,
  BulbOutlined,
  StarOutlined,
  SyncOutlined,
  HistoryOutlined,
  DollarOutlined,
  AppstoreOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
} from '@ant-design/icons';
import { useUIStore } from '@/stores';
import type { MenuProps } from 'antd';

const { Header, Sider, Content } = Layout;

// Import global styles for skip-to-content link
import '@/styles/global.css';

type MenuItem = Required<MenuProps>['items'][number];

function getItem(
  label: string,
  key: string,
  icon: React.ReactNode,
  children?: MenuItem[]
): MenuItem {
  return {
    key,
    icon,
    children,
    label,
  } as MenuItem;
}

const menuItems: MenuItem[] = [
  getItem('股票查询', '/query', <LineChartOutlined aria-hidden="true" />),
  getItem('AI智能筛选', '/ai-screen', <RobotOutlined aria-hidden="true" />),
  getItem('AI预测', '/prediction', <BulbOutlined aria-hidden="true" />),
  getItem('我的收藏', '/favorites', <StarOutlined aria-hidden="true" />),
  getItem('更新管理', '/update', <SyncOutlined aria-hidden="true" />),
  getItem('任务历史', '/tasks', <HistoryOutlined aria-hidden="true" />),
  getItem('财务数据', '/financial', <DollarOutlined aria-hidden="true" />),
  getItem('板块中心', '/boards', <AppstoreOutlined aria-hidden="true" />),
];

function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const { sidebarCollapsed, toggleSidebar, setActiveTab } = useUIStore();

  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken();

  const handleMenuClick: MenuProps['onClick'] = (e) => {
    navigate(e.key);
    setActiveTab(e.key as any);
  };

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <a href="#main-content" className="skip-to-content">
        跳转到主内容
      </a>
      <Sider trigger={null} collapsible collapsed={sidebarCollapsed}>
        <h1
          style={{
            height: 32,
            margin: 16,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: '#fff',
            fontSize: '18px',
            fontWeight: 'bold',
          }}
        >
          <span>{sidebarCollapsed ? 'MT' : 'MyTrader'}</span>
        </h1>
        <Menu
          theme="dark"
          mode="inline"
          selectedKeys={[location.pathname]}
          items={menuItems}
          onClick={handleMenuClick}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            padding: '0 24px',
            background: colorBgContainer,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
          }}
        >
          <div
            onClick={toggleSidebar}
            style={{ fontSize: '16px', cursor: 'pointer', transition: 'color 0.3s' }}
            role="button"
            aria-label={sidebarCollapsed ? '展开侧边栏' : '收起侧边栏'}
            aria-expanded={sidebarCollapsed}
            tabIndex={0}
          >
            {sidebarCollapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
          </div>
          <div style={{ fontSize: '14px', color: '#666' }}>智能股票分析系统</div>
        </Header>
        <Content
          id="main-content"
          style={{
            margin: '24px 16px',
            padding: 24,
            minHeight: 280,
            background: colorBgContainer,
            borderRadius: borderRadiusLG,
          }}
        >
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}

export default AppLayout;
