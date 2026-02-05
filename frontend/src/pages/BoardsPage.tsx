import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Card,
  Input,
  Row,
  Col,
  Button,
  Typography,
  Divider,
  Space,
  Tag,
  Empty,
  message,
  Alert,
} from 'antd';
import {
  AppstoreOutlined,
  SearchOutlined,
  FilterOutlined,
  ReloadOutlined,
  BarChartOutlined,
} from '@ant-design/icons';
import { BoardCard } from '@/components/boards/BoardCard';
import { ConstituentsModal } from '@/components/boards/ConstituentsModal';
import { useBoardData } from '@/hooks';
import { useQueryStore } from '@/stores';
import type { BoardDetail } from '@/types';

const { Title, Text } = Typography;
const { Search } = Input;

function BoardsPage() {
  const navigate = useNavigate();
  const [searchText, setSearchText] = useState('');
  const [selectedBoard, setSelectedBoard] = useState<BoardDetail | null>(null);
  const [modalVisible, setModalVisible] = useState(false);

  const { boards, loading, error, fetchBoards, fetchBoardDetail } = useBoardData();
  const { setSymbols, setDateRange } = useQueryStore();

  // 初始化加载板块列表
  useEffect(() => {
    fetchBoards();
  }, []);

  // 搜索板块
  const handleSearch = () => {
    if (searchText.trim()) {
      fetchBoards(searchText.trim());
    } else {
      fetchBoards();
    }
  };

  // 点击板块卡片
  const handleBoardClick = async (boardCode: string) => {
    try {
      const detail = await fetchBoardDetail(boardCode);
      setSelectedBoard(detail);
      setModalVisible(true);
    } catch (error) {
      message.error('加载板块详情失败');
    }
  };

  // 查看股票详情（跳转到查询页面）
  const handleViewStock = (stockCode: string) => {
    // 设置到queryStore
    setSymbols([{ code: stockCode, name: stockCode }]);
    setDateRange({
      start: '',
      end: '',
    });

    // 导航到查询页面
    navigate('/query');
    message.success(`已跳转到查询页面，请选择日期范围后查询`);
  };

  // 批量查询（从板块成分股中选择）
  const handleBatchQuery = (stockCodes: string[]) => {
    const stocks = stockCodes.map((code) => {
      const stock = selectedBoard?.stocks.find((s) => s.code === code);
      return { code: code, name: stock?.name || code };
    });

    setSymbols(stocks);
    navigate('/query');
    message.success(`已选择 ${stockCodes.length} 只股票，请选择日期范围后查询`);
  };

  // 过滤板块
  const filteredBoards = boards.filter((board) => {
    if (!searchText) return true;
    const search = searchText.toLowerCase();
    return (
      board.name.toLowerCase().includes(search) ||
      board.code.toLowerCase().includes(search) ||
      (board.description && board.description.toLowerCase().includes(search))
    );
  });

  // 板块分类标签
  const categoryTags = Array.from(new Set(boards.map((b) => b.category || '其他'))).filter(Boolean);

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>
        <AppstoreOutlined style={{ marginRight: 8 }} aria-hidden="true" />
        板块中心
      </Title>
      <Text type="secondary">查看行业板块成分股，分析板块估值数据</Text>

      <Divider />

      {/* 搜索和筛选 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space wrap>
          <Search
            placeholder="搜索板块名称或代码（如：金融、地产、科技）"
            value={searchText}
            onChange={(e) => setSearchText(e.target.value)}
            onSearch={handleSearch}
            enterButton
            style={{ width: 300 }}
            allowClear
          />
          <Button icon={<FilterOutlined aria-hidden="true" />}>筛选</Button>
          <Button icon={<ReloadOutlined aria-hidden="true" />} onClick={() => fetchBoards()}>
            刷新
          </Button>
        </Space>

        {/* 分类标签 */}
        {categoryTags.length > 0 && (
          <div style={{ marginTop: 12 }}>
            <Space wrap>
              <Text type="secondary">快速筛选：</Text>
              <Tag onClick={() => setSearchText('金融')}>金融</Tag>
              <Tag onClick={() => setSearchText('地产')}>地产</Tag>
              <Tag onClick={() => setSearchText('科技')}>科技</Tag>
              <Tag onClick={() => setSearchText('医药')}>医药</Tag>
              <Tag onClick={() => setSearchText('消费')}>消费</Tag>
              <Tag onClick={() => setSearchText('能源')}>能源</Tag>
            </Space>
          </div>
        )}
      </Card>

      {/* 统计信息 */}
      {boards.length > 0 && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Space size="large">
            <div>
              <Text type="secondary">板块总数</Text>
              <div style={{ fontSize: 20, fontWeight: 'bold' }}>{boards.length}</div>
            </div>
            <div>
              <Text type="secondary">搜索结果</Text>
              <div style={{ fontSize: 20, fontWeight: 'bold', color: '#1890ff' }}>
                {filteredBoards.length}
              </div>
            </div>
          </Space>
        </Card>
      )}

      {/* 错误提示 */}
      {error && (
        <Alert
          message="加载失败"
          description={error}
          type="error"
          showIcon
          closable
          style={{ marginBottom: 16 }}
        />
      )}

      {/* 板块列表 */}
      {loading ? (
        <Card>
          <div style={{ textAlign: 'center', padding: '60px 0' }}>加载中...</div>
        </Card>
      ) : boards.length === 0 ? (
        <Card>
          <Empty description="暂无板块数据" />
        </Card>
      ) : (
        <Row gutter={[16, 16]}>
          {filteredBoards.map((board) => (
            <Col key={board.code} xs={24} sm={12} md={8} lg={6}>
              <BoardCard board={board} onClick={() => handleBoardClick(board.code)} />
            </Col>
          ))}
        </Row>
      )}

      {/* 成分股对话框 */}
      <ConstituentsModal
        visible={modalVisible}
        board={selectedBoard}
        onClose={() => {
          setModalVisible(false);
          setSelectedBoard(null);
        }}
        onViewStock={handleViewStock}
        onBatchQuery={handleBatchQuery}
      />

      {/* 使用说明 */}
      {!loading && boards.length === 0 && (
        <Card title={<Text strong>板块中心功能</Text>} style={{ marginTop: 16 }}>
          <div style={{ lineHeight: 1.8 }}>
            <p>
              <Text strong>板块功能包括：</Text>
            </p>
            <ul style={{ paddingLeft: 20 }}>
              <li>浏览所有行业板块，查看板块基本信息</li>
              <li>查看板块成分股列表，支持搜索和筛选</li>
              <li>查看板块估值指标（市盈率、市净率、市值等）</li>
              <li>一键跳转到查询页面，查询板块成分股数据</li>
              <li>批量选择成分股进行对比分析</li>
            </ul>

            <p style={{ marginTop: 16 }}>
              <Text strong>使用方法：</Text>
            </p>
            <ul style={{ paddingLeft: 20 }}>
              <li>点击板块卡片查看成分股列表</li>
              <li>在成分股列表中选择感兴趣的股票</li>
              <li>点击"查询选中股票"跳转到查询页面</li>
              <li>或点击单只股票的"查看详情"直接查看</li>
            </ul>

            <p style={{ marginTop: 16 }}>
              <Text strong>板块分类：</Text>
            </p>
            <Text type="secondary">
              包括金融、地产、科技、医药、消费、能源、工业、材料、公用事业、电信等主要行业
            </Text>
          </div>
        </Card>
      )}
    </div>
  );
}

export default BoardsPage;
