import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Card,
  Typography,
  Divider,
  Space,
  Button,
  Alert,
  Tabs,
  message,
  Popconfirm,
  Collapse,
  Tag,
} from 'antd';
import {
  StarOutlined,
  SearchOutlined,
  ClearOutlined,
  BarChartOutlined,
} from '@ant-design/icons';
import { FavoritesList } from '@/components/favorites/FavoritesList';
import { AddFavorite } from '@/components/favorites/AddFavorite';
import { ValuationParamsForm, DcfConfigForm } from '@/components/valuation';
import { useFavoriteStore } from '@/stores';
import { useQueryStore } from '@/stores';
import type { ValuationMethod, CombineMethod, DCFConfig } from '@/types';

const { Title, Text } = Typography;

const METHOD_LABELS: Record<ValuationMethod, string> = {
  combined: '相对估值',
  peg: 'PEG',
  dcf: 'DCF',
};

function FavoritesPage() {
  const navigate = useNavigate();
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);
  const [valuationMethods, setValuationMethods] = useState<ValuationMethod[]>(['combined', 'peg', 'dcf']);
  const [valuationDate, setValuationDate] = useState('');
  const [valuationFiscalDate, setValuationFiscalDate] = useState('');
  const [combineMethod, setCombineMethod] = useState<CombineMethod>('min_fair_value');
  const [dcfConfig, setDcfConfig] = useState<DCFConfig>({ risk_profile: 'conservative' });
  const { favorites, clearFavorites } = useFavoriteStore();
  const { setSymbols, setDateRange, setPriceType } = useQueryStore();

  // 批量查询选中的股票
  const handleBatchQuery = () => {
    if (selectedKeys.length === 0) {
      message.warning('请先选择要查询的股票');
      return;
    }

    const selectedStocks = favorites
      .filter((f) => selectedKeys.includes(f.code))
      .map((f) => ({ code: f.code, name: f.name }));

    setSymbols(selectedStocks);

    const endDate = new Date();
    const startDate = new Date();
    startDate.setMonth(startDate.getMonth() - 3);
    setDateRange({
      start: startDate.toISOString().split('T')[0],
      end: endDate.toISOString().split('T')[0]
    });

    setPriceType('bfq');
    navigate('/query');
    message.success(`已选择 ${selectedKeys.length} 只股票`);
  };

  // 查询所有收藏
  const handleQueryAll = () => {
    if (favorites.length === 0) {
      message.warning('收藏列表为空');
      return;
    }

    const allStocks = favorites.map((f) => ({ code: f.code, name: f.name }));
    setSymbols(allStocks);

    const endDate = new Date();
    const startDate = new Date();
    startDate.setMonth(startDate.getMonth() - 3);
    setDateRange({
      start: startDate.toISOString().split('T')[0],
      end: endDate.toISOString().split('T')[0]
    });

    setPriceType('bfq');
    navigate('/query');
    message.success(`已选择全部 ${favorites.length} 只股票`);
  };

  // 清空所有收藏
  const handleClearAll = () => {
    clearFavorites();
    message.success('已清空所有收藏');
    setSelectedKeys([]);
  };

  const valuationMethodSummary = valuationMethods.length > 0
    ? valuationMethods.map((method) => METHOD_LABELS[method]).join(' + ')
    : '未选择方法';

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>
        <StarOutlined style={{ marginRight: 8, color: '#faad14' }} aria-hidden="true" />
        我的收藏
      </Title>
      <Text type="secondary">管理收藏的股票，支持批量查询</Text>

      <Divider />

      {/* 统计信息 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space size="large">
          <div>
            <Text type="secondary">收藏总数</Text>
            <div style={{ fontSize: 24, fontWeight: 'bold', color: '#1890ff' }}>
              {favorites.length}
            </div>
          </div>
          <div>
            <Text type="secondary">已选择</Text>
            <div style={{ fontSize: 24, fontWeight: 'bold', color: '#52c41a' }}>
              {selectedKeys.length}
            </div>
          </div>
        </Space>
      </Card>

      {/* 操作按钮 */}
      {favorites.length > 0 && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Space wrap>
            <Button
              type="primary"
              icon={<BarChartOutlined aria-hidden="true" />}
              onClick={handleBatchQuery}
              disabled={selectedKeys.length === 0}
            >
              查询选中 ({selectedKeys.length})
            </Button>
            <Button icon={<SearchOutlined aria-hidden="true" />} onClick={handleQueryAll}>
              查询全部 ({favorites.length})
            </Button>
            <Popconfirm
              title="确认清空"
              description="确定要清空所有收藏吗？此操作不可恢复。"
              onConfirm={handleClearAll}
              okText="确定"
              cancelText="取消"
            >
              <Button danger icon={<ClearOutlined aria-hidden="true" />}>
                清空全部
              </Button>
            </Popconfirm>
          </Space>
        </Card>
      )}

      {/* 估值参数 */}
      {favorites.length > 0 && (
        <Collapse
          defaultActiveKey={[]}
          style={{ marginBottom: 16 }}
          items={[
            {
              key: 'valuation-settings',
              label: (
                <Space wrap>
                  <Text strong>收藏估值参数</Text>
                  <Tag color={valuationMethods.length > 0 ? 'blue' : 'default'}>{valuationMethodSummary}</Tag>
                  {valuationMethods.length > 1 && <Tag color="gold">{combineMethod}</Tag>}
                  {valuationMethods.includes('dcf') && (
                    <Tag color="green">DCF {dcfConfig.risk_profile || 'custom'}</Tag>
                  )}
                </Space>
              ),
              children: (
                <Space direction="vertical" style={{ width: '100%' }} size="middle">
                  <ValuationParamsForm
                    methods={valuationMethods}
                    setMethods={setValuationMethods}
                    date={valuationDate}
                    setDate={setValuationDate}
                    fiscalDate={valuationFiscalDate}
                    setFiscalDate={setValuationFiscalDate}
                    combineMethod={combineMethod}
                    setCombineMethod={setCombineMethod}
                  />
                  <DcfConfigForm
                    dcfConfig={dcfConfig}
                    setDcfConfig={setDcfConfig}
                    methods={valuationMethods}
                  />
                </Space>
              ),
            },
          ]}
        />
      )}

      {/* 标签页 */}
      <Tabs
        defaultActiveKey="list"
        items={[
          {
            key: 'list',
            label: `收藏列表 (${favorites.length})`,
            children: (
              <Card>
                <FavoritesList
                  selectedRowKeys={selectedKeys}
                  onSelectionChange={setSelectedKeys}
                  showSelection={true}
                  valuationMethods={valuationMethods}
                  valuationDate={valuationDate}
                  valuationFiscalDate={valuationFiscalDate}
                  combineMethod={combineMethod}
                  dcfConfig={dcfConfig}
                />
              </Card>
            ),
          },
          {
            key: 'add',
            label: '添加股票',
            children: (
              <Card title="手动添加股票到收藏">
                <AddFavorite />

                <Divider />

                <Alert
                  message="添加方式"
                  description={
                    <ul style={{ margin: 0, paddingLeft: 20 }}>
                      <li>在输入框中搜索股票代码或名称</li>
                      <li>从搜索结果中选择股票</li>
                      <li>或直接输入股票代码添加</li>
                      <li>在查询页面或AI筛选中点击"收藏"按钮</li>
                    </ul>
                  }
                  type="info"
                  showIcon
                />
              </Card>
            ),
          },
        ]}
      />
    </div>
  );
}

export default FavoritesPage;
