/**
 * 风险管理页面
 */
import React, { useState, useEffect, useCallback } from 'react';
import { Row, Col, Card, InputNumber, Button, Space, Typography, Divider, message, Spin, Collapse } from 'antd';
import { SettingOutlined, PlusOutlined, ReloadOutlined, SaveOutlined, CloudSyncOutlined, HistoryOutlined } from '@ant-design/icons';
import {
  PositionList,
  PositionEditor,
  SellModal,
  NewStockForm,
  RiskSummaryCard,
  CalculationResult,
  PositionForm,
  MonthlySummaryCard,
  MonthlyHistoryTable
} from '../components/risk';
import {
  getPortfolioState,
  calculateNewPosition,
  loadFullPortfolio,
  savePortfolioSettings,
  updatePositionInDb,
  deletePositionFromDb,
  getCapitalState,
  adjustInitialCapital,
  getMonthlySnapshots,
  createMonthlySnapshot,
  updateMonthlyCapitalChange,
  addRealizedPnl,
  updateCash,
} from '../services/riskService';
import type {
  Position,
  PositionDetail,
  PortfolioState,
  CalculateResponse,
  SellResponse,
  AdjustStopLossResponse,
  PositionOutput,
  CapitalState,
  MonthlySnapshot,
} from '../types/risk.types';

const { Title, Text } = Typography;
const { Panel } = Collapse;

const RiskManagementPage: React.FC = () => {
  // 基本参数
  const [totalCapital, setTotalCapital] = useState<number>(650000);
  const [maxTotalRiskPercent, setMaxTotalRiskPercent] = useState<number>(6);
  const [maxSingleRiskPercent, setMaxSingleRiskPercent] = useState<number>(2);

  // 投资组合状态
  const [positions, setPositions] = useState<Position[]>([]);
  const [portfolioState, setPortfolioState] = useState<PortfolioState | null>(null);

  // 资金状态
  const [capitalState, setCapitalState] = useState<CapitalState | null>(null);

  // 月度快照
  const [monthlySnapshots, setMonthlySnapshots] = useState<MonthlySnapshot[]>([]);
  const [currentMonthSnapshot, setCurrentMonthSnapshot] = useState<MonthlySnapshot | null>(null);

  // 计算结果
  const [calcResult, setCalcResult] = useState<CalculateResponse | null>(null);
  const [calcLoading, setCalcLoading] = useState(false);

  // 弹窗状态
  const [editingPosition, setEditingPosition] = useState<PositionDetail | null>(null);
  const [showPositionEditor, setShowPositionEditor] = useState(false);
  const [sellingPosition, setSellingPosition] = useState<PositionDetail | null>(null);
  const [showSellModal, setShowSellModal] = useState(false);
  const [showAddPosition, setShowAddPosition] = useState(false);

  // 加载状态
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [snapshotLoading, setSnapshotLoading] = useState(false);

  // 从数据库加载投资组合
  const loadPortfolioFromDb = useCallback(async () => {
    setLoading(true);
    try {
      const portfolio = await loadFullPortfolio();
      setTotalCapital(portfolio.total_capital);
      setMaxTotalRiskPercent(portfolio.max_total_risk_percent);
      setMaxSingleRiskPercent(portfolio.max_single_risk_percent);
      // 转换 PositionOutput 到 Position
      const loadedPositions: Position[] = portfolio.positions.map((p: PositionOutput) => ({
        symbol: p.symbol,
        name: p.name,
        shares: p.shares,
        cost_price: p.cost_price,
        current_price: p.current_price,
        price_date: p.price_date,
        stop_loss_base: p.stop_loss_base,
        stop_loss_percent: p.stop_loss_percent,
      }));
      setPositions(loadedPositions);
    } catch (error) {
      console.error('加载投资组合失败:', error);
      message.error('加载投资组合失败');
    } finally {
      setLoading(false);
    }
  }, []);

  // 加载资金状态
  const loadCapitalState = useCallback(async () => {
    try {
      const state = await getCapitalState();
      setCapitalState(state);
    } catch (error) {
      console.error('加载资金状态失败:', error);
    }
  }, []);

  // 加载月度快照
  const loadMonthlySnapshots = useCallback(async () => {
    try {
      const snapshots = await getMonthlySnapshots({ limit: 12 });
      setMonthlySnapshots(snapshots);

      // 查找当月快照
      const currentMonth = new Date().toISOString().slice(0, 7);
      const current = snapshots.find(s => s.year_month === currentMonth);
      setCurrentMonthSnapshot(current || null);
    } catch (error) {
      console.error('加载月度快照失败:', error);
    }
  }, []);

  // 页面加载时从数据库读取
  useEffect(() => {
    loadPortfolioFromDb();
    loadCapitalState();
    loadMonthlySnapshots();
  }, [loadPortfolioFromDb, loadCapitalState, loadMonthlySnapshots]);

  // 更新投资组合状态
  const updatePortfolioState = useCallback(async () => {
    try {
      const state = await getPortfolioState({
        total_capital: totalCapital,
        max_total_risk_percent: maxTotalRiskPercent,
        max_single_risk_percent: maxSingleRiskPercent,
        positions,
      });
      setPortfolioState(state);
    } catch (error) {
      console.error('获取投资组合状态失败:', error);
    }
  }, [totalCapital, maxTotalRiskPercent, maxSingleRiskPercent, positions]);

  useEffect(() => {
    if (!loading) {
      updatePortfolioState();
    }
  }, [updatePortfolioState, loading]);

  // 保存设置到数据库
  const handleSaveSettings = async () => {
    setSaving(true);
    try {
      await savePortfolioSettings({
        total_capital: totalCapital,
        max_total_risk_percent: maxTotalRiskPercent,
        max_single_risk_percent: maxSingleRiskPercent,
      });
      message.success('设置已保存');
    } catch (error) {
      message.error('保存设置失败');
    } finally {
      setSaving(false);
    }
  };

  // 刷新数据（重新从数据库加载）
  const handleRefresh = () => {
    loadPortfolioFromDb();
    loadCapitalState();
    loadMonthlySnapshots();
    setCalcResult(null);
  };

  // 创建月度快照
  const handleCreateSnapshot = async () => {
    setSnapshotLoading(true);
    try {
      await createMonthlySnapshot();
      await loadMonthlySnapshots();
      message.success('月度快照已保存');
    } catch (error) {
      message.error('保存快照失败');
    } finally {
      setSnapshotLoading(false);
    }
  };

  // 更新月度资金变动
  const handleEditCapitalChange = async (yearMonth: string, amount: number, reason: string) => {
    try {
      await updateMonthlyCapitalChange({ year_month: yearMonth, amount, reason });
      await loadMonthlySnapshots();
    } catch (error) {
      message.error('更新资金变动失败');
    }
  };

  // 计算新股可买股数
  const handleCalculate = async (buyPrice: number, stopLossPercent: number) => {
    setCalcLoading(true);
    try {
      const result = await calculateNewPosition({
        total_capital: totalCapital,
        max_total_risk_percent: maxTotalRiskPercent,
        max_single_risk_percent: maxSingleRiskPercent,
        positions,
        buy_price: buyPrice,
        stop_loss_percent: stopLossPercent,
      });
      setCalcResult(result);
    } catch (error) {
      message.error('计算失败');
    } finally {
      setCalcLoading(false);
    }
  };

  // 调整止损
  const handleAdjustStopLoss = (position: PositionDetail) => {
    setEditingPosition(position);
    setShowPositionEditor(true);
  };

  const handleAdjustStopLossSuccess = async (response: AdjustStopLossResponse) => {
    // 更新数据库
    try {
      await updatePositionInDb(response.position.symbol, {
        stop_loss_base: response.position.stop_loss_base,
        stop_loss_percent: response.position.stop_loss_percent,
      });

      // 更新本地状态
      setPositions(prev => prev.map(p => {
        if (p.symbol === response.position.symbol) {
          return {
            ...p,
            stop_loss_base: response.position.stop_loss_base,
            stop_loss_percent: response.position.stop_loss_percent,
            current_price: response.position.current_price,
          };
        }
        return p;
      }));
      setCalcResult(null);
      message.success('止损参数已更新');
    } catch (error) {
      message.error('更新止损失败');
    }
  };

  // 卖出
  const handleSell = (position: PositionDetail) => {
    setSellingPosition(position);
    setShowSellModal(true);
  };

  const handleSellSuccess = async (response: SellResponse) => {
    try {
      // 添加已实现盈亏到累计盈亏，并更新现金
      await addRealizedPnl(
        response.sell_info.realized_profit,
        response.sell_info.sell_value,
        response.sell_info.symbol
      );

      if (response.remaining_shares === 0) {
        // 全部卖出，从数据库删除
        await deletePositionFromDb(response.sell_info.symbol);
        setPositions(prev => prev.filter(p => p.symbol !== response.sell_info.symbol));
        message.success(`已全部卖出，盈亏: ¥${response.sell_info.realized_profit.toLocaleString()}`);
      } else {
        // 部分卖出，更新数据库
        await updatePositionInDb(response.sell_info.symbol, {
          shares: response.remaining_shares,
        });
        setPositions(prev => prev.map(p => {
          if (p.symbol === response.sell_info.symbol) {
            return {
              ...p,
              shares: response.remaining_shares,
            };
          }
          return p;
        }));
        message.success(`已部分卖出，盈亏: ¥${response.sell_info.realized_profit.toLocaleString()}`);
      }
      setCalcResult(null);
      // 刷新资金状态
      loadCapitalState();
    } catch (error) {
      message.error('更新持仓失败');
    }
  };

  // 添加持仓成功后，重新从数据库加载
  const handleAddPositionSuccess = () => {
    loadPortfolioFromDb();
    loadCapitalState();
    setCalcResult(null);
  };

  // 删除所有持仓（重置）
  const handleResetPositions = async () => {
    try {
      // 删除所有持仓
      for (const p of positions) {
        await deletePositionFromDb(p.symbol);
      }
      setPositions([]);
      setCalcResult(null);
      loadCapitalState();
      message.success('已清空所有持仓');
    } catch (error) {
      message.error('清空持仓失败');
    }
  };

  if (loading) {
    return (
      <div style={{ padding: 24, textAlign: 'center' }}>
        <Spin size="large" />
        <div style={{ marginTop: 16 }}>加载中...</div>
      </div>
    );
  }

  return (
    <div style={{ padding: 24 }}>
      <Title level={2}>风险管理</Title>
      <Text type="secondary">
        单笔风险 ≤ {maxSingleRiskPercent}%，总风险 ≤ {maxTotalRiskPercent}%
      </Text>

      <Divider />

      <Row gutter={[16, 16]}>
        {/* 左侧：参数设置和持仓列表 */}
        <Col xs={24} lg={16}>
          <Card
            title={<><SettingOutlined /> 参数设置</>}
            size="small"
            style={{ marginBottom: 16 }}
            extra={
              <Button
                type="primary"
                icon={<SaveOutlined />}
                onClick={handleSaveSettings}
                loading={saving}
                size="small"
              >
                保存设置
              </Button>
            }
          >
            <Row gutter={16}>
              <Col span={8}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Text>总资金</Text>
                  <InputNumber
                    style={{ width: '100%' }}
                    value={totalCapital}
                    onChange={(v) => setTotalCapital(v || 0)}
                    min={0}
                    step={10000}
                    formatter={(v) => `¥ ${v}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
                    parser={(v) => v?.replace(/¥\s?|(,*)/g, '') as unknown as number}
                  />
                </Space>
              </Col>
              <Col span={8}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Text>总风险上限 (%)</Text>
                  <InputNumber
                    style={{ width: '100%' }}
                    value={maxTotalRiskPercent}
                    onChange={(v) => setMaxTotalRiskPercent(v || 6)}
                    min={1}
                    max={100}
                    step={1}
                  />
                </Space>
              </Col>
              <Col span={8}>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Text>单笔风险上限 (%)</Text>
                  <InputNumber
                    style={{ width: '100%' }}
                    value={maxSingleRiskPercent}
                    onChange={(v) => setMaxSingleRiskPercent(v || 2)}
                    min={0.5}
                    max={50}
                    step={0.5}
                  />
                </Space>
              </Col>
            </Row>
          </Card>

          <Card
            title="持仓列表"
            size="small"
            extra={
              <Space>
                <Button
                  type="link"
                  icon={<CloudSyncOutlined />}
                  onClick={handleRefresh}
                >
                  刷新
                </Button>
                <Button
                  type="link"
                  icon={<ReloadOutlined />}
                  onClick={handleResetPositions}
                  danger
                >
                  清空
                </Button>
                <Button
                  type="primary"
                  icon={<PlusOutlined />}
                  onClick={() => setShowAddPosition(true)}
                >
                  添加持仓
                </Button>
              </Space>
            }
          >
            <PositionList
              positions={portfolioState?.positions || []}
              onAdjustStopLoss={handleAdjustStopLoss}
              onSell={handleSell}
            />
          </Card>

          {/* 月度历史记录 */}
          <Card
            title={<><HistoryOutlined /> 月度历史</>}
            size="small"
            style={{ marginTop: 16 }}
          >
            <MonthlyHistoryTable
              snapshots={monthlySnapshots}
              onEditCapitalChange={handleEditCapitalChange}
            />
          </Card>
        </Col>

        {/* 右侧：风险汇总和计算 */}
        <Col xs={24} lg={8}>
          <RiskSummaryCard
            portfolio={portfolioState}
            capitalState={capitalState}
            onUpdateCash={async (cash: number) => {
              await updateCash(cash);
              loadCapitalState();
            }}
          />

          <div style={{ marginTop: 16 }}>
            <MonthlySummaryCard
              capitalState={capitalState}
              currentMonthSnapshot={currentMonthSnapshot}
              onCreateSnapshot={handleCreateSnapshot}
              loading={snapshotLoading}
            />
          </div>

          <div style={{ marginTop: 16 }}>
            <NewStockForm
              loading={calcLoading}
              onCalculate={handleCalculate}
            />
          </div>

          <div style={{ marginTop: 16 }}>
            <CalculationResult result={calcResult} />
          </div>
        </Col>
      </Row>

      {/* 弹窗 */}
      <PositionEditor
        visible={showPositionEditor}
        position={editingPosition}
        onClose={() => {
          setShowPositionEditor(false);
          setEditingPosition(null);
        }}
        onSuccess={handleAdjustStopLossSuccess}
      />

      <SellModal
        visible={showSellModal}
        position={sellingPosition}
        totalCapital={totalCapital}
        maxTotalRiskPercent={maxTotalRiskPercent}
        maxSingleRiskPercent={maxSingleRiskPercent}
        allPositions={positions}
        onClose={() => {
          setShowSellModal(false);
          setSellingPosition(null);
        }}
        onSuccess={handleSellSuccess}
      />

      <PositionForm
        visible={showAddPosition}
        onClose={() => setShowAddPosition(false)}
        onSuccess={handleAddPositionSuccess}
      />
    </div>
  );
};

export default RiskManagementPage;
