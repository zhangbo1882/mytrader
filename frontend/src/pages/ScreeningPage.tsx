import { useState } from 'react';
import { Typography, Card, Tabs, Alert, Spin, message, Button } from 'antd';
import { FilterOutlined, SaveOutlined, HistoryOutlined } from '@ant-design/icons';
import { PresetStrategySelector, CustomCriteriaBuilder, ScreeningResults, ScreeningHistory } from '@/components/screening';
import { screeningService } from '@/services';
import type { PresetStrategy, ScreeningConfig, ScreeningResult, Criteria } from '@/types';
import dayjs from 'dayjs';

const { Title, Text } = Typography;

type TabKey = 'preset' | 'custom' | 'history';

/**
 * 生成自动历史记录名称
 * 格式：条件描述_YYYYMMDD_HHmm
 */
function generateHistoryName(config: ScreeningConfig | null, presetStrategy: PresetStrategy | null): string {
  // 生成日期时间部分
  const now = dayjs();
  const dateTimePart = now.format('YYYYMMDD_HHmm');

  // 预设策略
  if (presetStrategy) {
    const strategyNames: Record<PresetStrategy, string> = {
      liquidity: '流动性策略',
      value: '价值投资策略',
      growth: '成长股策略',
      tech_growth: '科技成长策略',
      quality: '质量策略',
      dividend: '股息策略',
      low_volatility: '低波动策略',
      turnaround: '困境反转策略',
      momentum_quality: '动量质量策略',
      exclude_financials: '排除金融策略'
    };
    const strategyName = strategyNames[presetStrategy] || presetStrategy;
    return `${strategyName}_${dateTimePart}`;
  }

  // 自定义配置
  if (config && config.criteria && config.criteria.length > 0) {
    // 将条件转换为简短描述
    const criteriaDescriptions = config.criteria.map(criteriaToShortDescription).filter(Boolean);

    if (criteriaDescriptions.length > 0) {
      // 限制最多使用3个条件描述
      const maxCriteria = 3;
      let descriptionPart = '';
      if (criteriaDescriptions.length <= maxCriteria) {
        descriptionPart = criteriaDescriptions.join('_');
      } else {
        descriptionPart = criteriaDescriptions.slice(0, maxCriteria).join('_') + `_等${criteriaDescriptions.length - maxCriteria}条件`;
      }
      return `${descriptionPart}_${dateTimePart}`;
    }
  }

  // 默认名称
  return `筛选历史_${dateTimePart}`;
}

/**
 * 将单个条件转换为简短描述
 */
function criteriaToShortDescription(criteria: Criteria): string {
  const { type, column } = criteria;

  // 市场筛选
  if (type === 'MarketFilter') {
    const markets = criteria.markets || [];
    const mode = criteria.mode || 'include';
    if (markets.length === 0) return '';
    const marketStr = markets.length <= 2 ? markets.join('+') : `${markets[0]}等${markets.length}市场`;
    return mode === 'include' ? `市场:${marketStr}` : `排除市场:${marketStr}`;
  }

  // 行业筛选
  if (type === 'IndustryFilter') {
    const industries = criteria.industries || [];
    const mode = criteria.mode || 'include';
    if (industries.length === 0) return '';
    const industryStr = industries.length <= 2 ? industries.join('+') : `${industries[0]}等${industries.length}行业`;
    return mode === 'include' ? `行业:${industryStr}` : `排除行业:${industryStr}`;
  }

  // 范围条件
  if (type === 'Range' && column) {
    const { min_val, max_val } = criteria;
    if (min_val !== undefined && max_val !== undefined) {
      // 简化列名
      const colName = getShortColumnName(column);
      return `${colName}${min_val}-${max_val}`;
    }
  }

  // 大于条件
  if (type === 'GreaterThan' && column) {
    const { threshold } = criteria;
    if (threshold !== undefined) {
      const colName = getShortColumnName(column);
      return `${colName}>${threshold}`;
    }
  }

  // 小于条件
  if (type === 'LessThan' && column) {
    const { threshold } = criteria;
    if (threshold !== undefined) {
      const colName = getShortColumnName(column);
      return `${colName}<${threshold}`;
    }
  }

  // 百分位条件
  if (type === 'Percentile' && column) {
    const { percentile } = criteria;
    if (percentile !== undefined) {
      const colName = getShortColumnName(column);
      return `${colName}P${Math.round(percentile * 100)}`;
    }
  }

  // 行业相对条件
  if (type === 'IndustryRelative' && column) {
    const { percentile } = criteria;
    if (percentile !== undefined) {
      const colName = getShortColumnName(column);
      return `${colName}业内P${Math.round(percentile * 100)}`;
    }
  }

  // 技术指标条件
  if (type === 'AmplitudeColumn') {
    const { min_val, max_val, threshold } = criteria;
    if (min_val !== undefined && max_val !== undefined) {
      return `振幅${min_val}-${max_val}`;
    } else if (threshold !== undefined) {
      return `振幅>${threshold}`;
    }
  }

  if (type === 'TurnoverColumn') {
    const { min_val, max_val, threshold } = criteria;
    if (min_val !== undefined && max_val !== undefined) {
      return `换手${min_val}-${max_val}`;
    } else if (threshold !== undefined) {
      return `换手>${threshold}`;
    }
  }

  if (type === 'PositiveDays') {
    const { threshold } = criteria;
    if (threshold !== undefined) {
      return `正收益>${threshold}`;
    }
  }

  // 默认使用类型
  return type;
}

/**
 * 获取简短的列名
 */
function getShortColumnName(column: string): string {
  const columnMap: Record<string, string> = {
    'pe_ttm': 'PE',
    'pb': 'PB',
    'total_mv': '市值',
    'total_mv_yi': '市值',
    'circ_mv': '流通市值',
    'latest_roe': 'ROE',
    'latest_or_yoy': '营收增长',
    'latest_gr_yoy': '净利润增长',
    'debt_to_assets': '负债率',
    'amount': '成交额',
    'turnover': '换手率',
    'turnover_rate': '换手率',
    'close': '收盘价',
    'avg_amplitude': '振幅'
  };
  return columnMap[column] || column;
}

// Store current config for saving
let currentConfig: ScreeningConfig | null = null;
let currentPresetStrategy: PresetStrategy | null = null;

function ScreeningPage() {
  const [activeTab, setActiveTab] = useState<TabKey>('preset');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<ScreeningResult | null>(null);
  const [error, setError] = useState('');
  const [historyRefreshTrigger, setHistoryRefreshTrigger] = useState(0);
  const [presetTemplateConfig, setPresetTemplateConfig] = useState<ScreeningConfig | null>(null);
  const [presetLoadedMessage, setPresetLoadedMessage] = useState<string>('');

  const handlePresetSubmit = async (strategy: PresetStrategy, config?: ScreeningConfig) => {
    // 预设策略作为模板：将配置加载到自定义构建器
    if (config) {
      setPresetTemplateConfig(config);
      currentPresetStrategy = strategy;
      currentConfig = config;
      // 设置提示消息
      const strategyNames: Record<PresetStrategy, string> = {
        liquidity: '流动性策略',
        value: '价值投资策略',
        growth: '成长股策略',
        tech_growth: '科技成长策略',
        quality: '质量策略',
        dividend: '股息策略',
        low_volatility: '低波动策略',
        turnaround: '困境反转策略',
        momentum_quality: '动量质量策略',
        exclude_financials: '排除金融策略'
      };
      const strategyName = strategyNames[strategy] || strategy;
      setPresetLoadedMessage(`已加载 "${strategyName}" 的筛选条件作为模板，您可以在下方修改参数`);
      // 切换到自定义筛选标签页
      setActiveTab('custom');
      // 清空之前的筛选结果
      setResult(null);
    } else {
      // 如果没有配置，执行原来的直接筛选逻辑
      setLoading(true);
      setError('');
      currentPresetStrategy = strategy;
      currentConfig = null;
      try {
        const response = await screeningService.applyPresetStrategy(strategy, 100);
        setResult(response);
      } catch (err) {
        setError(err instanceof Error ? err.message : '筛选失败');
      } finally {
        setLoading(false);
      }
    }
  };

  const handleCustomSubmit = async (config: ScreeningConfig) => {
    setLoading(true);
    setError('');
    currentConfig = config;
    currentPresetStrategy = null;
    try {
      const response = await screeningService.applyCustomStrategy(config, 2000);
      setResult(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : '筛选失败');
    } finally {
      setLoading(false);
    }
  };

  const handleSaveToHistory = async () => {
    try {
      // 生成自动名称
      const autoName = generateHistoryName(currentConfig, currentPresetStrategy);

      // For preset strategy, we need to get the config from the strategy
      // For now, save with basic config structure
      const configToSave = currentConfig || {
        type: 'AND' as const,
        criteria: [{ type: 'PresetStrategy' as const, strategy: currentPresetStrategy || 'liquidity' }]
      };

      await screeningService.saveHistory(
        autoName,
        configToSave,
        result?.stocks
      );

      message.success('筛选历史保存成功');
    } catch (err) {
      message.error(err instanceof Error ? err.message : '保存失败');
    }
  };

  const handleTabChange = (key: string) => {
    setActiveTab(key as TabKey);
    if (key === 'history') {
      // Trigger refresh when switching to history tab
      setHistoryRefreshTrigger(prev => prev + 1);
    }
  };

  const handleReRunComplete = () => {
    // Switch to preset tab to show the re-run results
    setActiveTab('preset');
  };

  return (
    <div>
      <Title level={2}>
        <FilterOutlined style={{ marginRight: 8 }} aria-hidden="true" />
        股票筛选
      </Title>
      <Text type="secondary">使用预设策略或自定义条件筛选股票</Text>

      <Card style={{ marginTop: 16 }}>
        <Tabs
          activeKey={activeTab}
          onChange={handleTabChange}
          items={[
            {
              key: 'preset',
              label: '预设策略',
              children: (
                <PresetStrategySelector
                  onSubmit={handlePresetSubmit}
                  loading={loading}
                  showAllCriteria={true}
                />
              ),
            },
            {
              key: 'custom',
              label: '自定义筛选',
              children: (
                <CustomCriteriaBuilder
                  onSubmit={handleCustomSubmit}
                  loading={loading}
                />
              ),
            },
            {
              key: 'history',
              label: (
                <span>
                  <HistoryOutlined />
                  筛选历史
                </span>
              ),
              children: (
                <ScreeningHistory
                  refreshTrigger={historyRefreshTrigger}
                  onLoadHistory={(detail) => {
                    setResult({ success: true, count: detail.stocks_count, stocks: detail.stocks });
                    currentConfig = detail.config;
                  }}
                  onReRunComplete={handleReRunComplete}
                />
              ),
            },
          ]}
        />
      </Card>

      {error && (
        <Alert
          message="筛选失败"
          description={error}
          type="error"
          showIcon
          closable
          style={{ marginTop: 16 }}
          onClose={() => setError('')}
        />
      )}

      {loading && (
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <Spin size="large" tip="正在筛选股票..." />
        </div>
      )}

      {result && !loading && activeTab !== 'history' && (
        <ScreeningResults
          result={result}
          onSave={handleSaveToHistory}
        />
      )}

    </div>
  );
}

export default ScreeningPage;
