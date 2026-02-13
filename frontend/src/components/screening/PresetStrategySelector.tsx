import { useEffect, useState } from 'react';
import { Card, Row, Col, Typography, Spin, Alert } from 'antd';
import { BulbOutlined, DollarOutlined, RiseOutlined, ThunderboltOutlined, SafetyOutlined, FundOutlined, HeartOutlined, LineChartOutlined, ExperimentOutlined, FilterOutlined } from '@ant-design/icons';
import { screeningService } from '@/services';
import type { PresetStrategy, StrategiesListResponse, ScreeningConfig } from '@/types';

const { Text } = Typography;

// 格式化条件为可读文本
function formatCriteria(criteria: any): string[] {
  if (!criteria) return [];

  const result: string[] = [];

  // 处理AND/OR/NOT组合条件
  if (criteria.type === 'AND' || criteria.type === 'OR' || criteria.type === 'NOT') {
    if (criteria.criteria && Array.isArray(criteria.criteria)) {
      criteria.criteria.forEach((c: any) => {
        result.push(...formatCriteria(c));
      });
    }
    return result;
  }

  // 处理具体条件类型
  switch (criteria.type) {
    case 'Range':
      if (criteria.min_val !== undefined && criteria.max_val !== undefined) {
        return [`${criteria.column}: ${criteria.min_val} ~ ${criteria.max_val}`];
      } else if (criteria.min_val !== undefined) {
        return [`${criteria.column} ≥ ${criteria.min_val}`];
      } else if (criteria.max_val !== undefined) {
        return [`${criteria.column} ≤ ${criteria.max_val}`];
      }
      break;

    case 'GreaterThan':
      return [`${criteria.column} > ${criteria.threshold}`];

    case 'LessThan':
      return [`${criteria.column} < ${criteria.threshold}`];

    case 'Percentile':
      return [`${criteria.column} 前 ${(criteria.percentile * 100).toFixed(0)}%`];

    case 'IndustryFilter':
      if (criteria.mode === 'whitelist') {
        return [`行业白名单: ${criteria.industries?.join(', ') || '未指定'}`];
      } else {
        return [`行业黑名单: ${criteria.industries?.join(', ') || '未指定'}`];
      }

    case 'IndustryRelative':
      return [`行业内${criteria.column} 前 ${(criteria.percentile * 100).toFixed(0)}%`];

    default:
      return [`${criteria.type}: ${JSON.stringify(criteria).slice(0, 50)}...`];
  }

  return [];
}

interface StrategyInfo {
  key: PresetStrategy;
  name: string;
  description: string;
  icon: React.ReactNode;
  criteria_config?: ScreeningConfig;  // 新增：筛选条件配置
}

const STRATEGY_INFO: Record<PresetStrategy, Omit<StrategyInfo, 'key'>> = {
  liquidity: {
    name: '流动性策略',
    description: '筛选流动性较好的股票，关注成交额和换手率',
    icon: <LineChartOutlined />
  },
  value: {
    name: '价值投资策略',
    description: '寻找低估值股票，关注PE、PB等估值指标',
    icon: <DollarOutlined />
  },
  growth: {
    name: '成长股策略',
    description: '筛选高成长性股票，关注营收和利润增长率',
    icon: <RiseOutlined />
  },
  tech_growth: {
    name: '科技成长策略',
    description: '聚焦科技行业中的高成长公司',
    icon: <ThunderboltOutlined />
  },
  quality: {
    name: '质量策略',
    description: '筛选财务质量优秀的公司',
    icon: <SafetyOutlined />
  },
  dividend: {
    name: '股息策略',
    description: '寻找高股息收益率的股票',
    icon: <FundOutlined />
  },
  low_volatility: {
    name: '低波动策略',
    description: '筛选波动率较低的稳健股票',
    icon: <HeartOutlined />
  },
  turnaround: {
    name: '困境反转策略',
    description: '寻找可能触底反弹的股票',
    icon: <ExperimentOutlined />
  },
  momentum_quality: {
    name: '动量质量策略',
    description: '结合动量和质量因子的选股策略',
    icon: <RiseOutlined />
  },
  exclude_financials: {
    name: '排除金融股',
    description: '从筛选结果中排除金融行业股票',
    icon: <FilterOutlined />
  }
};

interface PresetStrategySelectorProps {
  onSubmit: (strategy: PresetStrategy, config?: ScreeningConfig) => void;
  loading?: boolean;
  showAllCriteria?: boolean;
}

function PresetStrategySelector({ onSubmit, loading, showAllCriteria = false }: PresetStrategySelectorProps) {
  const [strategies, setStrategies] = useState<StrategyInfo[]>([]);
  const [loadingStrategies, setLoadingStrategies] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchStrategies = async () => {
      setLoadingStrategies(true);
      setError('');
      try {
        const response = await screeningService.listStrategies() as StrategiesListResponse;
        if (response.success && response.strategies) {
          const strategyList = response.strategies.map(s => ({
            key: s.name,
            name: STRATEGY_INFO[s.name].name,
            description: s.description || STRATEGY_INFO[s.name].description,
            icon: STRATEGY_INFO[s.name].icon,
            criteria_config: s.criteria_config
          }));
          setStrategies(strategyList);
        } else {
          // Fallback to default strategies if API doesn't return data
          setStrategies(Object.entries(STRATEGY_INFO).map(([key, info]) => ({
            key: key as PresetStrategy,
            ...info
          })));
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : '加载策略列表失败');
        // Fallback to default strategies
        setStrategies(Object.entries(STRATEGY_INFO).map(([key, info]) => ({
          key: key as PresetStrategy,
          ...info
        })));
      } finally {
        setLoadingStrategies(false);
      }
    };

    fetchStrategies();
  }, []);

  if (loadingStrategies) {
    return (
      <div style={{ textAlign: 'center', padding: '40px 0' }}>
        <Spin size="large" tip="加载策略列表..." />
      </div>
    );
  }

  return (
    <div>
      {error && (
        <Alert
          message="加载策略列表时出错"
          description={error}
          type="warning"
          showIcon
          closable
          style={{ marginBottom: 16 }}
        />
      )}

      <Text type="secondary" style={{ display: 'block', marginBottom: 16 }}>
        选择一个预设策略进行股票筛选
      </Text>

      <Row gutter={[16, 16]}>
        {strategies.map((strategy) => (
          <Col xs={24} sm={12} md={8} key={strategy.key}>
            <Card
              hoverable
              onClick={() => !loading && onSubmit(strategy.key, strategy.criteria_config)}
              style={{
                height: '100%',
                cursor: loading ? 'not-allowed' : 'pointer',
                opacity: loading ? 0.6 : 1
              }}
            >
              <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12 }}>
                <div style={{
                  fontSize: '24px',
                  color: '#1890ff',
                  flexShrink: 0
                }}>
                  {strategy.icon}
                </div>
                <div style={{ flex: 1 }}>
                  <div style={{ fontWeight: 600, marginBottom: 8 }}>
                    {strategy.name}
                  </div>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    {strategy.description}
                  </Text>

                  {/* 显示筛选条件 */}
                  {strategy.criteria_config && (
                    <div style={{ marginTop: 8 }}>
                      <Text type="secondary" style={{ fontSize: '10px', display: 'block', marginBottom: 4 }}>
                        筛选条件：
                      </Text>
                      {(() => {
                        const criteriaList = formatCriteria(strategy.criteria_config);
                        const displayCount = showAllCriteria ? criteriaList.length : Math.min(criteriaList.length, 3);
                        return (
                          <>
                            {criteriaList.slice(0, displayCount).map((criterion, index) => (
                              <Text
                                key={index}
                                type="secondary"
                                style={{
                                  fontSize: '10px',
                                  display: 'block',
                                  lineHeight: '1.4',
                                  marginBottom: 2
                                }}
                              >
                                • {criterion}
                              </Text>
                            ))}
                            {!showAllCriteria && criteriaList.length > 3 && (
                              <Text type="secondary" style={{ fontSize: '10px' }}>
                                • ...等{criteriaList.length}个条件
                              </Text>
                            )}
                          </>
                        );
                      })()}
                    </div>
                  )}
                </div>
              </div>
            </Card>
          </Col>
        ))}
      </Row>
    </div>
  );
}

export default PresetStrategySelector;
