import { useState, useEffect } from 'react';
import { Card, Form, Select, InputNumber, Button, Space, Row, Col, Radio, Checkbox, Divider, Typography, Tag, message, DatePicker, Tooltip } from 'antd';
import { PlusOutlined, DeleteOutlined, PlayCircleOutlined, FilterOutlined, CalendarOutlined } from '@ant-design/icons';
import type { Criteria, CriteriaType, ScreeningConfig } from '@/types';
import { screeningService } from '@/services';
import dayjs, { Dayjs } from 'dayjs';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;

// Field options - fields that can be filtered on
const FIELD_OPTIONS = [
  { value: 'market', label: '市场' },
  { value: 'industry', label: '行业' },
  { value: 'pe_ttm', label: '市盈率(TTM)' },
  { value: 'pb', label: '市净率' },
  { value: 'total_mv_yi', label: '总市值(亿)' },
  { value: 'turnover_rate', label: '换手率(%)' },
  { value: 'avg_amplitude', label: '平均振幅(%)' },
  { value: 'positive_days', label: '正收益天数占比' },
  { value: 'bear_to_bull', label: '熊牛交替信号' },
];

// Field descriptions for tooltips
const FIELD_DESCRIPTIONS: Record<string, string> = {
  market: '筛选股票所属的市场板块（主板、创业板、科创板、北交所）',
  industry: '筛选股票所属的行业分类，支持多级行业选择',
  pe_ttm: '市盈率（TTM）：股票价格与每股收益的比率，衡量估值水平',
  pb: '市净率：股票价格与每股净资产的比率',
  total_mv_yi: '总市值（亿元）：公司的市场总价值',
  turnover_rate: '换手率：一定时期内股票转手买卖的频率',
  avg_amplitude: '平均振幅：股票价格每日波动幅度的平均值',
  positive_days: '正收益天数占比：一定时期内股价上涨的天数比例',
  bear_to_bull: '当日为牛市，且之前N个交易日持续为熊市或震荡市'
};

// Industry level options
const INDUSTRY_LEVEL_OPTIONS = [
  { value: 1, label: '一级行业' },
  { value: 2, label: '二级行业' },
  { value: 3, label: '三级行业' },
];

// Market options
const MARKET_OPTIONS = [
  { value: '主板', label: '主板' },
  { value: '创业板', label: '创业板' },
  { value: '科创板', label: '科创板' },
  { value: '北交所', label: '北交所' },
  { value: '港股', label: '港股' },
];

// Criteria type options - only basic comparison types
const CRITERIA_TYPE_OPTIONS: { value: CriteriaType; label: string }[] = [
  { value: 'Range', label: '范围' },
  { value: 'GreaterThan', label: '大于' },
  { value: 'LessThan', label: '小于' },
  { value: 'Percentile', label: '百分位' },
  { value: 'IndustryRelative', label: '业内相对' },
];

// Criteria type descriptions for tooltips
const CRITERIA_TYPE_DESCRIPTIONS: Record<CriteriaType, string> = {
  Range: '设置数值的取值范围（最小值和最大值）',
  GreaterThan: '筛选大于指定阈值的数值',
  LessThan: '筛选小于指定阈值的数值',
  Percentile: '基于历史百分位筛选（0-100%）',
  IndustryRelative: '相对于行业平均水平的百分位筛选',
  BearToBull: '当日为牛市，且之前N个交易日持续为熊市或震荡市'
};

// Industry option structure
interface IndustryOption {
  code: string;
  name: string;
  parent_code?: string;
}

// Condition data structure
interface ConditionData {
  id: string;
  field: string;           // Selected field (market, industry, pe_ttm, etc.)
  type: CriteriaType;      // Condition type (Range, GreaterThan, etc.)
  params: {
    // Market field params
    markets?: string[];
    marketMode?: 'include' | 'exclude';

    // Industry field params
    industries?: string[];
    industryLevel?: number;
    industryMode?: 'include' | 'exclude';

    // Numeric field params (PE, PB, Market Cap)
    minVal?: number;
    maxVal?: number;
    threshold?: number;
    percentile?: number;

    // Technical indicator params (period is always needed)
    period?: number;

    // Bear-to-bull params
    cycle?: string;

    // Industry relative params
    industryField?: string;
  };
}

// Component props
interface CustomCriteriaBuilderProps {
  onSubmit: (config: ScreeningConfig) => void;
  loading?: boolean;
  initialConfig?: ScreeningConfig;
}

// Global time range state
interface GlobalTimeRange {
  mode: 'period' | 'dateRange';
  period?: number;
  startDate?: Dayjs;
  endDate?: Dayjs;
}

// Get available criteria types based on field
const getAvailableTypesForField = (field: string): CriteriaType[] => {
  if (field === 'market') {
    return ['Range', 'GreaterThan', 'LessThan']; // Market uses checkboxes, type affects backend
  }
  if (field === 'industry') {
    return ['Range', 'GreaterThan', 'LessThan']; // Industry uses selection, type affects backend
  }
  if (['pe_ttm', 'pb', 'total_mv_yi'].includes(field)) {
    return ['Range', 'GreaterThan', 'LessThan', 'Percentile', 'IndustryRelative'];
  }
  if (['turnover_rate', 'avg_amplitude', 'positive_days'].includes(field)) {
    return ['Range', 'GreaterThan', 'LessThan'];
  }
  if (field === 'bear_to_bull') {
    return ['BearToBull'];
  }
  return ['Range', 'GreaterThan', 'LessThan'];
};

function CustomCriteriaBuilder({ onSubmit, loading = false, initialConfig }: CustomCriteriaBuilderProps) {
  const [form] = Form.useForm();

  // State for conditions list
  const [conditions, setConditions] = useState<ConditionData[]>([
    {
      id: '1',
      field: 'pe_ttm',
      type: 'Range',
      params: { minVal: 0, maxVal: 30, period: 20 }
    }
  ]);

  // Logic type (AND/OR)
  const [logicType, setLogicType] = useState<'AND' | 'OR'>('AND');

  // Global time range configuration
  const [globalTimeRange, setGlobalTimeRange] = useState<GlobalTimeRange>({
    mode: 'period',
    period: 20
  });

  // Industry options by level
  const [industryOptions, setIndustryOptions] = useState<Record<string, IndustryOption[]>>({});
  const [loadingIndustries, setLoadingIndustries] = useState(false);

  // Load industries when component mounts
  useEffect(() => {
    const loadIndustries = async (level: number) => {
      try {
        setLoadingIndustries(true);
        const response = await screeningService.getIndustries(level);
        if (response.success && response.industries) {
          setIndustryOptions((prev) => ({
            ...prev,
            [`Level ${level}`]: response.industries.map(i => ({
              code: i.code,
              name: i.name,
              parent_code: i.parent_code
            }))
          }));
        }
      } catch (error) {
        console.error('Failed to load industries:', error);
      } finally {
        setLoadingIndustries(false);
      }
    };

    // Load level 1 industries by default
    loadIndustries(1);
  }, []);

  // Add a new condition
  const addCondition = () => {
    const newCondition: ConditionData = {
      id: Date.now().toString(),
      field: 'pe_ttm',
      type: 'Range',
      params: { minVal: 0, maxVal: 30, period: 20 }
    };
    setConditions([...conditions, newCondition]);
  };

  // Remove a condition
  const removeCondition = (id: string) => {
    setConditions(conditions.filter(c => c.id !== id));
  };

  // Update condition field
  const updateConditionField = (id: string, newField: string) => {
    setConditions(conditions.map(c => {
      if (c.id !== id) return c;

      // Reset params based on new field
      let newParams: any = { period: 20 };

      if (newField === 'market') {
        newParams = { markets: ['主板', '港股'], marketMode: 'include' };
      } else if (newField === 'industry') {
        newParams = { industries: [], industryLevel: 1, industryMode: 'include' };
      } else if (['pe_ttm', 'pb', 'total_mv_yi'].includes(newField)) {
        newParams = { minVal: 0, maxVal: 50, period: 20 };
      } else if (['turnover_rate', 'avg_amplitude'].includes(newField)) {
        newParams = { minVal: 0, maxVal: 10, period: 20 };
      } else if (newField === 'positive_days') {
        newParams = { threshold: 0.5, period: 20 };
      } else if (newField === 'bear_to_bull') {
        newParams = { period: 10, cycle: 'medium' };
        return { ...c, field: newField, type: 'BearToBull' as CriteriaType, params: newParams };
      }

      return { ...c, field: newField, params: newParams };
    }));
  };

  // Update condition type
  const updateConditionType = (id: string, newType: CriteriaType) => {
    setConditions(conditions.map(c => {
      if (c.id !== id) return c;
      return { ...c, type: newType };
    }));
  };

  // Update condition params
  const updateConditionParams = (id: string, updates: Partial<ConditionData['params']>) => {
    setConditions(conditions.map(c => {
      if (c.id !== id) return c;
      return { ...c, params: { ...c.params, ...updates } };
    }));
  };

  // Render condition configuration based on field and type
  const renderConditionConfig = (condition: ConditionData) => {
    const { field, type, params } = condition;

    // === Market Field ===
    if (field === 'market') {
      return (
        <div style={{ width: '100%' }}>
          {/* Mode selector */}
          <div style={{ marginBottom: 16 }}>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
              选择模式
            </Text>
            <Radio.Group
              value={params.marketMode || 'include'}
              onChange={(e) => updateConditionParams(condition.id, { marketMode: e.target.value })}
              size="middle"
              style={{ width: '100%' }}
            >
              <Radio.Button value="include" style={{ width: '50%', textAlign: 'center' }}>
                包含
              </Radio.Button>
              <Radio.Button value="exclude" style={{ width: '50%', textAlign: 'center' }}>
                排除
              </Radio.Button>
            </Radio.Group>
          </div>

          {/* Divider */}
          <Divider style={{ margin: '16px 0' }} />

          {/* Market selection */}
          <div style={{ marginTop: 16 }}>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
              选择市场（{params.marketMode === 'include' ? '将包含所选市场' : '将排除所选市场'}）
            </Text>
            <Checkbox.Group
              style={{ width: '100%' }}
              options={MARKET_OPTIONS}
              value={params.markets || []}
              onChange={(values) => updateConditionParams(condition.id, { markets: values as string[] })}
            />
          </div>
        </div>
      );
    }

    // === Industry Field ===
    if (field === 'industry') {
      const level = params.industryLevel || 1;
      const options = industryOptions[`Level ${level}`] || [];

      return (
        <div style={{ width: '100%' }}>
          {/* Industry level selection */}
          <div style={{ marginBottom: 16 }}>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
              行业级别
            </Text>
            <Select
              value={level}
              onChange={(value) => {
                updateConditionParams(condition.id, { industryLevel: value, industries: [] });
                // Load industries for new level
                screeningService.getIndustries(value).then(response => {
                  if (response.success && response.industries) {
                    setIndustryOptions(prev => ({
                      ...prev,
                      [`Level ${value}`]: response.industries!.map(i => ({
                        code: i.code,
                        name: i.name,
                        parent_code: i.parent_code
                      }))
                    }));
                  }
                });
              }}
              style={{ width: '100%' }}
              size="middle"
            >
              {INDUSTRY_LEVEL_OPTIONS.map(opt => (
                <Select.Option key={opt.value} value={opt.value}>{opt.label}</Select.Option>
              ))}
            </Select>
          </div>

          {/* Divider */}
          <Divider style={{ margin: '16px 0' }} />

          {/* Mode selector */}
          <div style={{ marginBottom: 16 }}>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
              选择模式
            </Text>
            <Radio.Group
              value={params.industryMode || 'include'}
              onChange={(e) => updateConditionParams(condition.id, { industryMode: e.target.value })}
              size="middle"
              style={{ width: '100%' }}
            >
              <Radio.Button value="include" style={{ width: '50%', textAlign: 'center' }}>
                包含
              </Radio.Button>
              <Radio.Button value="exclude" style={{ width: '50%', textAlign: 'center' }}>
                排除
              </Radio.Button>
            </Radio.Group>
          </div>

          {/* Divider */}
          <Divider style={{ margin: '16px 0' }} />

          {/* Industry selection */}
          <div style={{ marginTop: 16 }}>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
              选择行业（{params.industryMode === 'include' ? '将包含所选行业' : '将排除所选行业'}）
            </Text>
            <Select
              mode="multiple"
              value={params.industries || []}
              onChange={(values) => updateConditionParams(condition.id, { industries: values })}
              loading={loadingIndustries}
              placeholder="请选择行业"
              style={{ width: '100%' }}
              size="middle"
            >
              {options.map(opt => (
                <Select.Option key={opt.code} value={opt.name}>{opt.name}</Select.Option>
              ))}
            </Select>
          </div>
        </div>
      );
    }

    // === Numeric Fields (PE, PB, Market Cap) ===
    if (['pe_ttm', 'pb', 'total_mv_yi'].includes(field)) {
      const availableTypes = getAvailableTypesForField(field);

      return (
        <div style={{ width: '100%' }}>
          {/* Condition Type Selector */}
          <div style={{ marginBottom: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', marginBottom: 4 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                条件类型
              </Text>
              <Tooltip title={CRITERIA_TYPE_DESCRIPTIONS[type] || '选择筛选条件的类型'}>
                <span style={{ marginLeft: 4, color: '#1890ff', cursor: 'help' }}>ⓘ</span>
              </Tooltip>
            </div>
            <Select
              value={type}
              onChange={(value) => updateConditionType(condition.id, value as CriteriaType)}
              style={{ width: '100%' }}
              size="middle"
            >
              {availableTypes.map(t => (
                <Select.Option key={t} value={t}>
                  {CRITERIA_TYPE_OPTIONS.find(opt => opt.value === t)?.label || t}
                </Select.Option>
              ))}
            </Select>
          </div>

          {/* Divider */}
          <Divider style={{ margin: '16px 0' }} />

          {/* Parameter Configuration based on type */}
          <div style={{ marginTop: 16 }}>
            {/* Range Type */}
            {type === 'Range' && (
              <div>
                <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
                  取值范围
                </Text>
                <Row gutter={[8, 12]}>
                  <Col xs={24} md={12}>
                    <InputNumber
                      placeholder="最小值"
                      value={params.minVal}
                      onChange={(val) => updateConditionParams(condition.id, { minVal: val || 0 })}
                      style={{ width: '100%' }}
                      size="middle"
                    />
                  </Col>
                  <Col xs={24} md={12}>
                    <InputNumber
                      placeholder="最大值"
                      value={params.maxVal}
                      onChange={(val) => updateConditionParams(condition.id, { maxVal: val || 0 })}
                      style={{ width: '100%' }}
                      size="middle"
                    />
                  </Col>
                </Row>
              </div>
            )}

            {/* GreaterThan/LessThan Type */}
            {(type === 'GreaterThan' || type === 'LessThan') && (
              <div>
                <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
                  {type === 'GreaterThan' ? '大于阈值' : '小于阈值'}
                </Text>
                <InputNumber
                  placeholder="阈值"
                  value={params.threshold}
                  onChange={(val) => updateConditionParams(condition.id, { threshold: val || 0 })}
                  style={{ width: '100%' }}
                  size="middle"
                  addonAfter={type === 'GreaterThan' ? '>' : '<'}
                />
              </div>
            )}

            {/* Percentile Type */}
            {type === 'Percentile' && (
              <div>
                <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
                  百分位 (0-100)
                </Text>
                <InputNumber
                  placeholder="百分位"
                  value={params.percentile}
                  onChange={(val) => updateConditionParams(condition.id, { percentile: val || 0 })}
                  style={{ width: '100%' }}
                  size="middle"
                  min={0}
                  max={100}
                  addonAfter="%"
                />
              </div>
            )}

            {/* IndustryRelative Type */}
            {type === 'IndustryRelative' && (
              <div>
                <div style={{ marginBottom: 16 }}>
                  <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
                    相对指标
                  </Text>
                  <Select
                    placeholder="选择相对指标"
                    value={params.industryField}
                    onChange={(value) => updateConditionParams(condition.id, { industryField: value })}
                    style={{ width: '100%' }}
                    size="middle"
                  >
                    <Select.Option value="pe_ttm">PE(TTM)</Select.Option>
                    <Select.Option value="pb">PB</Select.Option>
                    <Select.Option value="total_mv_yi">总市值</Select.Option>
                  </Select>
                </div>

                <div>
                  <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
                    百分位 (0-100)
                  </Text>
                  <InputNumber
                    placeholder="百分位"
                    value={params.percentile}
                    onChange={(val) => updateConditionParams(condition.id, { percentile: val || 0 })}
                    style={{ width: '100%' }}
                    size="middle"
                    min={0}
                    max={100}
                    addonAfter="%"
                  />
                </div>
              </div>
            )}
          </div>
        </div>
      );
    }

    // === Technical Indicator Fields (Turnover, Amplitude, Positive Days) ===
    if (['turnover_rate', 'avg_amplitude', 'positive_days'].includes(field)) {
      const availableTypes = getAvailableTypesForField(field);

      return (
        <div style={{ width: '100%' }}>
          {/* Time range info */}
          <div style={{
            marginBottom: 16,
            padding: 8,
            backgroundColor: '#f6ffed',
            border: '1px solid #b7eb8f',
            borderRadius: 4
          }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              使用全局时间范围配置
              {globalTimeRange.mode === 'period'
                ? `（最近 ${globalTimeRange.period} 天）`
                : `（${globalTimeRange.startDate?.format('YYYY-MM-DD')} 至 ${globalTimeRange.endDate?.format('YYYY-MM-DD')}）`}
            </Text>
          </div>

          {/* Condition Type Selector */}
          <div style={{ marginBottom: 16 }}>
            <div style={{ display: 'flex', alignItems: 'center', marginBottom: 4 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                条件类型
              </Text>
              <Tooltip title={CRITERIA_TYPE_DESCRIPTIONS[type] || '选择筛选条件的类型'}>
                <span style={{ marginLeft: 4, color: '#1890ff', cursor: 'help' }}>ⓘ</span>
              </Tooltip>
            </div>
            <Select
              value={type}
              onChange={(value) => updateConditionType(condition.id, value as CriteriaType)}
              style={{ width: '100%' }}
              size="middle"
            >
              {availableTypes.map(t => (
                <Select.Option key={t} value={t}>
                  {CRITERIA_TYPE_OPTIONS.find(opt => opt.value === t)?.label || t}
                </Select.Option>
              ))}
            </Select>
          </div>

          {/* Divider */}
          <Divider style={{ margin: '16px 0' }} />

          {/* Parameter Configuration based on type */}
          <div style={{ marginTop: 16 }}>
            {/* Range Type */}
            {type === 'Range' && (
              <div>
                <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
                  取值范围
                </Text>
                <Row gutter={[8, 12]}>
                  <Col xs={24} md={12}>
                    <InputNumber
                      placeholder="最小值"
                      value={params.minVal}
                      onChange={(val) => updateConditionParams(condition.id, { minVal: val || 0 })}
                      style={{ width: '100%' }}
                      size="middle"
                    />
                  </Col>
                  <Col xs={24} md={12}>
                    <InputNumber
                      placeholder="最大值"
                      value={params.maxVal}
                      onChange={(val) => updateConditionParams(condition.id, { maxVal: val || 0 })}
                      style={{ width: '100%' }}
                      size="middle"
                    />
                  </Col>
                </Row>
              </div>
            )}

            {/* GreaterThan/LessThan Type */}
            {(type === 'GreaterThan' || type === 'LessThan') && (
              <div>
                <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
                  {type === 'GreaterThan' ? '大于阈值' : '小于阈值'}
                </Text>
                <InputNumber
                  placeholder="阈值"
                  value={params.threshold}
                  onChange={(val) => updateConditionParams(condition.id, { threshold: val || 0 })}
                  style={{ width: '100%' }}
                  size="middle"
                  addonAfter={type === 'GreaterThan' ? '>' : '<'}
                />
              </div>
            )}
          </div>
        </div>
      );
    }

    // === Bear-to-Bull Signal Field ===
    if (field === 'bear_to_bull') {
      return (
        <div style={{ width: '100%' }}>
          <div style={{ marginBottom: 16 }}>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
              之前天数
            </Text>
            <InputNumber
              placeholder="天数"
              value={params.period}
              onChange={(val) => updateConditionParams(condition.id, { period: val || 10 })}
              style={{ width: '100%' }}
              size="middle"
              min={1}
              max={120}
              addonAfter="天"
            />
          </div>

          <Divider style={{ margin: '16px 0' }} />

          <div>
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
              判断周期
            </Text>
            <Select
              value={params.cycle || 'medium'}
              onChange={(value) => updateConditionParams(condition.id, { cycle: value })}
              style={{ width: '100%' }}
              size="middle"
            >
              <Select.Option value="short">短周期（MA 3/5/10）</Select.Option>
              <Select.Option value="medium">中周期（MA 5/10/20）</Select.Option>
              <Select.Option value="long">长周期（MA 10/20/40）</Select.Option>
            </Select>
          </div>
        </div>
      );
    }

    return <Text type="secondary">请选择字段</Text>;
  };

  // Render a single condition card with vertical layout
  const renderConditionCard = (condition: ConditionData, index: number) => {
    return (
      <div key={condition.id} style={{ position: 'relative', height: '100%' }}>
        <Card
          size="small"
          style={{
            height: '100%',
            border: '1px solid #d9d9d9',
            borderRadius: 8,
            boxShadow: '0 2px 8px rgba(0,0,0,0.06)',
            display: 'flex',
            flexDirection: 'column'
          }}
        >
          {/* Card header with field selector */}
          <div style={{
            paddingBottom: 12,
            borderBottom: '1px solid #f0f0f0',
            marginBottom: 12
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <div style={{ flex: 1 }}>
                <div style={{ display: 'flex', alignItems: 'center', marginBottom: 4 }}>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    字段
                  </Text>
                  <Tooltip title={FIELD_DESCRIPTIONS[condition.field] || '选择要筛选的字段'}>
                    <span style={{ marginLeft: 4, color: '#1890ff', cursor: 'help' }}>ⓘ</span>
                  </Tooltip>
                </div>
                <Select
                  value={condition.field}
                  onChange={(value) => updateConditionField(condition.id, value)}
                  style={{ width: '100%' }}
                  size="middle"
                >
                  {FIELD_OPTIONS.map(opt => (
                    <Select.Option key={opt.value} value={opt.value}>
                      {opt.label}
                    </Select.Option>
                  ))}
                </Select>
              </div>

              {/* Condition status indicator */}
              <div style={{ marginLeft: 16, textAlign: 'center' }}>
                <div style={{
                  width: 12,
                  height: 12,
                  borderRadius: '50%',
                  backgroundColor: isConditionValid(condition) ? '#52c41a' : '#ff4d4f',
                  marginBottom: 4,
                  boxShadow: isConditionValid(condition) ? '0 0 4px rgba(82, 196, 26, 0.5)' : '0 0 4px rgba(255, 77, 79, 0.5)'
                }} />
                <Text type="secondary" style={{ fontSize: 10 }}>
                  {index + 1}
                </Text>
                <Text type="secondary" style={{ fontSize: 8, marginTop: 2 }}>
                  {isConditionValid(condition) ? '有效' : '无效'}
                </Text>
              </div>
            </div>
          </div>

          {/* Condition configuration - vertical stack */}
          <div style={{ marginBottom: 16 }}>
            {renderConditionConfig(condition)}
          </div>

          {/* Card footer with delete action */}
          <div style={{
            paddingTop: 12,
            borderTop: '1px solid #f0f0f0',
            display: 'flex',
            justifyContent: 'flex-end'
          }}>
            <Button
              type="default"
              size="middle"
              icon={<DeleteOutlined />}
              onClick={() => removeCondition(condition.id)}
              danger
              style={{ width: '100%' }}
            >
              删除条件
            </Button>
          </div>
        </Card>
      </div>
    );
  };

  // Check if a single condition is valid
  const isConditionValid = (c: ConditionData): boolean => {
    if (c.field === 'market') {
      if (!c.params.markets || c.params.markets.length === 0) {
        return false;
      }
    }
    if (c.field === 'industry') {
      if (!c.params.industries || c.params.industries.length === 0) {
        return false;
      }
    }
    if (['pe_ttm', 'pb', 'total_mv_yi'].includes(c.field)) {
      if (c.type === 'Range') {
        if (c.params.minVal === undefined || c.params.maxVal === undefined) {
          return false;
        }
      } else if (['GreaterThan', 'LessThan'].includes(c.type)) {
        if (c.params.threshold === undefined) {
          return false;
        }
      } else if (c.type === 'Percentile' || c.type === 'IndustryRelative') {
        if (c.params.percentile === undefined) {
          return false;
        }
      }
    }
    if (['turnover_rate', 'avg_amplitude'].includes(c.field)) {
      if (c.type === 'Range') {
        if (c.params.minVal === undefined || c.params.maxVal === undefined) {
          return false;
        }
      } else if (['GreaterThan', 'LessThan'].includes(c.type)) {
        if (c.params.threshold === undefined) {
          return false;
        }
      }
    }
    if (c.field === 'positive_days') {
      if (c.params.threshold === undefined) {
        return false;
      }
    }
    if (c.field === 'bear_to_bull') {
      return !!(c.params.period && c.params.period > 0);
    }
    return true;
  };

  // Validate conditions
  const validateConditions = (): boolean => {
    for (const c of conditions) {
      if (!isConditionValid(c)) {
        // Show appropriate error message
        if (c.field === 'market') {
          message.error('请选择至少一个市场');
        } else if (c.field === 'industry') {
          message.error('请选择至少一个行业');
        } else if (['pe_ttm', 'pb', 'total_mv_yi'].includes(c.field)) {
          if (c.type === 'Range') {
            message.error('请设置最小值和最大值');
          } else if (['GreaterThan', 'LessThan'].includes(c.type)) {
            message.error('请设置阈值');
          } else if (c.type === 'Percentile' || c.type === 'IndustryRelative') {
            message.error('请设置百分位');
          }
        } else if (['turnover_rate', 'avg_amplitude'].includes(c.field)) {
          if (c.type === 'Range') {
            message.error('请设置最小值和最大值');
          } else if (['GreaterThan', 'LessThan'].includes(c.type)) {
            message.error('请设置阈值');
          }
        } else if (c.field === 'positive_days') {
          message.error('请设置最小正收益天数比例');
        }
        return false;
      }
    }
    return true;
  };

  // Handle form submission
  const handleSubmit = () => {
    if (!validateConditions()) {
      return;
    }

    // Build screening config
    const config: ScreeningConfig = {
      type: logicType,
      criteria: conditions.map(c => {
        // Market field -> MarketFilter (for backward compatibility) or FieldFilter
        if (c.field === 'market') {
          return {
            type: 'MarketFilter',
            markets: c.params.markets || [],
            mode: c.params.marketMode || 'include'
          };
        }

        // Industry field -> IndustryFilter (for backward compatibility) or FieldFilter
        if (c.field === 'industry') {
          return {
            type: 'IndustryFilter',
            industries: c.params.industries || [],
            mode: c.params.industryMode || 'include',
            level: c.params.industryLevel || 1
          };
        }

        // Numeric fields with Range
        if (c.type === 'Range' && ['pe_ttm', 'pb', 'total_mv_yi'].includes(c.field)) {
          return {
            type: 'Range',
            column: c.field,
            min_val: c.params.minVal,
            max_val: c.params.maxVal
          };
        }

        // Numeric fields with GreaterThan
        if (c.type === 'GreaterThan' && ['pe_ttm', 'pb', 'total_mv_yi'].includes(c.field)) {
          return {
            type: 'GreaterThan',
            column: c.field,
            threshold: c.params.threshold
          };
        }

        // Numeric fields with LessThan
        if (c.type === 'LessThan' && ['pe_ttm', 'pb', 'total_mv_yi'].includes(c.field)) {
          return {
            type: 'LessThan',
            column: c.field,
            threshold: c.params.threshold
          };
        }

        // Numeric fields with Percentile
        if (c.type === 'Percentile' && ['pe_ttm', 'pb', 'total_mv_yi'].includes(c.field)) {
          return {
            type: 'Percentile',
            column: c.field,
            percentile: c.params.percentile
          };
        }

        // Numeric fields with IndustryRelative
        if (c.type === 'IndustryRelative' && ['pe_ttm', 'pb', 'total_mv_yi'].includes(c.field)) {
          return {
            type: 'IndustryRelative',
            column: c.field,
            percentile: c.params.percentile,
            industry_field: c.params.industryField || 'pe_ttm'
          };
        }

        // Technical indicator fields - TurnoverColumn
        if (c.field === 'turnover_rate') {
          // Use global time range
          const timeRangeConfig = globalTimeRange.mode === 'period'
            ? { period: globalTimeRange.period || 20 }
            : {
                start_date: globalTimeRange.startDate?.format('YYYYMMDD'),
                end_date: globalTimeRange.endDate?.format('YYYYMMDD')
              };

          if (c.type === 'Range') {
            return {
              type: 'TurnoverColumn',
              ...timeRangeConfig,
              min_val: c.params.minVal,
              max_val: c.params.maxVal
            };
          } else if (c.type === 'GreaterThan') {
            return {
              type: 'TurnoverColumn',
              ...timeRangeConfig,
              threshold: c.params.threshold
            };
          } else if (c.type === 'LessThan') {
            return {
              type: 'TurnoverColumn',
              ...timeRangeConfig,
              max_threshold: c.params.threshold
            };
          }
        }

        // Technical indicator fields - AmplitudeColumn
        if (c.field === 'avg_amplitude') {
          // Use global time range
          const timeRangeConfig = globalTimeRange.mode === 'period'
            ? { period: globalTimeRange.period || 20 }
            : {
                start_date: globalTimeRange.startDate?.format('YYYYMMDD'),
                end_date: globalTimeRange.endDate?.format('YYYYMMDD')
              };

          if (c.type === 'Range') {
            return {
              type: 'AmplitudeColumn',
              ...timeRangeConfig,
              min_val: c.params.minVal,
              max_val: c.params.maxVal
            };
          } else if (c.type === 'GreaterThan') {
            return {
              type: 'AmplitudeColumn',
              ...timeRangeConfig,
              threshold: c.params.threshold
            };
          } else if (c.type === 'LessThan') {
            return {
              type: 'AmplitudeColumn',
              ...timeRangeConfig,
              max_threshold: c.params.threshold
            };
          }
        }

        // Technical indicator fields - PositiveDays
        if (c.field === 'positive_days') {
          // Use global time range
          const timeRangeConfig = globalTimeRange.mode === 'period'
            ? { period: globalTimeRange.period || 20 }
            : {
                start_date: globalTimeRange.startDate?.format('YYYYMMDD'),
                end_date: globalTimeRange.endDate?.format('YYYYMMDD')
              };

          return {
            type: 'PositiveDays',
            ...timeRangeConfig,
            threshold: c.params.threshold,
            min_positive_ratio: c.params.min_positive_ratio || 0.5
          };
        }

        // Bear-to-Bull transition signal
        if (c.field === 'bear_to_bull') {
          return {
            type: 'BearToBull',
            period: c.params.period || 10,
            cycle: c.params.cycle || 'medium'
          };
        }

        // Fallback
        return {
          type: c.type,
          column: c.field,
          ...c.params
        } as Criteria;
      })
    };

    onSubmit(config);
  };

  return (
    <div>
      <Title level={2}>
        <FilterOutlined style={{ marginRight: 8 }} />
        自定义筛选
      </Title>

      <Card style={{ marginTop: 16 }}>
        {/* Configuration Row: Logic Type + Time Range */}
        <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
          {/* Logic Type Selector - Compact */}
          <Col xs={24} md={12} lg={10}>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                <FilterOutlined style={{ marginRight: 8, color: '#1890ff' }} />
                <Text strong style={{ fontSize: 14 }}>条件组合方式</Text>
                <Tooltip title="选择所有条件之间的逻辑关系">
                  <span style={{ marginLeft: 4, color: '#1890ff', cursor: 'help' }}>ⓘ</span>
                </Tooltip>
              </div>
              <Radio.Group
                value={logicType}
                onChange={(e) => setLogicType(e.target.value)}
                size="middle"
                style={{ width: '100%' }}
              >
                <Radio.Button
                  value="AND"
                  style={{
                    width: '50%',
                    textAlign: 'center',
                    padding: '8px 0',
                    backgroundColor: logicType === 'AND' ? '#1890ff' : '#fafafa',
                    color: logicType === 'AND' ? 'white' : 'inherit'
                  }}
                >
                  <Text strong={logicType === 'AND'} style={{ color: logicType === 'AND' ? 'white' : 'inherit' }}>
                    AND（且）
                  </Text>
                </Radio.Button>
                <Radio.Button
                  value="OR"
                  style={{
                    width: '50%',
                    textAlign: 'center',
                    padding: '8px 0',
                    backgroundColor: logicType === 'OR' ? '#1890ff' : '#fafafa',
                    color: logicType === 'OR' ? 'white' : 'inherit'
                  }}
                >
                  <Text strong={logicType === 'OR'} style={{ color: logicType === 'OR' ? 'white' : 'inherit' }}>
                    OR（或）
                  </Text>
                </Radio.Button>
              </Radio.Group>
              <Text type="secondary" style={{ fontSize: 12, marginTop: 4, display: 'block' }}>
                {logicType === 'AND' ? '所有条件必须同时满足' : '满足任意条件即可'}
              </Text>
            </div>
          </Col>

          {/* Global Time Range Configuration - Compact */}
          <Col xs={24} md={12} lg={14}>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                <CalendarOutlined style={{ marginRight: 8, color: '#1890ff' }} />
                <Text strong style={{ fontSize: 14 }}>时间范围配置</Text>
                <Tag color="blue" size="small" style={{ marginLeft: 8 }}>全局设置</Tag>
                <Tooltip title="应用于所有需要历史数据的条件">
                  <span style={{ marginLeft: 4, color: '#1890ff', cursor: 'help' }}>ⓘ</span>
                </Tooltip>
              </div>

              <div style={{ backgroundColor: '#e6f7ff', padding: 12, borderRadius: 8, border: '1px solid #91d5ff' }}>
                {/* Mode selector */}
                <div style={{ marginBottom: 12 }}>
                  <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
                    时间范围模式
                  </Text>
                  <Radio.Group
                    value={globalTimeRange.mode}
                    onChange={(e) => setGlobalTimeRange(prev => ({ ...prev, mode: e.target.value }))}
                    size="small"
                    style={{ width: '100%' }}
                  >
                    <Radio.Button value="period" style={{ width: '50%', textAlign: 'center', padding: '4px 0' }}>
                      最近N天
                    </Radio.Button>
                    <Radio.Button value="dateRange" style={{ width: '50%', textAlign: 'center', padding: '4px 0' }}>
                      日期范围
                    </Radio.Button>
                  </Radio.Group>
                </div>

                {/* Parameter configuration based on mode */}
                <div>
                  {globalTimeRange.mode === 'period' ? (
                    <div>
                      <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
                        计算周期（天）
                      </Text>
                      <InputNumber
                        value={globalTimeRange.period}
                        onChange={(val) => setGlobalTimeRange(prev => ({ ...prev, period: val || 20 }))}
                        style={{ width: '100%' }}
                        size="small"
                        min={5}
                        max={120}
                        addonAfter="天"
                      />
                    </div>
                  ) : (
                    <div>
                      <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
                        选择日期范围
                      </Text>
                      <RangePicker
                        value={globalTimeRange.startDate && globalTimeRange.endDate ? [globalTimeRange.startDate, globalTimeRange.endDate] : null}
                        onChange={(dates) => {
                          setGlobalTimeRange({
                            mode: 'dateRange',
                            startDate: dates?.[0] || dayjs().subtract(30, 'day'),
                            endDate: dates?.[1] || dayjs()
                          });
                        }}
                        style={{ width: '100%' }}
                        size="small"
                        format="YYYY-MM-DD"
                      />
                    </div>
                  )}
                </div>
              </div>
            </div>
          </Col>
        </Row>

        {/* Conditions List */}
        <Card
          size="small"
          style={{
            marginBottom: 24,
            border: '1px solid #d9d9d9',
            borderRadius: 8
          }}
          title={
            <Space style={{ width: '100%', justifyContent: 'space-between' }}>
              <Space>
                <FilterOutlined />
                <Text strong>筛选条件</Text>
                <Tag color="blue">{conditions.length} 个条件</Tag>
              </Space>
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={addCondition}
                size="middle"
              >
                添加条件
              </Button>
            </Space>
          }
        >
          <div style={{ padding: 8 }}>
            {conditions.length === 0 ? (
              <div style={{ textAlign: 'center', padding: 40 }}>
                <FilterOutlined style={{ fontSize: 48, color: '#d9d9d9', marginBottom: 16 }} />
                <Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
                  暂无筛选条件
                </Text>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  点击上方"添加条件"按钮开始构建筛选规则
                </Text>
              </div>
            ) : (
              <div>
                <div style={{ marginBottom: 16 }}>
                  <Text type="secondary">
                    已添加 {conditions.length} 个条件，使用 <Tag size="small" color="blue">{logicType}</Tag> 逻辑组合
                  </Text>
                </div>
                <Row gutter={[16, 16]}>
                  {conditions.map((condition, index) => (
                    <Col key={condition.id} xs={24} md={12} lg={8} xl={6}>
                      {renderConditionCard(condition, index)}
                    </Col>
                  ))}
                </Row>
              </div>
            )}
          </div>
        </Card>

        {/* Submit Button */}
        <Card
          size="small"
          style={{
            marginTop: 24,
            border: '1px solid #52c41a',
            borderRadius: 8,
            backgroundColor: '#f6ffed'
          }}
        >
          <div style={{ padding: 16, textAlign: 'center' }}>
            <div style={{ marginBottom: 16 }}>
              <PlayCircleOutlined style={{ fontSize: 32, color: '#52c41a', marginBottom: 8 }} />
              <div>
                <Text strong style={{ fontSize: 16, display: 'block', marginBottom: 4 }}>
                  准备开始筛选
                </Text>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  已配置 {conditions.length} 个筛选条件，使用 {logicType} 逻辑组合
                </Text>
              </div>
            </div>
            <Button
              type="primary"
              icon={<PlayCircleOutlined />}
              onClick={handleSubmit}
              loading={loading}
              size="large"
              block
              style={{ height: 48, fontSize: 16 }}
            >
              开始筛选
            </Button>
            <div style={{ marginTop: 12 }}>
              <Text type="secondary" style={{ fontSize: 12 }}>
                点击按钮将根据以上配置筛选符合条件的股票
              </Text>
            </div>
          </div>
        </Card>
      </Card>
    </div>
  );
}

export default CustomCriteriaBuilder;
