import { Card, Checkbox, DatePicker, Select, Space, Divider, Tooltip } from 'antd';
import { SettingOutlined, InfoCircleOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import type { ValuationMethod, CombineMethod } from '@/types';

const generateQuarterOptions = () => {
  const options = [];
  const now = dayjs();
  let current = now;
  
  for (let i = 0; i < 16; i++) {
    const year = current.year();
    const month = current.month() + 1;
    let quarter: number;
    let quarterEndDate: string;
    
    if (month >= 11) {
      quarter = 3;
      quarterEndDate = `${year}-09-30`;
    } else if (month >= 9) {
      quarter = 2;
      quarterEndDate = `${year}-06-30`;
    } else if (month >= 5) {
      quarter = 1;
      quarterEndDate = `${year}-03-31`;
    } else {
      quarter = 4;
      quarterEndDate = `${year - 1}-12-31`;
    }
    
    const quarterLabel = `${quarter === 4 ? year - 1 : year}Q${quarter}`;
    options.push({
      label: quarterLabel,
      value: quarterEndDate,
    });
    
    current = current.subtract(3, 'month');
  }
  
  return options;
};

const FISCAL_QUARTER_OPTIONS = generateQuarterOptions();

interface ValuationParamsFormProps {
  methods: ValuationMethod[];
  setMethods: (methods: ValuationMethod[]) => void;
  date: string;
  setDate: (date: string) => void;
  fiscalDate: string;
  setFiscalDate: (date: string) => void;
  combineMethod: CombineMethod;
  setCombineMethod: (method: CombineMethod) => void;
}

const METHOD_OPTIONS = [
  {
    label: '相对估值（PE/PB/PS）',
    value: 'combined' as ValuationMethod,
    tooltip: '综合PE(市盈率)、PB(市净率)、PS(市销率)三种相对估值方法，适用于各类公司'
  },
  {
    label: 'PEG估值',
    value: 'peg' as ValuationMethod,
    tooltip: '适用于成长股，PEG = PE/增长率，PEG < 1 表示低估'
  },
  {
    label: 'DCF估值（绝对估值）',
    value: 'dcf' as ValuationMethod,
    tooltip: '基于现金流折现的绝对估值法，适合现金流稳定的公司'
  },
];

const COMBINE_METHOD_OPTIONS = [
  { label: '加权平均', value: 'weighted' },
  { label: '保守最低值', value: 'min_fair_value' },
  { label: '简单平均', value: 'average' },
  { label: '中位数', value: 'median' },
  { label: '最高置信度', value: 'max_confidence' },
  { label: '贝叶斯加权（数据驱动）', value: 'bayesian' },
];

export function ValuationParamsForm({
  methods,
  setMethods,
  date,
  setDate,
  fiscalDate,
  setFiscalDate,
  combineMethod,
  setCombineMethod
}: ValuationParamsFormProps) {
  // 构建选项列表
  const checkboxOptions = METHOD_OPTIONS.map(opt => ({
    label: (
      <Tooltip title={opt.tooltip}>
        <span>{opt.label}</span>
      </Tooltip>
    ),
    value: opt.value,
  }));

  return (
    <Card
      size="small"
      title={
        <Space>
          <SettingOutlined />
          <span>估值参数</span>
        </Space>
      }
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        {/* Valuation method selection */}
        <div>
          <div style={{ marginBottom: 8, color: '#666' }}>
            估值方法：
            <Tooltip title="选择多种方法可进行组合估值">
              <InfoCircleOutlined style={{ marginLeft: 4, color: '#999', cursor: 'help' }} />
            </Tooltip>
          </div>
          <Checkbox.Group
            value={methods}
            onChange={(vals) => setMethods(vals as ValuationMethod[])}
            options={checkboxOptions}
          />
        </div>

        <Divider style={{ margin: '12px 0' }} />

        {/* Date selection */}
        <Space wrap>
          <Space>
            <span style={{ color: '#666' }}>股价日期：</span>
            <DatePicker
              value={date ? dayjs(date) : null}
              onChange={(dateObj) => {
                setDate(dateObj ? dateObj.format('YYYY-MM-DD') : '');
              }}
              format="YYYY-MM-DD"
              placeholder="选择日期（默认最新）"
              allowClear
            />
          </Space>
          <Space>
            <span style={{ color: '#666' }}>
              财报期：
              <Tooltip title="选择财报期，使用该报告期及之前的财务数据进行估值。例如选2025Q3则使用2025年三季报数据">
                <InfoCircleOutlined style={{ marginLeft: 4, color: '#999', cursor: 'help' }} />
              </Tooltip>
            </span>
            <Select
              value={fiscalDate || undefined}
              onChange={(value) => setFiscalDate(value || '')}
              options={FISCAL_QUARTER_OPTIONS}
              placeholder="默认最新财报"
              allowClear
              style={{ width: 150 }}
            />
          </Space>
        </Space>

        {/* Combination method */}
        {methods.length > 1 && (
          <>
            <Divider style={{ margin: '12px 0' }} />
            <Space>
              <span style={{ color: '#666' }}>组合方式：</span>
              <Select
                value={combineMethod}
                onChange={setCombineMethod}
                options={COMBINE_METHOD_OPTIONS}
                style={{ width: 120 }}
              />
            </Space>
          </>
        )}
      </Space>
    </Card>
  );
}
