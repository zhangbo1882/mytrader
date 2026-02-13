import { Card, Checkbox, DatePicker, Select, Space, Divider, Tooltip } from 'antd';
import { SettingOutlined, InfoCircleOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import type { ValuationMethod, CombineMethod } from '@/types';

interface ValuationParamsFormProps {
  methods: ValuationMethod[];
  setMethods: (methods: ValuationMethod[]) => void;
  date: string;
  setDate: (date: string) => void;
  combineMethod: CombineMethod;
  setCombineMethod: (method: CombineMethod) => void;
}

const METHOD_OPTIONS = [
  {
    label: 'PE估值（市盈率）',
    value: 'pe' as ValuationMethod,
    tooltip: '适用于盈利稳定的公司，PE = 股价/每股收益'
  },
  {
    label: 'PB估值（市净率）',
    value: 'pb' as ValuationMethod,
    tooltip: '适用于金融、周期性行业，PB = 股价/每股净资产'
  },
  {
    label: 'PS估值（市销率）',
    value: 'ps' as ValuationMethod,
    tooltip: '适用于尚未盈利或高增长公司，PS = 股价/每股销售额'
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
  {
    label: '组合估值',
    value: 'combined' as ValuationMethod,
    tooltip: '自动组合PE、PB、PS三种相对估值方法（不包含DCF，如需DCF请单独勾选）'
  },
];

const COMBINE_METHOD_OPTIONS = [
  { label: '加权平均', value: 'weighted' },
  { label: '简单平均', value: 'average' },
  { label: '中位数', value: 'median' },
  { label: '最高置信度', value: 'max_confidence' },
];

export function ValuationParamsForm({
  methods,
  setMethods,
  date,
  setDate,
  combineMethod,
  setCombineMethod
}: ValuationParamsFormProps) {
  // 检查是否选择了组合估值
  const hasCombined = methods.includes('combined');

  // 处理方法选择变化
  const handleMethodChange = (vals: ValuationMethod[]) => {
    const newMethods = vals as ValuationMethod[];

    // 如果选择了组合估值，自动移除pe/pb/ps（因为组合估值已包含）
    if (newMethods.includes('combined')) {
      const filtered = newMethods.filter(m => m !== 'pe' && m !== 'pb' && m !== 'ps');
      setMethods(filtered);
    } else {
      setMethods(newMethods);
    }
  };

  // 构建选项列表（当选择组合估值时，pe/pb/ps变为禁用状态）
  const checkboxOptions = METHOD_OPTIONS.map(opt => {
    const isRelativeMethod = opt.value === 'pe' || opt.value === 'pb' || opt.value === 'ps';
    const disabled = hasCombined && isRelativeMethod;

    return {
      label: (
        <Tooltip title={disabled ? '组合估值已包含此方法' : opt.tooltip}>
          <span style={disabled ? { color: '#999' } : undefined}>
            {opt.label}
            {disabled && ' (已包含)'}
          </span>
        </Tooltip>
      ),
      value: opt.value,
      disabled
    };
  });

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
            onChange={handleMethodChange}
            options={checkboxOptions}
          />
          {hasCombined && (
            <div style={{ marginTop: 8, fontSize: 12, color: '#999' }}>
              <InfoCircleOutlined style={{ marginRight: 4 }} />
              组合估值 = PE + PB + PS 综合估值（如需DCF请单独勾选）
            </div>
          )}
        </div>

        <Divider style={{ margin: '12px 0' }} />

        {/* Date selection */}
        <Space>
          <span style={{ color: '#666' }}>估值日期：</span>
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
