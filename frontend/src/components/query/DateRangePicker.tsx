import { useState, useEffect } from 'react';
import { DatePicker, Button, Space, Radio } from 'antd';
import dayjs, { Dayjs } from 'dayjs';
import { useQueryStore } from '@/stores';
import { getDateRanges } from '@/utils';

const { RangePicker } = DatePicker;

export function DateRangePicker() {
  const { dateRange, setDateRange, priceType, setPriceType } = useQueryStore();
  const [internalValue, setInternalValue] = useState<[Dayjs, Dayjs] | null>(null);

  // 初始化日期范围
  useEffect(() => {
    if (dateRange.start && dateRange.end) {
      setInternalValue([dayjs(dateRange.start), dayjs(dateRange.end)]);
    }
  }, [dateRange.start, dateRange.end]);

  // 快捷日期选择
  const handleQuickSelect = (key: string) => {
    const ranges = getDateRanges();
    const range = ranges[key];
    if (range) {
      setDateRange(range);
      setInternalValue([dayjs(range.start), dayjs(range.end)]);
    }
  };

  // 自定义日期选择
  const handleDateChange = (dates: null | [Dayjs | null, Dayjs | null]) => {
    if (dates && dates[0] && dates[1]) {
      const range = {
        start: dates[0].format('YYYY-MM-DD'),
        end: dates[1].format('YYYY-MM-DD'),
      };
      setDateRange(range);
      setInternalValue([dates[0], dates[1]]);
    } else {
      setInternalValue(null);
    }
  };

  return (
    <div>
      <div style={{ marginBottom: 12 }}>
        <Space direction="vertical" style={{ width: '100%' }}>
          {/* 快捷日期选择 */}
          <div>
            <span style={{ marginRight: 8, color: '#666' }}>快捷选择：</span>
            <Button size="small" onClick={() => handleQuickSelect('1M')}>
              1个月
            </Button>
            <Button size="small" onClick={() => handleQuickSelect('3M')}>
              3个月
            </Button>
            <Button size="small" onClick={() => handleQuickSelect('6M')}>
              6个月
            </Button>
            <Button size="small" onClick={() => handleQuickSelect('1Y')}>
              1年
            </Button>
            <Button size="small" onClick={() => handleQuickSelect('YTD')}>
              年初至今
            </Button>
          </div>

          {/* 日期范围选择器 */}
          <RangePicker
            value={internalValue}
            onChange={handleDateChange}
            style={{ width: '100%' }}
            format="YYYY-MM-DD"
            placeholder={['开始日期', '结束日期']}
          />

          {/* 复权类型选择 */}
          <div>
            <span style={{ marginRight: 8, color: '#666' }}>复权类型：</span>
            <Radio.Group
              value={priceType}
              onChange={(e) => setPriceType(e.target.value)}
              buttonStyle="solid"
            >
              <Radio.Button value="qfq">前复权</Radio.Button>
              <Radio.Button value="hfq">后复权</Radio.Button>
              <Radio.Button value="bfq">不复权</Radio.Button>
            </Radio.Group>
          </div>
        </Space>
      </div>

      {/* 当前选择显示 */}
      {dateRange.start && dateRange.end && (
        <div style={{ marginTop: 8, color: '#666', fontSize: 12 }}>
          已选择：{dateRange.start} 至 {dateRange.end}
          {dateRange.start === dateRange.end && '（单日）'}
        </div>
      )}
    </div>
  );
}
