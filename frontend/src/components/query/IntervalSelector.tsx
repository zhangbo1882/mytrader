// frontend/src/components/query/IntervalSelector.tsx
import { Select, Tag, Space, Tooltip } from 'antd';
import { ClockCircleOutlined, CheckCircleOutlined, DatabaseOutlined } from '@ant-design/icons';
import { useEffect, useState } from 'react';
import { stockService, type IntervalInfo } from '@/services/stockService';

export interface IntervalType {
  value: string;
  label: string;
}

interface IntervalSelectorProps {
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  style?: React.CSSProperties;
  className?: string;
  showAvailability?: boolean;
}

const INTERVAL_OPTIONS: IntervalType[] = [
  { label: '5分钟', value: '5m' },
  { label: '15分钟', value: '15m' },
  { label: '30分钟', value: '30m' },
  { label: '60分钟', value: '60m' },
  { label: '日线', value: '1d' },
];

export function IntervalSelector({
  value,
  onChange,
  disabled = false,
  style = { width: 120 },
  className,
  showAvailability = true,
}: IntervalSelectorProps) {
  const [intervalsInfo, setIntervalsInfo] = useState<Map<string, IntervalInfo>>(new Map());
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (!showAvailability) return;

    const fetchIntervals = async () => {
      setLoading(true);
      try {
        const response = await stockService.getAvailableIntervals();
        if (response.success && response.intervals) {
          const infoMap = new Map();
          response.intervals.forEach((info) => {
            infoMap.set(info.interval, info);
          });
          setIntervalsInfo(infoMap);
        }
      } catch (error) {
        console.error('Failed to fetch available intervals:', error);
      } finally {
        setLoading(false);
      }
    };

    fetchIntervals();
  }, [showAvailability]);

  // 生成选项，添加可用性标记和数据源信息
  const options = INTERVAL_OPTIONS.map((option) => {
    const info = intervalsInfo.get(option.value);
    const isAvailable = info?.available || false;

    // 构建数据源标签
    let sourceTags = null;
    if (showAvailability && !loading && isAvailable && info?.sources) {
      sourceTags = (
        <Space size={2}>
          {info.sources.includes('sqlite') && (
            <Tooltip title="SQLite数据库">
              <Tag color="blue" style={{ fontSize: '9px', margin: 0 }}>SQLite</Tag>
            </Tooltip>
          )}
          {info.sources.includes('duckdb') && (
            <Tooltip title="DuckDB数据库">
              <Tag color="green" style={{ fontSize: '9px', margin: 0 }}>DuckDB</Tag>
            </Tooltip>
          )}
        </Space>
      );
    }

    const label = (
      <Space size="small">
        <span>{option.label}</span>
        {showAvailability && !loading && (
          isAvailable ? (
            <>
              <CheckCircleOutlined style={{ color: '#52c41a', fontSize: '12px' }} />
              {sourceTags}
            </>
          ) : (
            <Tag color="default" style={{ fontSize: '10px', marginLeft: 4 }}>无数据</Tag>
          )
        )}
      </Space>
    );

    return {
      label,
      value: option.value,
      disabled: showAvailability && !loading && !isAvailable,
    };
  });

  return (
    <Select
      className={className}
      value={value}
      onChange={onChange}
      options={options}
      style={style}
      disabled={disabled}
      placeholder={loading ? '加载中...' : '选择周期'}
      suffixIcon={<ClockCircleOutlined />}
    />
  );
}

export default IntervalSelector;
