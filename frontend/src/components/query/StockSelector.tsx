import { useState, useEffect } from 'react';
import { AutoComplete, Spin, Tag, message } from 'antd';
import { LoadingOutlined } from '@ant-design/icons';
import { stockService } from '@/services';
import type { Stock } from '@/types';
import { useQueryStore } from '@/stores';
import { getDateRanges } from '@/utils';

interface StockSelectorProps {
  maxStocks?: number;
  placeholder?: string;
}

export function StockSelector({
  maxStocks = 10,
  placeholder = '输入股票代码或名称搜索（如：600382 或 茅台）',
}: StockSelectorProps) {
  const { symbols, setSymbols, dateRange, setDateRange } = useQueryStore();
  const [searchText, setSearchText] = useState('');
  const [options, setOptions] = useState<{ value: string; label: string; code: string; name: string }[]>(
    []
  );
  const [loading, setLoading] = useState(false);
  const [searchTimeout, setSearchTimeout] = useState<NodeJS.Timeout>();

  // 搜索股票
  useEffect(() => {
    if (!searchText || searchText.length < 2) {
      setOptions([]);
      return;
    }

    // 防抖处理
    if (searchTimeout) {
      clearTimeout(searchTimeout);
    }

    const timeout = setTimeout(async () => {
      setLoading(true);
      try {
        const result = await stockService.search(searchText);
        // 处理两种响应格式：{stocks: [...]} 或直接的数组
        const stockList = Array.isArray(result) ? result : (result?.stocks || []);

        setOptions(
          stockList.map((stock: Stock) => ({
            value: stock.code,
            label: `${stock.code} ${stock.name}`,
            code: stock.code,
            name: stock.name,
          }))
        );
      } catch (error) {
        console.error('Search error:', error);
      } finally {
        setLoading(false);
      }
    }, 300);

    setSearchTimeout(timeout);

    return () => clearTimeout(timeout);
  }, [searchText]);

  // 选择股票
  const handleSelect = (value: string, option: any) => {
    const exists = symbols.some((s) => s.code === option.code);

    if (!exists) {
      if (symbols.length >= maxStocks) {
        message.warning(`最多只能选择 ${maxStocks} 只股票`);
        return;
      }
      const newSymbols = [...symbols, { code: option.code, name: option.name }];
      setSymbols(newSymbols);

      // 如果是第一只股票且日期范围为空，自动设置默认日期范围（最近3个月）
      if (symbols.length === 0 && !dateRange.start && !dateRange.end) {
        const ranges = getDateRanges();
        const defaultRange = ranges['3M']; // 默认选择最近3个月
        setDateRange(defaultRange);
      }
    }

    setSearchText('');
    setOptions([]);
  };

  // 删除股票
  const handleRemove = (code: string) => {
    setSymbols(symbols.filter((s) => s.code !== code));
  };

  return (
    <div>
      <AutoComplete
        value={searchText}
        onChange={setSearchText}
        onSelect={handleSelect}
        options={options}
        placeholder={placeholder}
        style={{ width: '100%' }}
        notFoundContent={loading ? <Spin indicator={<LoadingOutlined spin />} /> : '未找到股票'}
        filterOption={false}
        allowClear
      />

      {/* 已选择的股票标签 */}
      {symbols.length > 0 && (
        <div style={{ marginTop: 8, display: 'flex', flexWrap: 'wrap', gap: 8 }}>
          {symbols.map((stock) => (
            <Tag
              key={stock.code}
              closable
              onClose={() => handleRemove(stock.code)}
              style={{ marginBottom: 4 }}
            >
              <strong>{stock.code}</strong> {stock.name}
            </Tag>
          ))}
          <span style={{ color: '#999', fontSize: 12 }}>
            已选 {symbols.length}/{maxStocks} 只
          </span>
        </div>
      )}
    </div>
  );
}
