import { useState, useEffect } from 'react';
import { AutoComplete, Spin, Tag, message } from 'antd';
import { LoadingOutlined, HistoryOutlined } from '@ant-design/icons';
import { stockService } from '@/services';
import type { Stock } from '@/types';
import { useQueryStore } from '@/stores';
import { getDateRanges } from '@/utils';

interface StockSelectorProps {
  placeholder?: string;
}

const HISTORY_KEY = 'stock_search_history';
const MAX_HISTORY = 20;

// 历史记录类型
interface HistoryItem {
  code: string;
  name: string;
  time: number;
}

// 获取历史记录
const getHistory = (): HistoryItem[] => {
  try {
    const data = localStorage.getItem(HISTORY_KEY);
    return data ? JSON.parse(data) : [];
  } catch {
    return [];
  }
};

// 保存历史记录
const saveHistory = (items: HistoryItem[]) => {
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(items.slice(0, MAX_HISTORY)));
  } catch {
    // ignore
  }
};

// 添加到历史记录
const addToHistory = (code: string, name: string) => {
  const history = getHistory();
  // 移除已存在的
  const filtered = history.filter(h => h.code !== code);
  // 添加到开头
  const newHistory = [{ code, name, time: Date.now() }, ...filtered];
  saveHistory(newHistory);
};

export function StockSelector({
  placeholder = '输入股票代码或名称搜索（如：600382 或 茅台）',
}: StockSelectorProps) {
  const { symbols, setSymbols, dateRange, setDateRange } = useQueryStore();
  const [searchText, setSearchText] = useState('');
  const [options, setOptions] = useState<{ value: string; label: React.ReactNode; code: string; name: string }[]>(
    []
  );
  const [loading, setLoading] = useState(false);
  const [searchTimeout, setSearchTimeout] = useState<NodeJS.Timeout>();
  const [history, setHistory] = useState<HistoryItem[]>([]);

  // 当前选中的股票
  const selectedStock = symbols[0];

  // 加载历史记录
  useEffect(() => {
    setHistory(getHistory());
  }, []);

  // 搜索股票
  useEffect(() => {
    // 如果已选择股票，不显示搜索结果
    if (selectedStock) {
      setOptions([]);
      return;
    }

    if (!searchText || searchText.length < 2) {
      // 显示历史记录
      if (searchText.length === 0 && history.length > 0) {
        const historyOptions = history.slice(0, 10).map(h => ({
          value: h.code,
          label: (
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <HistoryOutlined style={{ color: '#999' }} />
              <span><strong>{h.code}</strong> {h.name}</span>
            </div>
          ),
          code: h.code,
          name: h.name,
        }));
        setOptions(historyOptions);
      } else {
        setOptions([]);
      }
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
  }, [searchText, history, selectedStock]);

  // 选择股票
  const handleSelect = (value: string, option: any) => {
    // 只保留一只股票
    setSymbols([{ code: option.code, name: option.name }]);

    // 添加到历史记录
    addToHistory(option.code, option.name);
    setHistory(getHistory());

    // 如果日期范围为空，自动设置默认日期范围（最近3个月）
    if (!dateRange.start && !dateRange.end) {
      const ranges = getDateRanges();
      const defaultRange = ranges['3M'];
      setDateRange(defaultRange);
    }

    setSearchText('');
    setOptions([]);
  };

  // 删除股票
  const handleRemove = () => {
    setSymbols([]);
  };

  // 清除历史记录
  const clearHistory = () => {
    localStorage.removeItem(HISTORY_KEY);
    setHistory([]);
    setOptions([]);
  };

  // 如果已选择股票，显示标签
  if (selectedStock) {
    return (
      <div>
        <Tag
          closable
          onClose={handleRemove}
          style={{ fontSize: 14, padding: '4px 8px' }}
        >
          <strong>{selectedStock.code}</strong> {selectedStock.name}
        </Tag>
      </div>
    );
  }

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
        autoFocus
      />

      {/* 历史记录提示 */}
      {history.length > 0 && searchText.length === 0 && (
        <div style={{ marginTop: 4, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: '#999', fontSize: 12 }}>
            <HistoryOutlined style={{ marginRight: 4 }} />
            输入为空时显示最近查询的股票
          </span>
          <a
            style={{ fontSize: 12, color: '#999', cursor: 'pointer' }}
            onClick={clearHistory}
          >
            清除历史
          </a>
        </div>
      )}
    </div>
  );
}
