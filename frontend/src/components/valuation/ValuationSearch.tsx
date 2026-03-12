import { useState } from 'react';
import { AutoComplete, Button, Space } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import { stockService } from '@/services';

interface ValuationSearchProps {
  value: string;
  onChange: (value: string) => void;
  onSearch: () => void;
  loading?: boolean;
}

export function ValuationSearch({ value, onChange, onSearch, loading }: ValuationSearchProps) {
  const [options, setOptions] = useState<{ value: string; label: string }[]>([]);

  const handleSearch = async (text: string) => {
    if (!text || text.length < 2) {
      setOptions([]);
      return;
    }

    try {
      const result = await stockService.search(text) as any;
      const stockList = Array.isArray(result) ? result : (result?.stocks || []);

      setOptions(
        stockList.map((stock: any) => ({
          value: stock.code,
          label: `${stock.code} ${stock.name}`,
        }))
      );
    } catch (err) {
      // Ignore search errors
    }
  };

  return (
    <Space.Compact style={{ width: '100%', maxWidth: 500 }}>
      <AutoComplete
        value={value}
        onChange={(val) => {
          onChange(val);
        }}
        onSearch={handleSearch}
        options={options}
        placeholder="输入股票代码或名称（如：600382 或 茅台）"
        style={{ flex: 1 }}
        filterOption={false}
      />
      <Button
        type="primary"
        icon={<SearchOutlined />}
        onClick={onSearch}
        loading={loading}
      >
        估值
      </Button>
    </Space.Compact>
  );
}
