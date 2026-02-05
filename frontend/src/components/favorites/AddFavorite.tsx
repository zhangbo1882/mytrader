import { useState } from 'react';
import { Input, Button, Space, message, AutoComplete, Divider } from 'antd';
import { PlusOutlined, SearchOutlined } from '@ant-design/icons';
import { stockService } from '@/services';
import { useFavoriteStore } from '@/stores';

export function AddFavorite() {
  const [searchText, setSearchText] = useState('');
  const [options, setOptions] = useState<{ value: string; label: string; code: string; name: string }[]>(
    []
  );
  const [loading, setLoading] = useState(false);
  const { addFavorite, isInFavorites } = useFavoriteStore();

  // 搜索股票
  const handleSearch = async (value: string) => {
    setSearchText(value);

    if (!value || value.length < 2) {
      setOptions([]);
      return;
    }

    setLoading(true);
    try {
      const result = await stockService.search(value);
      const stockList = result.stocks || [];

      setOptions(
        stockList.map((stock: any) => ({
          value: `${stock.code} ${stock.name}`,
          code: stock.code,
          name: stock.name,
          label: (
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>
                <strong>{stock.code}</strong> {stock.name}
              </span>
              {!isInFavorites(stock.code) && (
                <Button size="small" type="primary" icon={<PlusOutlined aria-hidden="true" />}>
                  添加
                </Button>
              )}
            </div>
          ),
        }))
      );
    } catch (error) {
      console.error('Search error:', error);
    } finally {
      setLoading(false);
    }
  };

  // 选择股票
  const handleSelect = (value: string, option: any) => {
    if (isInFavorites(option.code)) {
      message.warning('该股票已在收藏列表中');
      return;
    }

    addFavorite(option.code, option.name);
    message.success(`已添加收藏：${option.name} (${option.code})`);
    setSearchText('');
    setOptions([]);
  };

  // 手动输入添加
  const handleManualAdd = () => {
    const code = searchText.trim();
    if (!code) {
      message.warning('请输入股票代码');
      return;
    }

    if (isInFavorites(code)) {
      message.warning('该股票已在收藏列表中');
      return;
    }

    addFavorite(code, '');
    message.success(`已添加收藏：${code}`);
    setSearchText('');
  };

  return (
    <div>
      <Space.Compact style={{ width: '100%' }}>
        <AutoComplete
          value={searchText}
          options={options}
          onSearch={handleSearch}
          onSelect={handleSelect}
          placeholder="输入股票代码或名称搜索（如：600382 或 茅台）"
          style={{ flex: 1 }}
          filterOption={false}
          notFoundContent={loading ? '搜索中...' : '未找到股票'}
        />
        <Button
          type="primary"
          icon={<PlusOutlined aria-hidden="true" />}
          onClick={handleManualAdd}
          disabled={!searchText.trim()}
        >
          添加
        </Button>
      </Space.Compact>

      <div style={{ marginTop: 8 }}>
        <span style={{ color: '#999', fontSize: 12 }}>
          提示：可以从搜索结果中选择，或直接输入股票代码添加
        </span>
      </div>
    </div>
  );
}
