import { useEffect, useState } from 'react';
import { Alert, AutoComplete, Button, Card, Form, Input, InputNumber, message, Space, Spin, Tag } from 'antd';
import { DatePicker } from 'antd';
import { CheckCircleOutlined, LoadingOutlined, PlayCircleOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import { IntervalSelector } from '@/components/query/IntervalSelector';
import { stockService, backtestService } from '@/services';
import type { BacktestRequest, Stock, StrategySelectorValue } from '@/types';
import { StrategySelector } from './StrategySelector';

const { RangePicker } = DatePicker;

type StockOption = {
  value: string;
  label: string;
  code: string;
  name: string;
};

interface BacktestParamsFormProps {
  onSubmit: (params: BacktestRequest) => void;
  loading?: boolean;
}

export function BacktestParamsForm({ onSubmit, loading }: BacktestParamsFormProps) {
  const [stock, setStock] = useState('');
  const [stockOptions, setStockOptions] = useState<StockOption[]>([]);
  const [stockLoading, setStockLoading] = useState(false);
  const [dateRange, setDateRange] = useState<[string, string]>(['', '']);
  const [interval, setInterval] = useState('1d');
  const [capital, setCapital] = useState(1000000);
  const [commission, setCommission] = useState(0.0002);
  const [benchmark, setBenchmark] = useState('');
  const [strategy, setStrategy] = useState<StrategySelectorValue>({
    strategy: 'price_breakout',
    strategy_params: {},
  });
  const [optimizedParamsLoading, setOptimizedParamsLoading] = useState(false);
  const [hasOptimizedParams, setHasOptimizedParams] = useState(false);

  useEffect(() => {
    const loadOptimizedParams = async () => {
      if (!stock || stock.length < 4) {
        setHasOptimizedParams(false);
        return;
      }

      setOptimizedParamsLoading(true);
      try {
        const hasParamsResponse = await backtestService.hasOptimizedParams(stock);
        if (hasParamsResponse.has_params) {
          const paramsResponse = await backtestService.getOptimizedParams(stock);
          setStrategy({
            strategy: paramsResponse.strategy || 'price_breakout',
            strategy_params: paramsResponse.strategy_params || {},
          });
          setHasOptimizedParams(true);
          message.success(`已加载 ${stock} 的最优参数（更新于 ${paramsResponse.updated_at}）`);
        } else {
          setHasOptimizedParams(false);
        }
      } catch (error) {
        console.error('Failed to load optimized params:', error);
        setHasOptimizedParams(false);
      } finally {
        setOptimizedParamsLoading(false);
      }
    };

    loadOptimizedParams();
  }, [stock]);

  const handleStockSearch = async (searchText: string) => {
    if (!searchText || searchText.length < 2) {
      setStockOptions([]);
      return;
    }

    setStockLoading(true);
    try {
      const result = await stockService.search(searchText);
      const stockList = Array.isArray(result) ? result : (result?.stocks || []);

      setStockOptions(
        stockList.map((item: Stock) => ({
          value: item.code,
          label: `${item.code} ${item.name}`,
          code: item.code,
          name: item.name,
        }))
      );
    } catch (error) {
      console.error('Search error:', error);
      setStockOptions([]);
    } finally {
      setStockLoading(false);
    }
  };

  const handleStockSelect = (value: string) => {
    setStock(value);
  };

  const handleSubmit = () => {
    if (!stock) {
      message.warning('请输入或选择股票代码');
      return;
    }
    if (!dateRange[0] || !dateRange[1]) {
      message.warning('请选择日期范围');
      return;
    }

    onSubmit({
      stock,
      start_date: dateRange[0],
      end_date: dateRange[1],
      interval,
      cash: capital,
      commission,
      benchmark: benchmark || undefined,
      strategy: strategy.strategy,
      strategy_params: strategy.strategy_params,
    });
  };

  const handleDateChange = (dates: null | [dayjs.Dayjs | null, dayjs.Dayjs | null]) => {
    if (dates && dates[0] && dates[1]) {
      setDateRange([dates[0].format('YYYY-MM-DD'), dates[1].format('YYYY-MM-DD')]);
      return;
    }
    setDateRange(['', '']);
  };

  const getSelectedStockName = () => {
    if (!stock) {
      return '';
    }
    const selected = stockOptions.find((option) => option.code === stock);
    return selected ? selected.name : '';
  };

  return (
    <Card title="回测参数配置">
      <Form layout="vertical">
        <Form.Item label="股票代码" required>
          <Space direction="vertical" style={{ width: '100%' }}>
            <AutoComplete
              value={stock}
              options={stockOptions}
              onSearch={handleStockSearch}
              onSelect={handleStockSelect}
              onChange={setStock}
              placeholder="输入股票代码或名称搜索（如：00941 或 中国移动）"
              style={{ width: '100%' }}
              notFoundContent={stockLoading ? <Spin indicator={<LoadingOutlined spin />} /> : '未找到股票'}
              filterOption={false}
              allowClear
            />

            {stock && (
              <Tag color="blue" closable onClose={() => setStock('')} style={{ marginTop: 4 }}>
                <strong>{stock}</strong> {getSelectedStockName()}
              </Tag>
            )}
          </Space>
        </Form.Item>

        <Form.Item label="日期范围" required>
          <RangePicker
            value={dateRange[0] && dateRange[1] ? [dayjs(dateRange[0]), dayjs(dateRange[1])] : null}
            onChange={handleDateChange}
            style={{ width: '100%' }}
            format="YYYY-MM-DD"
            placeholder={['开始日期', '结束日期']}
          />
        </Form.Item>

        <Form.Item label="时间周期">
          <IntervalSelector
            value={interval}
            onChange={setInterval}
            showAvailability={true}
            style={{ width: 150 }}
          />
        </Form.Item>

        <Space size="large" style={{ marginBottom: 16 }}>
          <Form.Item label="初始资金" style={{ marginBottom: 0 }}>
            <InputNumber
              value={capital}
              onChange={(val) => setCapital(val || 1000000)}
              min={10000}
              max={100000000}
              step={10000}
              style={{ width: 200 }}
              formatter={(value) => `¥ ${value}`.replace(/\B(?=(\d{3})+(?!\d))/g, ',')}
            />
          </Form.Item>

          <Form.Item label="手续费率" style={{ marginBottom: 0 }}>
            <InputNumber
              value={commission}
              onChange={(val) => setCommission(val || 0.0002)}
              min={0.0001}
              max={0.01}
              step={0.0001}
              style={{ width: 150 }}
              formatter={(value) => `${(Number(value) * 100).toFixed(3)}%`}
              parser={(displayValue) => {
                const parsed = parseFloat(displayValue?.replace('%', '') || '0');
                return parsed / 100;
              }}
            />
          </Form.Item>
        </Space>

        <Form.Item label="基准指数 (可选)">
          <Input
            value={benchmark}
            onChange={(e) => setBenchmark(e.target.value)}
            placeholder="输入指数代码，如：000300（沪深300）"
          />
        </Form.Item>

        <Form.Item label="策略配置" required>
          <Space direction="vertical" style={{ width: '100%' }}>
            {hasOptimizedParams && (
              <Alert
                message="已从数据库加载该股票的最优参数"
                type="success"
                showIcon
                icon={<CheckCircleOutlined />}
                style={{ marginBottom: 12 }}
              />
            )}
            {optimizedParamsLoading && (
              <Alert
                message="正在从数据库加载最优参数..."
                type="info"
                showIcon
                style={{ marginBottom: 12 }}
              />
            )}
            <StrategySelector
              value={strategy}
              onChange={setStrategy}
              disabled={optimizedParamsLoading}
            />
          </Space>
        </Form.Item>

        <Button
          type="primary"
          icon={<PlayCircleOutlined />}
          onClick={handleSubmit}
          loading={loading}
          size="large"
        >
          运行回测
        </Button>
      </Form>
    </Card>
  );
}
