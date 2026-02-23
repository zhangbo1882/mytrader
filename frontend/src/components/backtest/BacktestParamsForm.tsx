import { useState, useEffect } from 'react';
import { Button, Form, InputNumber, Card, Space, message, Select, AutoComplete, Tag, Input, Spin, Alert } from 'antd';
import { PlayCircleOutlined, LoadingOutlined, CheckCircleOutlined } from '@ant-design/icons';
import { stockService, backtestService } from '@/services';
import { DatePicker } from 'antd';
import dayjs from 'dayjs';
import { StrategySelector } from './StrategySelector';
import { IntervalSelector } from '@/components/query/IntervalSelector';
import type { BacktestRequest, StrategySelectorValue, Stock } from '@/types';

const { RangePicker } = DatePicker;

interface BacktestParamsFormProps {
  onSubmit: (params: BacktestRequest) => void;
  loading?: boolean;
}

export function BacktestParamsForm({ onSubmit, loading }: BacktestParamsFormProps) {
  const [stock, setStock] = useState<string>('');
  const [stockOptions, setStockOptions] = useState<{ value: string; label: string; code: string; name: string }[]>([]);
  const [stockLoading, setStockLoading] = useState(false);
  const [dateRange, setDateRange] = useState<[string, string]>(['', '']);
  const [interval, setInterval] = useState<string>('1d');
  const [capital, setCapital] = useState(1000000);
  const [commission, setCommission] = useState(0.0002);
  const [benchmark, setBenchmark] = useState('');
  const [strategy, setStrategy] = useState<StrategySelectorValue>({
    strategy: 'price_breakout',
    strategy_params: {},
  });
  const [optimizedParamsLoading, setOptimizedParamsLoading] = useState(false);
  const [hasOptimizedParams, setHasOptimizedParams] = useState(false);

  // 当股票代码变化时，自动加载最优参数
  useEffect(() => {
    const loadOptimizedParams = async () => {
      if (!stock || stock.length < 4) {
        setHasOptimizedParams(false);
        return;
      }

      setOptimizedParamsLoading(true);
      try {
        // 检查是否有优化参数
        const hasParamsResponse = await backtestService.hasOptimizedParams(stock);
        if (hasParamsResponse.has_params) {
          // 获取最优参数
          const paramsResponse = await backtestService.getOptimizedParams(stock);

          // 更新策略参数
          setStrategy({
            strategy: 'price_breakout',
            strategy_params: paramsResponse.strategy_params || {}
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

  // 搜索股票
  const handleStockSearch = async (searchText: string) => {
    if (!searchText || searchText.length < 2) {
      setStockOptions([]);
      return;
    }

    setStockLoading(true);
    try {
      const result = await stockService.search(searchText);
      // 处理两种响应格式：{stocks: [...]} 或直接的数组
      const stockList = Array.isArray(result) ? result : (result?.stocks || []);

      setStockOptions(
        stockList.map((stock: Stock) => ({
          value: stock.code,
          label: `${stock.code} ${stock.name}`,
          code: stock.code,
          name: stock.name,
        }))
      );
    } catch (error) {
      console.error('Search error:', error);
      setStockOptions([]);
    } finally {
      setStockLoading(false);
    }
  };

  // 选择股票
  const handleStockSelect = (value: string, option: any) => {
    setStock(value);
    // 同时更新股票名称显示
    if (option && option.name) {
      // 可以在这里添加股票名称显示
    }
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
    } else {
      setDateRange(['', '']);
    }
  };

  // 根据搜索选项过滤标签显示
  const getSelectedStockName = () => {
    if (stock) {
      const selected = stockOptions.find(opt => opt.code === stock);
      return selected ? selected.name : '';
    }
    return '';
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
              onChange={(value) => setStock(value)}
              placeholder="输入股票代码或名称搜索（如：00941 或 中国移动）"
              style={{ width: '100%' }}
              notFoundContent={stockLoading ? <Spin indicator={<LoadingOutlined spin />} /> : '未找到股票'}
              filterOption={false}
              allowClear
            />

            {/* 已选择的股票标签 */}
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
