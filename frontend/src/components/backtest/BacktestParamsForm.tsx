import { useState } from 'react';
import { Button, Form, InputNumber, Card, Space, Input, message } from 'antd';
import { PlayCircleOutlined } from '@ant-design/icons';
import { stockService } from '@/services';
import { DatePicker } from 'antd';
import dayjs from 'dayjs';
import { StrategySelector } from './StrategySelector';
import type { BacktestRequest, StrategySelectorValue } from '@/types';

const { RangePicker } = DatePicker;

interface BacktestParamsFormProps {
  onSubmit: (params: BacktestRequest) => void;
  loading?: boolean;
}

export function BacktestParamsForm({ onSubmit, loading }: BacktestParamsFormProps) {
  const [stock, setStock] = useState<string>('');
  const [stockName, setStockName] = useState<string>('');
  const [dateRange, setDateRange] = useState<[string, string]>(['', '']);
  const [capital, setCapital] = useState(1000000);
  const [commission, setCommission] = useState(0.0002);
  const [benchmark, setBenchmark] = useState('');
  const [strategy, setStrategy] = useState<StrategySelectorValue>({
    strategy: 'sma_cross',
    strategy_params: {},
  });

  const handleSubmit = () => {
    if (!stock) {
      message.warning('请输入股票代码');
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

  return (
    <Card title="回测参数配置">
      <Form layout="vertical">
        <Form.Item label="股票代码" required>
          <Input
            value={stock}
            onChange={(e) => setStock(e.target.value)}
            placeholder="输入股票代码，如：600382"
            suffix={stockName && <span style={{ color: '#999' }}>{stockName}</span>}
            onBlur={async () => {
              if (stock && stock.length >= 6) {
                try {
                  const result = await stockService.search(stock) as any;
                  const stockList = Array.isArray(result) ? result : (result?.stocks || []);
                  const found = stockList.find((s: any) => s.code === stock);
                  if (found) {
                    setStockName(found.name);
                  }
                } catch (err) {
                  // Ignore
                }
              }
            }}
          />
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
          <StrategySelector value={strategy} onChange={setStrategy} />
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
