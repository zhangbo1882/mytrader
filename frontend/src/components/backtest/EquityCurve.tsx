import { Card } from 'antd';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts';
import type { BacktestResult } from '@/types';

interface EquityCurveProps {
  result: BacktestResult;
}

interface CurvePoint {
  date: string;
  value: number;
  benchmark_value?: number;
}

export function EquityCurve({ result }: EquityCurveProps) {
  const portfolioValues = generatePortfolioValues(result);

  const formatYAxis = (value: number) => {
    return `¥${(value / 10000).toFixed(1)}万`;
  };

  const formatTooltip = (value: number | string, name: string) => {
    if (name === 'value' && typeof value === 'number') {
      return [`¥${value.toLocaleString()}`, '投资组合'];
    }
    if (name === 'benchmark_value' && typeof value === 'number') {
      return [`¥${value.toLocaleString()}`, '基准指数'];
    }
    return [String(value), name];
  };

  const hasBenchmark = portfolioValues.some((point) => point.benchmark_value !== undefined);

  return (
    <Card title="收益曲线">
      <ResponsiveContainer width="100%" height={400}>
        <LineChart data={portfolioValues} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 12 }}
            tickFormatter={(value) => String(value).substring(5)}
          />
          <YAxis tickFormatter={formatYAxis} />
          <Tooltip formatter={formatTooltip} />
          <Legend />
          <Line
            type="monotone"
            dataKey="value"
            name="投资组合"
            stroke="#1890ff"
            strokeWidth={2}
            dot={false}
          />
          {hasBenchmark && (
            <Line
              type="monotone"
              dataKey="benchmark_value"
              name="基准指数"
              stroke="#52c41a"
              strokeWidth={2}
              strokeDasharray="5 5"
              dot={false}
            />
          )}
        </LineChart>
      </ResponsiveContainer>
    </Card>
  );
}

function generatePortfolioValues(result: BacktestResult): CurvePoint[] {
  const values: CurvePoint[] = [];
  const { trades, basic_info } = result;

  let cash = basic_info.initial_cash;
  let holdings = 0;
  const startDate = new Date(basic_info.start_date);
  const endDate = new Date(basic_info.end_date);

  const sortedTrades = [...trades].sort((a, b) => a.buy_date.localeCompare(b.buy_date));

  const tradeMap = new Map<string, { type: 'buy' | 'sell'; price: number; size: number }>();
  sortedTrades.forEach((trade) => {
    tradeMap.set(trade.buy_date, { type: 'buy', price: trade.buy_price, size: trade.size });
    if (trade.sell_date && trade.sell_price != null) {
      tradeMap.set(trade.sell_date, { type: 'sell', price: trade.sell_price, size: trade.size });
    }
  });

  let currentDate = new Date(startDate);
  while (currentDate <= endDate) {
    const dateStr = currentDate.toISOString().split('T')[0];
    const trade = tradeMap.get(dateStr);

    if (trade) {
      if (trade.type === 'buy') {
        const cost = trade.price * trade.size;
        cash -= cost;
        holdings = trade.size;
      } else {
        const proceeds = trade.price * trade.size;
        cash += proceeds;
        holdings = 0;
      }
    }

    let portfolioValue = cash;
    if (holdings > 0) {
      const lastBuy = [...sortedTrades]
        .reverse()
        .find((item) => item.buy_date <= dateStr && (item.sell_date ?? '9999-12-31') > dateStr);
      if (lastBuy) {
        portfolioValue = cash + holdings * lastBuy.buy_price;
      }
    }

    values.push({
      date: dateStr,
      value: portfolioValue,
    });

    currentDate.setDate(currentDate.getDate() + 1);
  }

  return values;
}
