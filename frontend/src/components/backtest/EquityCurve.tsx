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

export function EquityCurve({ result }: EquityCurveProps) {
  // Generate portfolio values from trades if not provided
  const portfolioValues = result.portfolio_values || generatePortfolioValues(result);

  const formatYAxis = (value: number) => {
    return `¥${(value / 10000).toFixed(1)}万`;
  };

  const formatTooltip = (value: any, name: any) => {
    if (name === 'portfolio_value') {
      return [`¥${value.toLocaleString()}`, '投资组合'];
    }
    if (name === 'benchmark_value') {
      return [`¥${value.toLocaleString()}`, '基准指数'];
    }
    return [value, name];
  };

  const hasBenchmark = portfolioValues.some(v => v.benchmark_value !== undefined);

  return (
    <Card title="收益曲线">
      <ResponsiveContainer width="100%" height={400}>
        <LineChart data={portfolioValues} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 12 }}
            tickFormatter={(value) => value.substring(5)} // Show MM-DD only
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

// Helper function to generate portfolio values from trades
function generatePortfolioValues(result: BacktestResult) {
  const values: Array<{ date: string; value: number; benchmark_value?: number }> = [];
  const { trades, basic_info } = result;

  let cash = basic_info.initial_cash;
  let holdings = 0;
  const startDate = new Date(basic_info.start_date);
  const endDate = new Date(basic_info.end_date);

  // Sort trades by buy date
  const sortedTrades = [...trades].sort((a, b) => a.buy_date.localeCompare(b.buy_date));

  // Create a map of dates to portfolio values
  const tradeMap = new Map<string, { type: 'buy' | 'sell'; price: number; size: number }>();
  sortedTrades.forEach(trade => {
    tradeMap.set(trade.buy_date, { type: 'buy', price: trade.buy_price, size: trade.size });
    tradeMap.set(trade.sell_date, { type: 'sell', price: trade.sell_price, size: trade.size });
  });

  // Generate daily values
  let currentDate = new Date(startDate);
  while (currentDate <= endDate) {
    const dateStr = currentDate.toISOString().split('T')[0];
    const trade = tradeMap.get(dateStr);

    if (trade) {
      if (trade.type === 'buy') {
        const cost = trade.price * trade.size;
        cash -= cost;
        holdings = trade.size;
      } else if (trade.type === 'sell') {
        const proceeds = trade.price * trade.size;
        cash += proceeds;
        holdings = 0;
      }
    }

    // Calculate portfolio value (use last trade price if holding, or cash if not)
    let portfolioValue = cash;
    if (holdings > 0) {
      // Find the most recent buy price
      const lastBuy = [...sortedTrades]
        .reverse()
        .find(t => t.buy_date <= dateStr && t.sell_date > dateStr);
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
