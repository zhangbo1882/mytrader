import { Card, Empty } from 'antd';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ReferenceLine
} from 'recharts';
import type { ValuationResult } from '@/types';

interface ModelComparisonChartProps {
  results: ValuationResult[];
}

export function ModelComparisonChart({ results }: ModelComparisonChartProps) {
  if (!results || results.length === 0) {
    return <Empty description="暂无数据" />;
  }

  // Prepare chart data
  const data = results.map(r => ({
    model: r.model.replace('Relative_', '').replace('_', ' '),
    fairValue: r.fair_value,
    currentPrice: r.current_price,
    upside: r.upside_downside,
    confidence: r.confidence * 100
  }));

  return (
    <Card title="模型估值对比">
      <ResponsiveContainer width="100%" height={350}>
        <BarChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="model"
            tick={{ fontSize: 12 }}
            angle={-30}
            textAnchor="end"
            height={60}
          />
          <YAxis
            label={{ value: '价格 (¥)', position: 'insideLeft', angle: -90 }}
            tick={{ fontSize: 12 }}
          />
          <Tooltip
            formatter={(value: any, name: any) => {
              if (name === 'upside') return [`${value.toFixed(2)}%`, '上下空间'];
              if (name === 'confidence') return [`${value.toFixed(0)}%`, '置信度'];
              return [`¥${value.toFixed(2)}`, name === 'fairValue' ? '合理价值' : '当前价格'];
            }}
          />
          <Legend />
          <ReferenceLine
            y={results[0].current_price}
            stroke="#999"
            strokeDasharray="3 3"
            label="当前价格"
          />
          <Bar dataKey="fairValue" name="合理价值" fill="#1890ff" />
          <Bar dataKey="currentPrice" name="当前价格" fill="#52c41a" />
        </BarChart>
      </ResponsiveContainer>
    </Card>
  );
}
