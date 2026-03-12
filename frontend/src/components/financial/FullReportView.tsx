import { Card, Row, Col, Empty } from 'antd';
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
import type { FinancialReport } from '@/types';

interface FullReportViewProps {
  report: FinancialReport;
  loading?: boolean;
}

// 格式化大数字
const formatAmount = (value: number | string) => {
  const num = Number(value);
  if (Number.isNaN(num)) return '-';

  if (Math.abs(num) >= 100000000) {
    return `${(num / 100000000).toFixed(2)}亿`;
  }

  if (Math.abs(num) >= 10000) {
    return `${(num / 10000).toFixed(2)}万`;
  }

  return num.toFixed(2);
};

// 格式化百分比
const formatPercent = (value: number) => {
  if (Number.isNaN(value)) return '-';
  return `${value.toFixed(2)}%`;
};

// 格式化日期显示
const formatDate = (dateStr: string) => {
  if (!dateStr || dateStr.length < 8) return dateStr;
  // 20241231 -> 24Q4
  const year = dateStr.substring(2, 4);
  const month = parseInt(dateStr.substring(4, 6), 10);
  const quarter = Math.ceil(month / 3);
  return `${year}Q${quarter}`;
};

// 将数据转换为图表格式
const transformDataForChart = (
  items: Array<{ item: string; value: number; date: string }>,
  itemNames: string[]
) => {
  // 按日期分组
  const dateMap = new Map<string, Record<string, number>>();

  items.forEach((item) => {
    if (!itemNames.includes(item.item)) return;

    const dateStr = String(item.date);
    if (!dateMap.has(dateStr)) {
      dateMap.set(dateStr, { date: dateStr } as any);
    }
    const dateData = dateMap.get(dateStr)!;
    dateData[item.item] = item.value;
  });

  // 转换为数组并按日期升序排序
  return Array.from(dateMap.values()).sort((a, b) => String(a.date).localeCompare(String(b.date)));
};

// 单个折线图组件
interface ChartCardProps {
  title: string;
  data: Array<Record<string, any>>;
  lines: { key: string; color: string; name: string }[];
  formatValue?: (value: number) => string;
  loading?: boolean;
}

function ChartCard({ title, data, lines, formatValue, loading }: ChartCardProps) {
  if (!data || data.length === 0) {
    return (
      <Card title={title} size="small">
        <Empty description="暂无数据" image={Empty.PRESENTED_IMAGE_SIMPLE} />
      </Card>
    );
  }

  const defaultFormat = (v: number) => formatAmount(v);
  const formatter = formatValue || defaultFormat;

  return (
    <Card title={title} size="small" loading={loading}>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 5, right: 30, left: 10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis
            dataKey="date"
            tickFormatter={formatDate}
            tick={{ fontSize: 11 }}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fontSize: 11 }}
            tickFormatter={(v) => {
              if (Math.abs(v) >= 100000000) return `${(v / 100000000).toFixed(0)}亿`;
              if (Math.abs(v) >= 10000) return `${(v / 10000).toFixed(0)}万`;
              return v.toFixed(0);
            }}
            width={70}
          />
          <Tooltip
            formatter={(value: number, name: string) => [formatter(value), name]}
            labelFormatter={(label) => `日期: ${formatDate(String(label))}`}
          />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          {lines.map((line) => (
            <Line
              key={line.key}
              type="monotone"
              dataKey={line.key}
              name={line.name}
              stroke={line.color}
              strokeWidth={2}
              dot={{ r: 3 }}
              activeDot={{ r: 5 }}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </Card>
  );
}

export function FullReportView({ report, loading }: FullReportViewProps) {
  if (!report) {
    return (
      <Card>
        <Empty description="暂无财务数据" />
      </Card>
    );
  }

  // 利润表项目配置
  const incomeItems = ['营业收入', '营业成本', '营业利润', '净利润'];
  const incomeData = transformDataForChart(report.income || [], incomeItems);
  const incomeLines = [
    { key: '营业收入', name: '营业收入', color: '#1890ff' },
    { key: '营业成本', name: '营业成本', color: '#ff7875' },
    { key: '营业利润', name: '营业利润', color: '#52c41a' },
    { key: '净利润', name: '净利润', color: '#faad14' },
  ];

  // 资产负债表项目配置
  const balanceItems = ['资产总计', '负债合计', '所有者权益合计'];
  const balanceData = transformDataForChart(report.balance || [], balanceItems);
  const balanceLines = [
    { key: '资产总计', name: '资产总计', color: '#1890ff' },
    { key: '负债合计', name: '负债合计', color: '#ff7875' },
    { key: '所有者权益合计', name: '所有者权益', color: '#52c41a' },
  ];

  // 现金流量表项目配置
  const cashflowItems = ['经营活动现金流量小计', '投资活动现金流量小计', '筹资活动现金流量小计', '期末现金及现金等价物余额'];
  const cashflowData = transformDataForChart(report.cashflow || [], cashflowItems);
  const cashflowLines = [
    { key: '经营活动现金流量小计', name: '经营活动', color: '#1890ff' },
    { key: '投资活动现金流量小计', name: '投资活动', color: '#ff7875' },
    { key: '筹资活动现金流量小计', name: '筹资活动', color: '#52c41a' },
    { key: '期末现金及现金等价物余额', name: '期末现金', color: '#722ed1' },
  ];

  // 财务指标 - 盈利能力
  const profitabilityItems = ['净资产收益率', '销售毛利率', '销售净利率'];
  const profitabilityData = transformDataForChart(
    (report.indicators || []).filter((i) => profitabilityItems.includes(i.item)),
    profitabilityItems
  );
  const profitabilityLines = [
    { key: '净资产收益率', name: 'ROE', color: '#1890ff' },
    { key: '销售毛利率', name: '毛利率', color: '#52c41a' },
    { key: '销售净利率', name: '净利率', color: '#faad14' },
  ];

  // 财务指标 - 成长能力
  const growthItems = ['营业收入增长率', '净利润增长率'];
  const growthData = transformDataForChart(
    (report.indicators || []).filter((i) => growthItems.includes(i.item)),
    growthItems
  );
  const growthLines = [
    { key: '营业收入增长率', name: '营收增长', color: '#1890ff' },
    { key: '净利润增长率', name: '利润增长', color: '#52c41a' },
  ];

  // 估值指标 - PE/PB
  const valuationPePbItems = ['PE_TTM', 'PB'];
  const valuationPePbData = transformDataForChart(
    (report.valuation || []).filter((i) => valuationPePbItems.includes(i.item)),
    valuationPePbItems
  );
  const valuationPePbLines = [
    { key: 'PE_TTM', name: 'PE(TTM)', color: '#1890ff' },
    { key: 'PB', name: 'PB', color: '#52c41a' },
  ];

  // 估值指标 - 市值
  const valuationMvItems = ['总市值'];
  const valuationMvData = transformDataForChart(
    (report.valuation || []).filter((i) => valuationMvItems.includes(i.item)),
    valuationMvItems
  );
  const valuationMvLines = [
    { key: '总市值', name: '总市值', color: '#722ed1' },
  ];

  // 百分比格式化器
  const percentFormatter = (v: number) => formatPercent(v);

  // 市值格式化器（亿元）
  const mvFormatter = (v: number) => `${v.toFixed(2)}亿`;

  return (
    <div>
      {/* 利润表趋势 */}
      <div style={{ marginBottom: 16 }}>
        <ChartCard
          title="利润表趋势"
          data={incomeData}
          lines={incomeLines}
          loading={loading}
        />
      </div>

      {/* 资产负债表趋势 */}
      <div style={{ marginBottom: 16 }}>
        <ChartCard
          title="资产负债表趋势"
          data={balanceData}
          lines={balanceLines}
          loading={loading}
        />
      </div>

      {/* 现金流量表趋势 */}
      <div style={{ marginBottom: 16 }}>
        <ChartCard
          title="现金流量表趋势"
          data={cashflowData}
          lines={cashflowLines}
          loading={loading}
        />
      </div>

      {/* 财务指标趋势 */}
      <Row gutter={16}>
        <Col xs={24} md={12}>
          <div style={{ marginBottom: 16 }}>
            <ChartCard
              title="盈利能力趋势"
              data={profitabilityData}
              lines={profitabilityLines}
              formatValue={percentFormatter}
              loading={loading}
            />
          </div>
        </Col>
        <Col xs={24} md={12}>
          <div style={{ marginBottom: 16 }}>
            <ChartCard
              title="成长能力趋势"
              data={growthData}
              lines={growthLines}
              formatValue={percentFormatter}
              loading={loading}
            />
          </div>
        </Col>
      </Row>

      {/* 估值趋势 */}
      <Row gutter={16}>
        <Col xs={24} md={12}>
          <div style={{ marginBottom: 16 }}>
            <ChartCard
              title="估值趋势 (PE/PB)"
              data={valuationPePbData}
              lines={valuationPePbLines}
              loading={loading}
            />
          </div>
        </Col>
        <Col xs={24} md={12}>
          <div style={{ marginBottom: 16 }}>
            <ChartCard
              title="市值趋势"
              data={valuationMvData}
              lines={valuationMvLines}
              formatValue={mvFormatter}
              loading={loading}
            />
          </div>
        </Col>
      </Row>
    </div>
  );
}
