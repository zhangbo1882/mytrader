import { useCallback, useEffect } from 'react';
import { Card, Space, Button, Alert, Divider, Typography, message } from 'antd';
import { SearchOutlined, ClearOutlined } from '@ant-design/icons';
import { StockSelector } from '@/components/query/StockSelector';
import { DateRangePicker } from '@/components/query/DateRangePicker';
import { QueryResults } from '@/components/query/QueryResults';
import { ExportButtons } from '@/components/query/ExportButtons';
import { useQueryStore } from '@/stores';
import { useQuery } from '@/hooks';
import { stockService } from '@/services';

const { Title, Text } = Typography;

function QueryPage() {
  const { symbols, dateRange, priceType, loading, error, results, clearResults } = useQueryStore();
  const { executeQuery } = useQuery();

  // 初始化最小日期
  useEffect(() => {
    const initMinDate = async () => {
      try {
        const result = await stockService.getMinDate();
        if (result.date) {
          // 可以设置默认日期范围为最近3个月
          // 这里暂时不设置，让用户自己选择
        }
      } catch (error) {
        console.error('Failed to get min date:', error);
      }
    };
    initMinDate();
  }, []);

  // 执行查询
  const handleQuery = useCallback(async () => {
    if (symbols.length === 0) {
      message.warning('请先选择至少一只股票');
      return;
    }

    if (!dateRange.start || !dateRange.end) {
      message.warning('请先选择日期范围');
      return;
    }

    try {
      await executeQuery({
        symbols: symbols.map((s) => s.code),
        startDate: dateRange.start,
        endDate: dateRange.end,
        priceType,
      });
    } catch (error) {
      console.error('Query error:', error);
    }
  }, [symbols, dateRange, priceType, executeQuery]);

  // 清空结果
  const handleClear = useCallback(() => {
    clearResults();
    message.success('已清空查询结果');
  }, [clearResults]);

  const hasResults = Object.keys(results).length > 0;
  const canQuery = symbols.length > 0 && dateRange.start && dateRange.end;

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>股票查询</Title>
      <Text type="secondary">选择股票和日期范围，查询历史行情数据</Text>

      <Divider />

      <Space direction="vertical" style={{ width: '100%' }} size="large">
        {/* 股票选择 */}
        <Card title="选择股票" size="small">
          <StockSelector maxStocks={10} />
        </Card>

        {/* 日期范围和复权类型 */}
        <Card title="选择日期范围" size="small">
          <DateRangePicker />
        </Card>

        {/* 操作按钮 */}
        <Card size="small">
          <Space>
            <Button
              type="primary"
              icon={<SearchOutlined aria-hidden="true" />}
              onClick={handleQuery}
              loading={loading}
              disabled={!canQuery}
            >
              查询数据
            </Button>
            <Button icon={<ClearOutlined aria-hidden="true" />} onClick={handleClear} disabled={!hasResults}>
              清空结果
            </Button>
            <ExportButtons disabled={!hasResults} />
          </Space>
        </Card>

        {/* 错误提示 */}
        {error && (
          <Alert
            message="查询失败"
            description={error}
            type="error"
            showIcon
            closable
            style={{ marginBottom: 16 }}
          />
        )}

        {/* 查询结果 */}
        {hasResults && <QueryResults results={results} loading={loading} />}
      </Space>
    </div>
  );
}

export default QueryPage;
