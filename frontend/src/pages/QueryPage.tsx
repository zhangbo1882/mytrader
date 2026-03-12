import { useCallback, useEffect } from 'react';
import { Card, Space, Button, Alert, Divider, Typography, message, Modal, Row, Col } from 'antd';
import { SearchOutlined, ClearOutlined, StarOutlined } from '@ant-design/icons';
import { StockSelector } from '@/components/query/StockSelector';
import { DateRangePicker } from '@/components/query/DateRangePicker';
import { IntervalSelector } from '@/components/query/IntervalSelector';
import { QueryResults } from '@/components/query/QueryResults';
import { ExportButtons } from '@/components/query/ExportButtons';
import { useQueryStore } from '@/stores';
import { useFavoriteStore } from '@/stores';
import { useQuery } from '@/hooks';
import { stockService } from '@/services';
import type { IntervalType } from '@/types';

const { Title, Text } = Typography;

function QueryPage() {
  const { symbols, dateRange, priceType, interval, setInterval, loading, error, results, clearResults } = useQueryStore();
  const { executeQuery } = useQuery();
  const { batchAddFavorites, isInFavorites } = useFavoriteStore();

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
        interval,
        priceType,
      });
    } catch (error) {
      console.error('Query error:', error);
    }
  }, [symbols, dateRange, interval, priceType, executeQuery]);

  // 清空结果
  const handleClear = useCallback(() => {
    clearResults();
    message.success('已清空查询结果');
  }, [clearResults]);

  // 批量收藏查询结果
  const handleBatchAddQueryResults = useCallback(async () => {
    if (symbols.length === 0) {
      message.warning('没有可收藏的股票');
      return;
    }

    const stockCodes = symbols.map((s) => s.code);
    const newCodes = stockCodes.filter((code) => !isInFavorites(code));

    if (newCodes.length === 0) {
      message.warning('所有股票已在收藏列表中');
      return;
    }

    Modal.confirm({
      title: '确认批量收藏',
      content: `确定要将查询的 ${symbols.length} 只股票添加到收藏列表吗？其中 ${newCodes.length} 只为新股票。`,
      okText: '确定',
      cancelText: '取消',
      onOk: async () => {
        try {
          const response = await batchAddFavorites(newCodes);
          if (response.failed === 0) {
            message.success(`成功收藏 ${response.success} 只股票`);
          } else {
            message.warning(
              `收藏完成：成功 ${response.success} 只，失败 ${response.failed} 只`
            );
          }
        } catch (error) {
          message.error(error instanceof Error ? error.message : '批量收藏失败');
        }
      },
    });
  }, [symbols, isInFavorites, batchAddFavorites]);

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
          <StockSelector />
        </Card>

        {/* 日期范围和复权类型 */}
        <Card title="选择日期范围" size="small">
          <Space direction="vertical" style={{ width: '100%' }}>
            <Row gutter={16}>
              <Col>
                <Text>时间周期：</Text>
                <IntervalSelector
                  value={interval}
                  onChange={(val) => setInterval(val as IntervalType)}
                  style={{ width: 120, marginLeft: 8 }}
                />
              </Col>
            </Row>
            <DateRangePicker />
          </Space>
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
            <Button
              icon={<StarOutlined aria-hidden="true" />}
              onClick={handleBatchAddQueryResults}
              disabled={symbols.length === 0}
            >
              批量收藏
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
