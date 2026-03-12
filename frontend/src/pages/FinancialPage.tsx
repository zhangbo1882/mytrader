import { useState } from 'react';
import {
  Card,
  Input,
  Button,
  Typography,
  Divider,
  Space,
  Alert,
  Row,
  Col,
  message,
} from 'antd';
import {
  DollarOutlined,
  DownloadOutlined,
  BarChartOutlined,
} from '@ant-design/icons';
import { FullReportView } from '@/components/financial/FullReportView';
import { useFinancialData } from '@/hooks';
import { formatDate } from '@/utils';

const { Title, Text } = Typography;
const { Search } = Input;

function FinancialPage() {
  const [stockCode, setStockCode] = useState('');
  const { summary, report, loading, error, fetchSummary, fetchReport, exportData } =
    useFinancialData();

  const handleSearch = async () => {
    if (!stockCode.trim()) {
      message.warning('请输入股票代码');
      return;
    }

    // 同时获取摘要和完整报表
    await Promise.all([
      fetchSummary(stockCode.trim()),
      fetchReport(stockCode.trim()),
    ]);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const handleExport = async (format: 'csv' | 'excel') => {
    if (!stockCode.trim()) {
      message.warning('请先输入股票代码查询');
      return;
    }

    await exportData(stockCode.trim(), format);
  };

  const hasData = Boolean(summary && summary.indicators && summary.indicators.length > 0);

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>
        <DollarOutlined style={{ marginRight: 8 }} aria-hidden="true" />
        财务数据
      </Title>
      <Text type="secondary">查看上市公司的财务报表和关键指标</Text>

      <Divider />

      {/* 搜索栏 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Space.Compact style={{ width: '100%', maxWidth: 600 }}>
          <Search
            placeholder="输入股票代码查询财务数据（如：600382）"
            value={stockCode}
            onChange={(e) => setStockCode(e.target.value)}
            onKeyDown={handleKeyDown}
            onSearch={handleSearch}
            enterButton="查询"
            size="large"
            loading={loading}
            maxLength={6}
          />
        </Space.Compact>

        {hasData && (
          <Space style={{ marginLeft: 16 }}>
            <Button icon={<DownloadOutlined aria-hidden="true" />} onClick={() => handleExport('excel')}>
              导出Excel
            </Button>
          </Space>
        )}
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

      {/* 内容区域 */}
      {hasData ? (
        <div>
          {/* 基本信息 */}
          <Card size="small" style={{ marginBottom: 16 }}>
            <Row gutter={16}>
              <Col span={6}>
                <div>
                  <Text type="secondary">股票代码</Text>
                  <div style={{ fontSize: 20, fontWeight: 'bold', marginTop: 4 }}>
                    {summary?.stockCode}
                  </div>
                </div>
              </Col>
              <Col span={6}>
                <div>
                  <Text type="secondary">股票名称</Text>
                  <div style={{ fontSize: 20, fontWeight: 'bold', marginTop: 4 }}>
                    {summary?.stockName}
                  </div>
                </div>
              </Col>
              <Col span={12}>
                <div>
                  <Text type="secondary">更新时间</Text>
                  <div style={{ fontSize: 14, marginTop: 4 }}>
                    {summary?.updateTime ? formatDate(summary.updateTime, 'YYYY-MM-DD HH:mm:ss') : '-'}
                  </div>
                </div>
              </Col>
            </Row>
          </Card>

          {/* 完整报表（折线图展示） */}
          {report && <FullReportView report={report} loading={loading} />}
        </div>
      ) : (
        <Card>
          <div style={{ textAlign: 'center', padding: '60px 0', color: '#999' }}>
            <BarChartOutlined style={{ fontSize: 64, marginBottom: 16 }} aria-hidden="true" />
            <div style={{ fontSize: 16, marginBottom: 8 }}>查询财务数据</div>
            <div style={{ fontSize: 14 }}>请输入股票代码（如：600382）查询该公司的财务报表</div>
          </div>
        </Card>
      )}

      {/* 使用说明 */}
      {!hasData && (
        <Card title={<Text strong>使用说明</Text>} style={{ marginTop: 16 }}>
          <div style={{ lineHeight: 1.8 }}>
            <p>
              <Text strong>财务数据包括：</Text>
            </p>
            <ul style={{ paddingLeft: 20 }}>
              <li>利润表趋势：营业收入、营业成本、营业利润、净利润</li>
              <li>资产负债表趋势：资产总计、负债合计、所有者权益</li>
              <li>现金流量表趋势：经营/投资/筹资活动现金流</li>
              <li>财务指标趋势：ROE、毛利率、净利率、增长率等</li>
            </ul>

            <p style={{ marginTop: 16 }}>
              <Text strong>数据说明：</Text>
            </p>
            <Text type="secondary">
              折线图展示最近8个季度的财务数据趋势，帮助您直观了解公司财务状况变化
            </Text>
          </div>
        </Card>
      )}
    </div>
  );
}

export default FinancialPage;
