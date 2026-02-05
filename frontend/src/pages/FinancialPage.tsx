import { useState } from 'react';
import {
  Card,
  Input,
  Button,
  Typography,
  Divider,
  Tabs,
  Space,
  Alert,
  Row,
  Col,
  message,
} from 'antd';
import {
  DollarOutlined,
  FileTextOutlined,
  DownloadOutlined,
  BarChartOutlined,
} from '@ant-design/icons';
import { SummaryCards } from '@/components/financial/SummaryCards';
import { FullReportView } from '@/components/financial/FullReportView';
import { useFinancialData } from '@/hooks';
import type { FinancialSummary, MetricCard as MetricCardType } from '@/types';
import { formatDate } from '@/utils';

const { Title, Text } = Typography;
const { Search } = Input;

function FinancialPage() {
  const [stockCode, setStockCode] = useState('');
  const [activeTab, setActiveTab] = useState('summary');
  const { summary, report, loading, error, fetchSummary, fetchReport, exportData } =
    useFinancialData();

  const handleSearch = async () => {
    if (!stockCode.trim()) {
      message.warning('请输入股票代码');
      return;
    }

    await fetchSummary(stockCode.trim());
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const handleViewFullReport = async () => {
    if (!stockCode.trim()) {
      message.warning('请先输入股票代码查询');
      return;
    }

    await fetchReport(stockCode.trim());
    setActiveTab('full');
  };

  const handleExport = async (format: 'csv' | 'excel') => {
    if (!stockCode.trim()) {
      message.warning('请先输入股票代码查询');
      return;
    }

    await exportData(stockCode.trim(), format);
  };

  // 转换summary到metrics格式
  const convertToMetrics = (summary: FinancialSummary | null): MetricCardType[] => {
    if (!summary) return [];

    const metrics: MetricCardType[] = summary.indicators.map((ind) => ({
      key: ind.key,
      title: ind.name,
      value: ind.value,
      unit: ind.unit || 'number',
      trend: 'neutral' as const,
      description: `数据日期：${ind.date || summary.updateTime}`,
    }));

    return metrics;
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
            <Button icon={<BarChartOutlined aria-hidden="true" />} onClick={handleViewFullReport}>
              完整报表
            </Button>
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
        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          defaultActiveKey="summary"
          items={[
            {
              key: 'summary',
              label: `摘要视图 (${summary.indicators.length}个指标)`,
              children: (
                <div>
                  {/* 基本信息 */}
                  <Card size="small" style={{ marginBottom: 16 }}>
                    <Row gutter={16}>
                      <Col span={6}>
                        <div>
                          <Text type="secondary">股票代码</Text>
                          <div style={{ fontSize: 20, fontWeight: 'bold', marginTop: 4 }}>
                            {summary.stockCode}
                          </div>
                        </div>
                      </Col>
                      <Col span={6}>
                        <div>
                          <Text type="secondary">股票名称</Text>
                          <div style={{ fontSize: 20, fontWeight: 'bold', marginTop: 4 }}>
                            {summary.stockName}
                          </div>
                        </div>
                      </Col>
                      <Col span={12}>
                        <div>
                          <Text type="secondary">更新时间</Text>
                          <div style={{ fontSize: 14, marginTop: 4 }}>
                            {formatDate(summary.updateTime, 'YYYY-MM-DD HH:mm:ss')}
                          </div>
                        </div>
                      </Col>
                    </Row>
                  </Card>

                  {/* 29个指标卡片 */}
                  <SummaryCards metrics={convertToMetrics(summary)} loading={loading} />
                </div>
              ),
            },
            {
              key: 'full',
              label: '完整报表',
              children: report ? (
                <FullReportView report={report} loading={loading} />
              ) : (
                <Card>
                  <div style={{ textAlign: 'center', padding: '40px 0', color: '#999' }}>
                    <FileTextOutlined style={{ fontSize: 48, marginBottom: 16 }} aria-hidden="true" />
                    <div>请先点击"完整报表"按钮获取完整财务数据</div>
                  </div>
                </Card>
              ),
            },
          ]}
        />
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
              <li>摘要视图：29个关键财务指标，分为9大类</li>
              <li>完整报表：利润表、资产负债表、现金流量表、财务指标</li>
            </ul>

            <p style={{ marginTop: 16 }}>
              <Text strong>指标分类：</Text>
            </p>
            <ul style={{ paddingLeft: 20 }}>
              <li>盈利能力：净资产收益率、毛利率、净利率等</li>
              <li>成长能力：营收增长率、利润增长率等</li>
              <li>估值指标：市盈率、市净率、市销率等</li>
              <li>偿债能力：流动比率、速动比率等</li>
              <li>杠杆比率：资产负债率、权益乘数等</li>
              <li>运营效率：存货周转率、应收账款周转率等</li>
              <li>每股指标：每股收益、每股净资产等</li>
              <li>市场数据：总市值、流通市值等</li>
              <li>其他：股息率、每股现金流等</li>
            </ul>

            <p style={{ marginTop: 16 }}>
              <Text strong>数据来源：</Text>
            </p>
            <Text type="secondary">
              数据来自公开披露的财务报表，包括季度报告、半年度报告和年度报告
            </Text>
          </div>
        </Card>
      )}
    </div>
  );
}

export default FinancialPage;
