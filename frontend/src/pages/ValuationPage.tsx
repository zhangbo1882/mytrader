import { useState } from 'react';
import {
  Typography,
  Divider,
  Space,
  Alert,
  Tabs,
  Spin,
  Button,
  Card
} from 'antd';
import { FundOutlined, ExperimentOutlined } from '@ant-design/icons';
import { ValuationSearch, ValuationParamsForm, ValuationResultCard, ModelComparisonChart, DcfConfigForm, BayesianMatrixPanel } from '@/components/valuation';
import { useValuation } from '@/hooks';
import type { ValuationMethod, CombineMethod, DCFConfig } from '@/types';

const { Title, Text } = Typography;

function ValuationPage() {
  const [stockCode, setStockCode] = useState('');
  const [methods, setMethods] = useState<ValuationMethod[]>(['combined', 'peg', 'dcf']);
  const [date, setDate] = useState('');
  const [fiscalDate, setFiscalDate] = useState('');
  const [combineMethod, setCombineMethod] = useState<CombineMethod>('min_fair_value');
  const [dcfConfig, setDcfConfig] = useState<DCFConfig>({ risk_profile: 'conservative' });
  const [activeTab, setActiveTab] = useState('valuation');

  const { result, loading, error, fetchValuation, clear } = useValuation();

  const handleSearch = async () => {
    if (!stockCode.trim()) {
      return;
    }

    await fetchValuation({
      symbol: stockCode.trim(),
      methods,
      date: date || undefined,
      fiscal_date: fiscalDate || undefined,
      combine_method: combineMethod,
      dcf_config: dcfConfig
    });
  };

  const handleReset = () => {
    setStockCode('');
    setDcfConfig({ risk_profile: 'conservative' });
    clear();
  };

  return (
    <div>
      <Title level={2}>
        <FundOutlined style={{ marginRight: 8 }} aria-hidden="true" />
        股票估值
      </Title>
      <Text type="secondary">基于多种估值模型分析股票合理价值</Text>

      <Divider />

      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          {
            key: 'valuation',
            label: '估值分析',
            children: (
              <>
                {/* Search and parameter configuration */}
                <Space direction="vertical" style={{ width: '100%' }} size="large">
                  <ValuationSearch
                    value={stockCode}
                    onChange={setStockCode}
                    onSearch={handleSearch}
                    loading={loading}
                  />

                  <ValuationParamsForm
                    methods={methods}
                    setMethods={setMethods}
                    date={date}
                    setDate={setDate}
                    fiscalDate={fiscalDate}
                    setFiscalDate={setFiscalDate}
                    combineMethod={combineMethod}
                    setCombineMethod={setCombineMethod}
                  />

                  <DcfConfigForm
                    dcfConfig={dcfConfig}
                    setDcfConfig={setDcfConfig}
                    methods={methods}
                  />
                </Space>

                {/* Error alert */}
                {error && (
                  <Alert
                    message="估值失败"
                    description={error}
                    type="error"
                    showIcon
                    closable
                    style={{ marginTop: 16 }}
                  />
                )}

                {/* Valuation results */}
                {result && !loading && (
                  <>
                    <Divider />

                    <Tabs
                      items={[
                        {
                          key: 'summary',
                          label: '估值摘要',
                          children: (
                            <>
                              <ValuationResultCard result={result} />

                              {/* If there are multiple model results, show comparison chart */}
                              {result.individual_results && result.individual_results.length > 1 && (
                                <div style={{ marginTop: 24 }}>
                                  <ModelComparisonChart
                                    results={[result, ...result.individual_results]}
                                  />
                                </div>
                              )}
                            </>
                          ),
                        },
                        {
                          key: 'details',
                          label: '详细分析',
                          children: (
                            <Card title="模型详情">
                              <Alert
                                message="详细分析功能开发中"
                                description="将展示历史估值趋势、行业对比等更多分析"
                                type="info"
                                showIcon
                              />
                            </Card>
                          ),
                        },
                      ]}
                    />

                    <Space style={{ marginTop: 24 }}>
                      <Button onClick={handleReset}>重置</Button>
                    </Space>
                  </>
                )}

                {/* Loading state */}
                {loading && (
                  <div style={{ textAlign: 'center', padding: '40px 0' }}>
                    <Spin size="large" tip="正在计算估值..." />
                  </div>
                )}
              </>
            ),
          },
          {
            key: 'bayesian',
            label: (
              <span>
                <ExperimentOutlined />
                贝叶斯先验矩阵
              </span>
            ),
            children: <BayesianMatrixPanel />,
          },
        ]}
      />
    </div>
  );
}

export default ValuationPage;
