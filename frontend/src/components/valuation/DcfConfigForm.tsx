import { Card, Radio, Slider, Space, Col, Row, InputNumber, Collapse, Typography, Alert } from 'antd';
import { SettingOutlined } from '@ant-design/icons';
import type { DCFConfig, RiskProfile } from '@/types';

const { Text } = Typography;
const { Panel } = Collapse;

interface DcfConfigFormProps {
  dcfConfig: DCFConfig;
  setDcfConfig: (config: DCFConfig) => void;
  methods: string[];
}

const RISK_PROFILE_OPTIONS = [
  { label: '保守型', value: 'conservative' },
  { label: '平衡型（推荐）', value: 'balanced' },
  { label: '积极型', value: 'aggressive' },
  { label: '自定义', value: 'custom' },
];

const RISK_PROFILE_DESCRIPTIONS = {
  conservative: {
    title: '保守型参数',
    desc: '适用于不确定性高、风险厌恶的投资场景',
    scenarios: ['周期性公司', '高负债公司', '业绩波动大'],
    color: '#1890ff',
  },
  balanced: {
    title: '平衡型参数',
    desc: '适用于大多数成熟稳定公司',
    scenarios: ['行业龙头', '业绩稳定', '适度增长', '中国市场主流'],
    color: '#52c41a',
  },
  aggressive: {
    title: '积极型参数',
    desc: '适用于确定性高、有护城河的优质公司',
    scenarios: ['强势品牌（如茅台）', '垄断性业务', '持续高ROE', '长期增长确定性'],
    color: '#faad14',
  },
};

export function DcfConfigForm({ dcfConfig, setDcfConfig, methods }: DcfConfigFormProps) {
  // 只有选择了DCF方法，或者选择了combined且配置了DCF参数时才显示
  const hasDcfMethod = methods.includes('dcf');
  const hasCombinedWithDcf = methods.includes('combined') && (
    dcfConfig.risk_profile || dcfConfig.forecast_years !== undefined
  );
  const showDcfConfig = hasDcfMethod || hasCombinedWithDcf;

  const riskProfile = dcfConfig.risk_profile || 'balanced';
  const isCustom = dcfConfig.risk_profile === undefined;

  const handleRiskProfileChange = (value: string) => {
    if (value === 'custom') {
      // When switching to custom, keep existing params but remove risk_profile
      const { risk_profile, ...customParams } = dcfConfig;
      setDcfConfig(customParams);
    } else {
      // When selecting a preset, clear all other params to avoid overrides
      setDcfConfig({ risk_profile: value as RiskProfile });
    }
  };

  const updateConfig = (key: keyof DCFConfig, value: any) => {
    // When user adjusts any custom param, automatically switch to custom mode
    // This prevents the param from being overridden by a preset's risk_profile
    setDcfConfig({ ...dcfConfig, risk_profile: undefined, [key]: value });
  };

  if (!showDcfConfig) {
    return null;
  }

  return (
    <Card
      size="small"
      title={
        <Space>
          <SettingOutlined />
          <span>DCF估值参数</span>
        </Space>
      }
      style={{ marginTop: 16 }}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        {/* Risk Profile Selection */}
        <div>
          <div style={{ marginBottom: 8, color: '#666' }}>风险偏好预设：</div>
          <Radio.Group
            value={isCustom ? 'custom' : riskProfile}
            onChange={(e) => handleRiskProfileChange(e.target.value)}
            optionType="button"
            buttonStyle="solid"
          >
            {RISK_PROFILE_OPTIONS.map((opt) => (
              <Radio.Button key={opt.value} value={opt.value}>
                {opt.label}
              </Radio.Button>
            ))}
          </Radio.Group>

          {/* Risk Profile Description */}
          {!isCustom && RISK_PROFILE_DESCRIPTIONS[riskProfile] && (
            <Alert
              message={RISK_PROFILE_DESCRIPTIONS[riskProfile].title}
              description={
                <div>
                  <div>{RISK_PROFILE_DESCRIPTIONS[riskProfile].desc}</div>
                  <div style={{ marginTop: 8 }}>
                    <Text strong>适用场景：</Text>
                    {RISK_PROFILE_DESCRIPTIONS[riskProfile].scenarios.map((s, i) => (
                      <span key={i}>
                        {i > 0 && '、'}
                        {s}
                      </span>
                    ))}
                  </div>
                </div>
              }
              type="info"
              showIcon
              style={{ marginTop: 12 }}
            />
          )}
        </div>

        {/* Advanced Parameters */}
        <Collapse
          ghost
        >
          <Panel header="高级参数（自定义）" key="advanced">
            <Row gutter={[16, 16]}>
              {/* Forecast Years */}
              <Col span={8}>
                <div>
                  <div style={{ marginBottom: 8, color: '#666' }}>
                    预测期（年）：
                    <Text strong>{dcfConfig.forecast_years || 5}</Text>
                  </div>
                  <Slider
                    min={1}
                    max={10}
                    value={dcfConfig.forecast_years || 5}
                    onChange={(val) => updateConfig('forecast_years', val)}
                  />
                </div>
              </Col>

              {/* Terminal Growth Rate */}
              <Col span={8}>
                <div>
                  <div style={{ marginBottom: 8, color: '#666' }}>
                    终值增长率：
                    <Text strong>{((dcfConfig.terminal_growth ?? 0.02) * 100).toFixed(1)}%</Text>
                  </div>
                  <Slider
                    min={0}
                    max={0.05}
                    step={0.005}
                    value={dcfConfig.terminal_growth ?? 0.02}
                    onChange={(val) => updateConfig('terminal_growth', val)}
                  />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    建议值：1%-3%
                  </Text>
                </div>
              </Col>

              {/* Growth Rate Cap */}
              <Col span={8}>
                <div>
                  <div style={{ marginBottom: 8, color: '#666' }}>
                    增长率上限：
                    <Text strong>{((dcfConfig.growth_rate_cap ?? 0.06) * 100).toFixed(0)}%</Text>
                  </div>
                  <Slider
                    min={0}
                    max={0.15}
                    step={0.01}
                    value={dcfConfig.growth_rate_cap ?? 0.06}
                    onChange={(val) => updateConfig('growth_rate_cap', val)}
                  />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    建议值：4%-8%
                  </Text>
                </div>
              </Col>

              {/* WACC Min */}
              <Col span={8}>
                <div>
                  <div style={{ marginBottom: 8, color: '#666' }}>
                    WACC下限：
                    <Text strong>{((dcfConfig.wacc_min ?? 0.07) * 100).toFixed(1)}%</Text>
                  </div>
                  <Slider
                    min={0.05}
                    max={0.12}
                    step={0.005}
                    value={dcfConfig.wacc_min ?? 0.07}
                    onChange={(val) => updateConfig('wacc_min', val)}
                  />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    建议值：7%-9%
                  </Text>
                </div>
              </Col>

              {/* WACC Max */}
              <Col span={8}>
                <div>
                  <div style={{ marginBottom: 8, color: '#666' }}>
                    WACC上限：
                    <Text strong>{((dcfConfig.wacc_max ?? 0.12) * 100).toFixed(1)}%</Text>
                  </div>
                  <Slider
                    min={0.08}
                    max={0.18}
                    step={0.01}
                    value={dcfConfig.wacc_max ?? 0.12}
                    onChange={(val) => updateConfig('wacc_max', val)}
                  />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    建议值：11%-14%
                  </Text>
                </div>
              </Col>

              {/* Beta */}
              <Col span={8}>
                <div>
                  <div style={{ marginBottom: 8, color: '#666' }}>
                    Beta系数：
                    <Text strong>{dcfConfig.beta || '自动（根据行业）'}</Text>
                  </div>
                  <InputNumber
                    min={0.5}
                    max={2}
                    step={0.1}
                    value={dcfConfig.beta}
                    onChange={(val) => updateConfig('beta', val)}
                    placeholder="自动"
                    style={{ width: '100%' }}
                  />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    留空则自动计算
                  </Text>
                </div>
              </Col>
            </Row>

            {/* Parameter Tips */}
            <Alert
              message="参数提示"
              description={
                <ul style={{ margin: 0, paddingLeft: 16 }}>
                  <li>
                    <Text strong>WACC（加权平均资本成本）</Text>：越高→估值越低，
                    建议品牌龙头使用7-8%，普通公司使用8-10%
                  </li>
                  <li>
                    <Text strong>终值增长率</Text>：越高→估值越高，
                    建议成熟公司使用1-2%，成长公司使用2-3%
                  </li>
                  <li>
                    <Text strong>增长率上限</Text>：限制预测期内的增长假设，
                    建议根据公司历史增长率设定
                  </li>
                </ul>
              }
              type="info"
              showIcon
              style={{ marginTop: 8 }}
            />
          </Panel>
        </Collapse>
      </Space>
    </Card>
  );
}
