import { useState } from 'react';
import { Form, Input, Select, DatePicker, Button, Switch, Space, Card, Typography, Alert, Divider } from 'antd';
import { RocketOutlined, StockOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import type { ModelTrainingParams, ModelType, PredictionTarget } from '@/types';
import { useFavoriteStore } from '@/stores';

const { Title, Text } = Typography;
const { Option } = Select;
const { RangePicker } = DatePicker;

interface ModelTrainingFormProps {
  onSubmit: (params: ModelTrainingParams) => void;
  loading?: boolean;
}

export function ModelTrainingForm({ onSubmit, loading = false }: ModelTrainingFormProps) {
  const { favorites } = useFavoriteStore();
  const [form] = Form.useForm();
  const [walkForward, setWalkForward] = useState(true);  // 默认开启滚动窗口

  const predictionTargets: { value: PredictionTarget; label: string }[] = [
    { value: '1d_return', label: '1日收益率' },
    { value: '5d_return', label: '5日收益率' },
    { value: 'trend', label: '1日价格趋势' },
    { value: 'trend_5d', label: '5日价格趋势' },
    { value: 'volatility', label: '波动率预测' },
  ];

  const handleFinish = (values: any) => {
    const params: ModelTrainingParams = {
      stockCode: values.stockCode,
      startDate: values.dateRange[0].format('YYYY-MM-DD'),
      endDate: values.dateRange[1].format('YYYY-MM-DD'),
      target: values.target,
      modelType: 'lightgbm', // 固定使用 LightGBM
      features: values.features,
      testSize: values.testSize || 0.2,
      validationSplit: values.validationSplit || 0.2,
      walkForward: walkForward,
      nSplits: walkForward ? (values.nSplits || 5) : undefined,
    };

    onSubmit(params);
  };

  // 从收藏中选择
  const handleSelectFromFavorites = (code: string) => {
    form.setFieldValue('stockCode', code);
  };

  return (
    <Card title={<Title level={4} style={{ margin: 0 }}>模型训练</Title>}>
      <Form
        form={form}
        layout="vertical"
        onFinish={handleFinish}
        initialValues={{
          target: '1d_return',
          testSize: 0.2,
          validationSplit: 0.2,
          dateRange: [dayjs().subtract(2, 'year'), dayjs().subtract(1, 'day')],
        }}
      >
        {/* 股票代码 */}
        <Form.Item
          label="股票代码"
          name="stockCode"
          rules={[{ required: true, message: '请输入股票代码' }]}
        >
          <Input
            placeholder="输入股票代码，如：600382"
            prefix={<StockOutlined />}
            style={{ width: '100%' }}
          />
        </Form.Item>

        {/* 快速选择收藏 */}
        {favorites.length > 0 && (
          <div style={{ marginBottom: 16 }}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              快速选择：
            </Text>
            <div style={{ marginTop: 8 }}>
              {favorites.slice(0, 5).map((fav) => (
                <Button
                  key={fav.code}
                  size="small"
                  style={{ marginRight: 8, marginBottom: 8 }}
                  onClick={() => handleSelectFromFavorites(fav.code)}
                >
                  {fav.code}
                </Button>
              ))}
            </div>
          </div>
        )}

        {/* 日期范围 */}
        <Form.Item
          label="训练数据日期范围"
          name="dateRange"
          rules={[{ required: true, message: '请选择日期范围' }]}
        >
          <RangePicker style={{ width: '100%' }} />
        </Form.Item>

        <Divider style={{ margin: '12px 0' }}>模型配置</Divider>

        {/* 模型类型：固定使用 LightGBM */}
        <Alert
          message="模型类型: LightGBM"
          description="梯度提升决策树，训练快速，性能优秀，适合股票预测"
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />

        {/* 预测目标 */}
        <Form.Item
          label="预测目标"
          name="target"
          rules={[{ required: true, message: '请选择预测目标' }]}
        >
          <Select>
            {predictionTargets.map((target) => (
              <Option key={target.value} value={target.value}>
                {target.label}
              </Option>
            ))}
          </Select>
        </Form.Item>

        {/* 高级设置 */}
        <Divider style={{ margin: '12px 0' }}>高级设置（可选）</Divider>

        <Form.Item label="测试集比例" name="testSize">
          <Select>
            <Option value={0.1}>10%</Option>
            <Option value={0.2}>20%</Option>
            <Option value={0.3}>30%</Option>
            <Option value={0.4}>40%</Option>
          </Select>
        </Form.Item>

        <Form.Item label="验证集比例" name="validationSplit">
          <Select>
            <Option value={0.1}>10%</Option>
            <Option value={0.2}>20%</Option>
            <Option value={0.3}>30%</Option>
          </Select>
        </Form.Item>

        <Divider style={{ margin: '12px 0' }}>滚动窗口验证（推荐）</Divider>

        <Form.Item
          label={
            <Space>
              <span>使用 Walk-Forward 滚动窗口</span>
              <Switch
                checked={walkForward}
                onChange={setWalkForward}
                checkedChildren="开启"
                unCheckedChildren="关闭"
              />
            </Space>
          }
        >
          <Alert
            message={walkForward ? "✓ 已启用滚动窗口验证（推荐）" : "未启用（使用单次时序划分）"}
            description={
              walkForward
                ? "将数据分为多个时间折叠，逐步扩展训练窗口，最终在全部数据上训练模型。比单次划分更可靠，尤其适合跨市场周期的数据（如牛市→熊市）。"
                : "将数据按时间顺序一次性划分为训练/验证/测试集，训练更快但跨市场周期评估可能不稳定。"
            }
            type={walkForward ? "success" : "warning"}
            showIcon
            style={{ marginBottom: 0 }}
          />
        </Form.Item>

        {walkForward && (
          <Form.Item label="折叠数 (n_splits)" name="nSplits" initialValue={5}>
            <Select>
              <Option value={3}>3 折（数据较少时推荐）</Option>
              <Option value={5}>5 折（默认，推荐）</Option>
              <Option value={10}>10 折（数据充足时使用）</Option>
            </Select>
          </Form.Item>
        )}

        {/* 提交按钮 */}
        <Form.Item style={{ marginBottom: 0 }}>
          <Button
            type="primary"
            htmlType="submit"
            icon={<RocketOutlined />}
            loading={loading}
            block
            size="large"
          >
            开始训练
          </Button>
        </Form.Item>
      </Form>

      {/* 提示信息 */}
      <Alert
        message="训练说明"
        description={
          <ul style={{ margin: 0, paddingLeft: 20 }}>
            <li>训练数据时间范围建议至少1年</li>
            <li>模型训练可能需要几分钟到几十分钟</li>
            <li>训练完成后可查看模型性能和预测结果</li>
            <li>建议先用少量数据测试，再增加数据量</li>
          </ul>
        }
        type="info"
        showIcon
        style={{ marginTop: 16 }}
      />
    </Card>
  );
}
