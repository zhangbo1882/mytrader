import { useState } from 'react';
import { Form, Input, Select, DatePicker, Button, Space, Card, Typography, Alert, Divider } from 'antd';
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

  const modelTypes: { value: ModelType; label: string; description: string }[] = [
    { value: 'lightgbm', label: 'LightGBM', description: '梯度提升决策树，训练快速，性能优秀' },
    { value: 'lstm', label: 'LSTM', description: '长短期记忆网络，适合时序预测' },
    { value: 'xgboost', label: 'XGBoost', description: '极端梯度提升，适合结构化数据' },
    { value: 'random_forest', label: '随机森林', description: '集成学习算法，稳定性好' },
  ];

  const predictionTargets: { value: PredictionTarget; label: string }[] = [
    { value: '1d_return', label: '1日收益率' },
    { value: '3d_return', label: '3日收益率' },
    { value: '7d_return', label: '7日收益率' },
    { value: 'trend', label: '价格趋势' },
    { value: 'volatility', label: '波动率预测' },
  ];

  const handleFinish = (values: any) => {
    const params: ModelTrainingParams = {
      stockCode: values.stockCode,
      startDate: values.dateRange[0].format('YYYY-MM-DD'),
      endDate: values.dateRange[1].format('YYYY-MM-DD'),
      target: values.target,
      modelType: values.modelType,
      features: values.features,
      testSize: values.testSize || 0.2,
      validationSplit: values.validationSplit || 0.2,
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
          modelType: 'lightgbm',
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

        {/* 模型类型 */}
        <Form.Item
          label="模型类型"
          name="modelType"
          rules={[{ required: true, message: '请选择模型类型' }]}
        >
          <Select>
            {modelTypes.map((type) => (
              <Option key={type.value} value={type.value}>
                <div>
                  <div>{type.label}</div>
                  <div style={{ fontSize: 12, color: '#999' }}>{type.description}</div>
                </div>
              </Option>
            ))}
          </Select>
        </Form.Item>

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
