import { useState, useEffect } from 'react';
import {
  Card,
  Typography,
  Divider,
  Tabs,
  Modal,
  Button,
  Descriptions,
  Tag,
  Row,
  Col,
  Statistic,
  message,
  Spin,
} from 'antd';
import {
  BulbOutlined,
  LineChartOutlined,
  BarChartOutlined,
  CheckCircleOutlined,
} from '@ant-design/icons';
import { ModelTrainingForm } from '@/components/ml/ModelTrainingForm';
import { ModelList } from '@/components/ml/ModelList';
import { PredictionResults } from '@/components/ml/PredictionResults';
import type { MLModel, ModelPerformance, PredictionResult } from '@/types';
import { mlService } from '@/services';
import { formatDate, formatPercent, formatNumber } from '@/utils';

const { Title, Text } = Typography;

function PredictionPage() {
  const [models, setModels] = useState<MLModel[]>([]);
  const [modelsLoading, setModelsLoading] = useState(false);
  const [trainingLoading, setTrainingLoading] = useState(false);
  const [predictions, setPredictions] = useState<PredictionResult[]>([]);
  const [predictionsLoading, setPredictionsLoading] = useState(false);
  const [selectedModel, setSelectedModel] = useState<MLModel | null>(null);
  const [modelDetailVisible, setModelDetailVisible] = useState(false);
  const [performance, setPerformance] = useState<ModelPerformance | null>(null);

  // 加载模型列表
  useEffect(() => {
    loadModels();
  }, []);

  const loadModels = async () => {
    setModelsLoading(true);
    try {
      const result = await mlService.listModels();
      const modelList = result.models || [];
      setModels(modelList);
    } catch (error) {
      message.error('加载模型列表失败');
    } finally {
      setModelsLoading(false);
    }
  };

  // 训练模型
  const handleTrain = async (params: any) => {
    setTrainingLoading(true);
    try {
      const result = await mlService.train(params);
      message.success('模型训练已启动，请在任务历史中查看进度');
      loadModels();
    } catch (error) {
      message.error(`启动训练失败：${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setTrainingLoading(false);
    }
  };

  // 删除模型
  const handleDeleteModel = async (id: string) => {
    try {
      await mlService.deleteModel(id);
      message.success('模型已删除');
      loadModels();
    } catch (error) {
      message.error('删除失败');
    }
  };

  // 查看模型详情
  const handleViewModel = async (model: MLModel) => {
    setSelectedModel(model);
    setModelDetailVisible(true);

    // 加载性能指标
    try {
      const perf = await mlService.getPerformance(model.id);
      setPerformance(perf);
    } catch (error) {
      console.error('Failed to load performance:', error);
    }
  };

  // 预测
  const handlePredict = async (model: MLModel) => {
    setPredictionsLoading(true);
    try {
      const results = await mlService.predict(model.id, model.stockCode, 7);
      setPredictions(results);
      message.success('预测完成');
    } catch (error) {
      message.error(`预测失败：${error instanceof Error ? error.message : '未知错误'}`);
    } finally {
      setPredictionsLoading(false);
    }
  };

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>
        <BulbOutlined style={{ marginRight: 8 }} aria-hidden="true" />
        AI预测
      </Title>
      <Text type="secondary">使用机器学习模型预测股票价格走势</Text>

      <Divider />

      <Tabs
        defaultActiveKey="train"
        items={[
          {
            key: 'train',
            label: '模型训练',
            children: (
              <Row gutter={16}>
                <Col span={12}>
                  <ModelTrainingForm onSubmit={handleTrain} loading={trainingLoading} />
                </Col>
                <Col span={12}>
                  <Card title={<Text strong>训练说明</Text>}>
                    <div style={{ lineHeight: 1.8 }}>
                      <p>
                        <Text strong>支持的模型类型：</Text>
                      </p>
                      <ul style={{ paddingLeft: 20 }}>
                        <li>
                          <Text>LightGBM：梯度提升决策树，训练快速，性能优秀</Text>
                        </li>
                        <li>
                          <Text>LSTM：长短期记忆网络，适合时序预测</Text>
                        </li>
                        <li>
                          <Text>XGBoost：极端梯度提升，适合结构化数据</Text>
                        </li>
                        <li>
                          <Text>随机森林：集成学习算法，稳定性好</Text>
                        </li>
                      </ul>

                      <p style={{ marginTop: 16 }}>
                        <Text strong>预测目标：</Text>
                      </p>
                      <ul style={{ paddingLeft: 20 }}>
                        <li>收益率预测：1日、3日、7日收益率</li>
                        <li>趋势预测：上涨/下跌/持平</li>
                        <li>波动率预测：价格波动程度</li>
                      </ul>

                      <p style={{ marginTop: 16 }}>
                        <Text strong>使用建议：</Text>
                      </p>
                      <ul style={{ paddingLeft: 20 }}>
                        <li>训练数据至少1年的历史数据</li>
                        <li>建议先用20%测试集验证模型效果</li>
                        <li>关注准确率、精确率、召回率等指标</li>
                        <li>模型训练可能需要几分钟时间</li>
                      </ul>
                    </div>
                  </Card>
                </Col>
              </Row>
            ),
          },
          {
            key: 'models',
            label: `模型列表 (${models.length})`,
            children: (
              <ModelList
                models={models}
                loading={modelsLoading}
                onDelete={handleDeleteModel}
                onView={handleViewModel}
                onPredict={handlePredict}
              />
            ),
          },
          {
            key: 'results',
            label: '预测结果',
            children: (
              <>
                {predictions.length === 0 ? (
                  <Card>
                    <div style={{ textAlign: 'center', padding: '40px 0', color: '#999' }}>
                      <LineChartOutlined style={{ fontSize: 48, marginBottom: 16 }} aria-hidden="true" />
                      <div>暂无预测结果</div>
                      <div style={{ fontSize: 12, marginTop: 8 }}>
                        请先在模型列表中选择一个已完成的模型进行预测
                      </div>
                    </div>
                  </Card>
                ) : (
                  <PredictionResults results={predictions} loading={predictionsLoading} />
                )}
              </>
            ),
          },
        ]}
      />

      {/* 模型详情对话框 */}
      <Modal
        title="模型详情"
        open={modelDetailVisible}
        onCancel={() => setModelDetailVisible(false)}
        footer={null}
        width={800}
      >
        {selectedModel && (
          <div>
            <Descriptions column={2} bordered size="small">
              <Descriptions.Item label="股票代码">{selectedModel.stockCode}</Descriptions.Item>
              <Descriptions.Item label="股票名称">{selectedModel.stockName}</Descriptions.Item>
              <Descriptions.Item label="模型类型">
                <Tag color="blue">{selectedModel.modelType.toUpperCase()}</Tag>
              </Descriptions.Item>
              <Descriptions.Item label="预测目标">{selectedModel.target}</Descriptions.Item>
              <Descriptions.Item label="训练开始时间">
                {formatDate(selectedModel.startDate, 'YYYY-MM-DD')}
              </Descriptions.Item>
              <Descriptions.Item label="训练结束时间">
                {formatDate(selectedModel.endDate, 'YYYY-MM-DD')}
              </Descriptions.Item>
              <Descriptions.Item label="创建时间" span={2}>
                {formatDate(selectedModel.createdAt, 'YYYY-MM-DD HH:mm:ss')}
              </Descriptions.Item>
            </Descriptions>

            {selectedModel.status === 'completed' && performance && (
              <>
                <Divider orientation="left">模型性能</Divider>
                <Row gutter={16} style={{ marginBottom: 16 }}>
                  <Col span={6}>
                    <Statistic
                      title="准确率"
                      value={formatNumber(performance.accuracy * 100, 2)}
                      suffix="%"
                      valueStyle={{ color: performance.accuracy > 0.7 ? '#3f8600' : '#cf1322' }}
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="精确率"
                      value={formatNumber(performance.precision * 100, 2)}
                      suffix="%"
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="召回率"
                      value={formatNumber(performance.recall * 100, 2)}
                      suffix="%"
                    />
                  </Col>
                  <Col span={6}>
                    <Statistic
                      title="F1分数"
                      value={formatNumber(performance.f1Score * 100, 2)}
                      suffix="%"
                      valueStyle={{ color: performance.f1Score > 0.7 ? '#3f8600' : '#faad14' }}
                    />
                  </Col>
                </Row>

                {performance.featureImportance && performance.featureImportance.length > 0 && (
                  <>
                    <Divider orientation="left">特征重要性</Divider>
                    <div style={{ maxHeight: 300, overflowY: 'auto' }}>
                      {performance.featureImportance.map((item, index) => (
                        <div key={index} style={{ marginBottom: 8 }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                            <Text>{item.feature}</Text>
                            <Text>{formatNumber(item.importance * 100, 2)}%</Text>
                          </div>
                          <Progress
                            percent={Math.round(item.importance * 100)}
                            size="small"
                            showInfo={false}
                          />
                        </div>
                      ))}
                    </div>
                  </>
                )}
              </>
            )}

            {selectedModel.status === 'failed' && (
              <>
                <Divider orientation="left">错误信息</Divider>
                <Text type="danger">{selectedModel.error || '训练失败'}</Text>
              </>
            )}
          </div>
        )}
      </Modal>
    </div>
  );
}

export default PredictionPage;
