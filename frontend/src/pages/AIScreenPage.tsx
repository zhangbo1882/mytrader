import { Card, Typography, Divider, Space, Alert } from 'antd';
import { RobotOutlined } from '@ant-design/icons';
import { ChatInterface } from '@/components/ai/ChatInterface';
import { useChatStore } from '@/stores';

const { Title, Text, Paragraph } = Typography;

function AIScreenPage() {
  const { error } = useChatStore();

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>
        <RobotOutlined style={{ marginRight: 8 }} aria-hidden="true" />
        AI智能筛选
      </Title>
      <Text type="secondary">使用自然语言描述筛选条件，AI将为你找出符合条件的股票</Text>

      <Divider />

      {/* 错误提示 */}
      {error && (
        <Alert
          message="操作失败"
          description={error}
          type="error"
          showIcon
          closable
          style={{ marginBottom: 16 }}
        />
      )}

      {/* 使用说明 */}
      <Card
        size="small"
        style={{ marginBottom: 16 }}
        title={
          <Space>
            <span>💡 使用提示</span>
          </Space>
        }
      >
        <Paragraph style={{ marginBottom: 8 }}>
          <Text strong>支持的筛选条件包括：</Text>
        </Paragraph>
        <ul style={{ margin: 0, paddingLeft: 20 }}>
          <li>技术指标：换手率、涨跌幅、成交量等</li>
          <li>估值指标：市盈率、市净率、市值等</li>
          <li>价格条件：价格区间、涨跌停等</li>
          <li>趋势判断：连续上涨、突破新高、MACD金叉等</li>
        </ul>
        <Paragraph style={{ marginTop: 12, marginBottom: 0 }}>
          <Text type="secondary">
            示例："查找换手率大于5%且市盈率小于20的股票"、"显示最近连续3天上涨的股票"
          </Text>
        </Paragraph>
      </Card>

      {/* 聊天界面 */}
      <Card
        title="智能对话"
        style={{
          height: 'calc(100vh - 380px)',
          minHeight: 500,
        }}
        bodyStyle={{
          padding: 0,
          height: '100%',
        }}
      >
        <ChatInterface
          welcomeMessage="你好！我是AI智能筛选助手，可以帮助你筛选符合条件的股票。请用自然语言描述你的筛选条件。"
        />
      </Card>
    </div>
  );
}

export default AIScreenPage;
