import { Avatar, Tag, Space, Table, Button, Typography } from 'antd';
import { UserOutlined, RobotOutlined, LoadingOutlined } from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import type { ChatMessage, Stock } from '@/types';
import { formatNumber, formatPercent } from '@/utils';
import { useFavoriteStore } from '@/stores';

const { Text } = Typography;

interface MessageBubbleProps {
  message: ChatMessage;
}

export function MessageBubble({ message }: MessageBubbleProps) {
  const isUser = message.role === 'user';
  const { addFavorite, isInFavorites } = useFavoriteStore();

  // 添加到收藏
  const handleAddFavorite = (stock: Stock) => {
    if (!isInFavorites(stock.code)) {
      addFavorite(stock.code, stock.name);
    }
  };

  // 渲染筛选结果表格
  const renderResults = (stocks: Stock[]) => {
    if (!stocks || stocks.length === 0) {
      return <Text type="secondary">未找到符合条件的股票</Text>;
    }

    const columns: ColumnsType<Stock> = [
      {
        title: '代码',
        dataIndex: 'code',
        key: 'code',
        width: 100,
      },
      {
        title: '名称',
        dataIndex: 'name',
        key: 'name',
      },
      {
        title: '操作',
        key: 'action',
        width: 120,
        render: (_, stock) => (
          <Button
            size="small"
            type={isInFavorites(stock.code) ? 'default' : 'primary'}
            onClick={() => handleAddFavorite(stock)}
            disabled={isInFavorites(stock.code)}
          >
            {isInFavorites(stock.code) ? '已收藏' : '收藏'}
          </Button>
        ),
      },
    ];

    return (
      <div style={{ marginTop: 12 }}>
        <Text strong>找到 {stocks.length} 只股票：</Text>
        <Table
          columns={columns}
          dataSource={stocks}
          size="small"
          pagination={{ pageSize: 10 }}
          style={{ marginTop: 8 }}
        />
        <Button
          type="primary"
          size="small"
          style={{ marginTop: 8 }}
          onClick={() => stocks.forEach((s) => handleAddFavorite(s))}
          disabled={stocks.every((s) => isInFavorites(s.code))}
        >
          全部收藏
        </Button>
      </div>
    );
  };

  return (
    <div
      style={{
        display: 'flex',
        justifyContent: isUser ? 'flex-end' : 'flex-start',
        marginBottom: 16,
      }}
    >
      <div
        style={{
          display: 'flex',
          maxWidth: '70%',
          flexDirection: isUser ? 'row-reverse' : 'row',
          alignItems: 'flex-start',
          gap: 8,
        }}
      >
        {/* Avatar */}
        <Avatar
          icon={isUser ? <UserOutlined /> : <RobotOutlined />}
          style={{
            backgroundColor: isUser ? '#1890ff' : '#52c41a',
            flexShrink: 0,
          }}
        />

        {/* Message content */}
        <div>
          <div
            style={{
              backgroundColor: isUser ? '#1890ff' : '#f5f5f5',
              color: isUser ? '#fff' : '#000',
              padding: '12px 16px',
              borderRadius: 8,
              wordBreak: 'break-word',
              whiteSpace: 'pre-wrap',
            }}
          >
            {message.loading ? (
              <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <LoadingOutlined spin />
                正在思考中...
              </span>
            ) : (
              <Text style={{ color: isUser ? '#fff' : 'inherit' }}>{message.content}</Text>
            )}
          </div>

          {/* Timestamp */}
          <Text
            type="secondary"
            style={{
              fontSize: 11,
              marginTop: 4,
              display: 'block',
              textAlign: isUser ? 'right' : 'left',
            }}
          >
            {new Date(message.timestamp).toLocaleTimeString('zh-CN', {
              hour: '2-digit',
              minute: '2-digit',
            })}
          </Text>

          {/* AI筛选结果 */}
          {!isUser && message.results && renderResults(message.results)}
        </div>
      </div>
    </div>
  );
}
