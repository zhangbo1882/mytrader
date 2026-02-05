import { useState, useRef, useEffect } from 'react';
import { Card, Input, Button, Space, Empty, Spin, Typography } from 'antd';
import { SendOutlined, ClearOutlined, LoadingOutlined } from '@ant-design/icons';
import { MessageBubble } from './MessageBubble';
import { Suggestions, DEFAULT_SUGGESTIONS } from './Suggestions';
import { useChatStore } from '@/stores';
import { stockService } from '@/services';
import type { ChatMessage, Stock } from '@/types';

const { TextArea } = Input;
const { Text } = Typography;

interface ChatInterfaceProps {
  welcomeMessage?: string;
}

export function ChatInterface({ welcomeMessage = '你好！我是AI智能筛选助手，可以帮助你筛选符合条件的股票。' }: ChatInterfaceProps) {
  const [inputText, setInputText] = useState('');
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const { messages, loading, addMessage, updateMessage, setLoading, setError, clearMessages } = useChatStore();

  // 自动滚动到底部
  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  // 发送消息
  const handleSend = async () => {
    const trimmedText = inputText.trim();
    if (!trimmedText || loading) return;

    // 添加用户消息
    const userMessage: ChatMessage = {
      id: Date.now().toString(),
      role: 'user',
      content: trimmedText,
      timestamp: new Date().toISOString(),
    };
    addMessage(userMessage);
    setInputText('');

    // 添加AI加载消息
    const aiMessageId = (Date.now() + 1).toString();
    const loadingMessage: ChatMessage = {
      id: aiMessageId,
      role: 'assistant',
      content: '',
      timestamp: new Date().toISOString(),
      loading: true,
    };
    addMessage(loadingMessage);
    setLoading(true);

    try {
      // 调用 AI 筛选 REST API
      const result = await stockService.aiScreen(trimmedText);

      // 更新AI消息
      updateMessage(aiMessageId, {
        loading: false,
        content: result.explanation || `根据你的条件"${trimmedText}"，我为你找到了以下股票：`,
        results: result.stocks || [],
      });
    } catch (error) {
      updateMessage(aiMessageId, {
        loading: false,
        content: `抱歉，筛选时出现错误：${error instanceof Error ? error.message : '未知错误'}`,
      });
      setError(error instanceof Error ? error.message : '未知错误');
    } finally {
      setLoading(false);
    }
  };

  // 选择建议提示
  const handleSelectSuggestion = (prompt: string) => {
    setInputText(prompt);
  };

  // 清空对话
  const handleClear = () => {
    clearMessages();
  };

  // 回车发送（Shift+Enter换行）
  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* 消息列表 */}
      <div
        style={{
          flex: 1,
          overflowY: 'auto',
          padding: 16,
          backgroundColor: '#fafafa',
          borderRadius: '8px 8px 0 0',
          border: '1px solid #d9d9d9',
          borderBottom: 'none',
        }}
      >
        {messages.length === 0 ? (
          <div
            style={{
              height: '100%',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              alignItems: 'center',
            }}
          >
            <Empty description={
              <div>
                <Text strong style={{ fontSize: 16 }}>
                  {welcomeMessage}
                </Text>
                <Suggestions prompts={DEFAULT_SUGGESTIONS} onSelect={handleSelectSuggestion} />
              </div>
            } />
          </div>
        ) : (
          <>
            {messages.map((message) => (
              <MessageBubble key={message.id} message={message} />
            ))}
            <div ref={messagesEndRef} />
          </>
        )}
      </div>

      {/* 输入区域 */}
      <div
        style={{
          padding: 16,
          backgroundColor: '#fff',
          borderRadius: '0 0 8px 8px',
          border: '1px solid #d9d9d9',
        }}
      >
        {messages.length === 0 && (
          <Suggestions prompts={DEFAULT_SUGGESTIONS} onSelect={handleSelectSuggestion} />
        )}

        <Space.Compact style={{ width: '100%' }}>
          <TextArea
            value={inputText}
            onChange={(e) => setInputText(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="输入筛选条件，例如：查找换手率大于5%的股票"
            autoSize={{ minRows: 2, maxRows: 6 }}
            disabled={loading}
          />
          <Button
            type="primary"
            icon={loading ? <LoadingOutlined spin /> : <SendOutlined aria-hidden="true" />}
            onClick={handleSend}
            disabled={!inputText.trim() || loading}
            style={{ height: 'auto' }}
          >
            发送
          </Button>
        </Space.Compact>

        {messages.length > 0 && (
          <div style={{ marginTop: 8, textAlign: 'center' }}>
            <Button size="small" icon={<ClearOutlined aria-hidden="true" />} onClick={handleClear}>
              清空对话
            </Button>
          </div>
        )}

        <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 8, textAlign: 'center' }}>
          按 Enter 发送，Shift + Enter 换行
        </Text>
      </div>
    </div>
  );
}
