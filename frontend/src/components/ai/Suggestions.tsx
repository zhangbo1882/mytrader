import { Space, Tag } from 'antd';
import { BulbOutlined } from '@ant-design/icons';
import type { SuggestionPrompt } from '@/types';

interface SuggestionsProps {
  prompts: SuggestionPrompt[];
  onSelect: (prompt: string) => void;
}

export function Suggestions({ prompts, onSelect }: SuggestionsProps) {
  return (
    <div style={{ marginBottom: 16 }}>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          marginBottom: 8,
          color: '#666',
        }}
      >
        <BulbOutlined />
        <span style={{ fontSize: 14 }}>试试这些：</span>
      </div>
      <Space wrap>
        {prompts.map((prompt, index) => (
          <Tag
            key={index}
            style={{
              cursor: 'pointer',
              fontSize: 13,
              padding: '4px 12px',
              borderRadius: 4,
            }}
            onClick={() => onSelect(prompt.query)}
          >
            {prompt.label}
          </Tag>
        ))}
      </Space>
    </div>
  );
}

// 默认建议提示
export const DEFAULT_SUGGESTIONS: SuggestionPrompt[] = [
  {
    label: '换手率大于5%',
    query: '查找换手率大于5%的股票',
  },
  {
    label: '市盈率小于20',
    query: '筛选市盈率小于20的股票',
  },
  {
    label: '连续上涨3天',
    query: '显示最近连续上涨3天的股票',
  },
  {
    label: '涨幅前10名',
    query: '今天涨幅排名前10的股票',
  },
  {
    label: '市值大于100亿',
    query: '查找总市值大于100亿的股票',
  },
  {
    label: '市净率小于1',
    query: '筛选市净率小于1的股票',
  },
];
