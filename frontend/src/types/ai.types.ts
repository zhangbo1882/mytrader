import { Stock } from './stock.types';

export type MessageRole = 'user' | 'assistant' | 'system';

export interface ChatMessage {
  id: string;
  role: MessageRole;
  content: string;
  timestamp: string;
  loading?: boolean;
  results?: Stock[];
}

export interface SuggestionPrompt {
  label: string;
  query: string;
  icon?: string;
}

export interface ChatState {
  messages: ChatMessage[];
  loading: boolean;
  error: string | null;
}
