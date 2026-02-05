/**
 * WebSocket Service for AI Stock Screening
 * Uses Socket.IO to communicate with the backend
 */
import { io, Socket } from 'socket.io-client';

type ScreenParams = {
  days?: number;
  turnover_min?: number | null;
  turnover_max?: number | null;
  pct_chg_min?: number | null;
  pct_chg_max?: number | null;
  price_min?: number | null;
  price_max?: number | null;
  volume_min?: number | null;
  volume_max?: number | null;
};

type ChatMessage = {
  role: 'user' | 'assistant';
  content: string;
};

type AIScreenResult = {
  success: boolean;
  params: ScreenParams;
  query: string;
  explanation?: string;
  stocks?: any[];
};

class WebSocketService {
  private socket: Socket | null = null;
  private connected = false;

  connect() {
    if (this.socket?.connected) {
      return this.socket;
    }

    // Connect to the backend server (port 5001)
    // In development, backend runs on http://127.0.0.1:5001
    const url = import.meta.env.VITE_API_URL || 'http://127.0.0.1:5001';
    console.log('[WebSocket] Connecting to:', url);

    this.socket = io(url, {
      transports: ['websocket', 'polling'],
      reconnection: true,
      reconnectionAttempts: 5,
      reconnectionDelay: 1000,
    });

    this.socket.on('connect', () => {
      console.log('[WebSocket] Connected to server');
      this.connected = true;
    });

    this.socket.on('disconnect', () => {
      console.log('[WebSocket] Disconnected from server');
      this.connected = false;
    });

    this.socket.on('ai_screen_status', (data: any) => {
      console.log('[WebSocket] Status:', data);
    });

    this.socket.on('ai_screen_error', (data: any) => {
      console.error('[WebSocket] Error:', data);
    });

    return this.socket;
  }

  disconnect() {
    if (this.socket) {
      this.socket.disconnect();
      this.socket = null;
      this.connected = false;
    }
  }

  isConnected(): boolean {
    return this.connected && this.socket?.connected === true;
  }

  /**
   * Send AI screening query via WebSocket
   * Returns a promise that resolves with the result
   */
  aiScreen(query: string): Promise<AIScreenResult> {
    return new Promise((resolve, reject) => {
      const socket = this.connect();

      // Wait for connection if not already connected
      if (!this.isConnected()) {
        const connectionTimeout = setTimeout(() => {
          reject(new Error('WebSocket 连接超时'));
        }, 5000);

        socket.once('connect', () => {
          clearTimeout(connectionTimeout);
          this.sendQuery(socket, query, resolve, reject);
        });
      } else {
        this.sendQuery(socket, query, resolve, reject);
      }
    });
  }

  private sendQuery(
    socket: Socket,
    query: string,
    resolve: (value: AIScreenResult) => void,
    reject: (reason: Error) => void
  ) {
    // Set up one-time listener for the result
    const timeout = setTimeout(() => {
      socket.off('ai_screen_result');
      socket.off('ai_screen_error');
      reject(new Error('请求超时'));
    }, 30000); // 30 second timeout

    socket.once('ai_screen_result', (data: AIScreenResult) => {
      clearTimeout(timeout);
      if (data.success) {
        resolve(data);
      } else {
        reject(new Error(data.params ? 'Failed to parse' : '未知错误'));
      }
    });

    socket.once('ai_screen_error', (data: { error: string }) => {
      clearTimeout(timeout);
      reject(new Error(data.error || '处理失败'));
    });

    // Send the query
    socket.emit('ai_screen_query', { query });
  }

  /**
   * Send AI chat query via WebSocket
   * Returns a promise that resolves with the response
   */
  aiChat(query: string, history: ChatMessage[]): Promise<{ response: string; params?: ScreenParams }> {
    return new Promise((resolve, reject) => {
      const socket = this.connect();

      if (!this.isConnected()) {
        reject(new Error('WebSocket 未连接'));
        return;
      }

      // Set up one-time listener for the result
      const timeout = setTimeout(() => {
        socket.off('ai_screen_chat_result');
        socket.off('ai_screen_error');
        reject(new Error('请求超时'));
      }, 30000);

      socket.once('ai_screen_chat_result', (data: { response: string; params?: ScreenParams }) => {
        clearTimeout(timeout);
        resolve(data);
      });

      socket.once('ai_screen_error', (data: { error: string }) => {
        clearTimeout(timeout);
        reject(new Error(data.error || '处理失败'));
      });

      // Send the chat message
      socket.emit('ai_screen_chat', { query, history });
    });
  }
}

// Export singleton instance
export const websocketService = new WebSocketService();
