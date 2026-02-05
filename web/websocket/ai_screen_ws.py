"""
WebSocket Service for AI Stock Screening
"""
import json
import re
import logging
from flask import request
from flask_socketio import emit, disconnect

# Configure logging
logger = logging.getLogger(__name__)


class MockClaudeProcessor:
    """
    Mock Claude CLI processor for AI screening.
    In production, this would interface with the actual Claude CLI.
    """

    def __init__(self):
        """Initialize the mock processor"""
        logger.info("[AI Screen] Mock processor initialized")

    def send_query(self, query):
        """
        Process a natural language query and extract screening parameters

        Args:
            query: Natural language query string

        Returns:
            dict: Extracted screening parameters
        """
        # Parse the query to extract screening parameters
        params = self._parse_query(query)
        return params

    def send_chat(self, query, history):
        """
        Process a conversational query with history

        Args:
            query: Current query
            history: Conversation history

        Returns:
            tuple: (response_text, extracted_params)
        """
        # Parse the query
        params = self._parse_query(query)

        # Build a natural response
        response = self._build_response(query, params, history)

        return response, params

    def _parse_query(self, query):
        """
        Parse natural language query to extract screening parameters

        Args:
            query: Natural language query

        Returns:
            dict: Extracted parameters
        """
        params = {}

        # Default days
        params['days'] = 5

        # Parse price ranges
        price_patterns = [
            (r'(?:价格|股价|低于|小于|<)\s*(\d+(?:\.\d+)?)\s*[元块钱]', 'price_max'),
            (r'(?:价格|股价|高于|大于|>)\s*(\d+(?:\.\d+)?)\s*[元块钱]', 'price_min'),
            (r'(\d+(?:\.\d+)?)\s*[元块钱]以下', 'price_max'),
            (r'(\d+(?:\.\d+)?)\s*[元块钱]以上', 'price_min'),
        ]

        # Parse turnover/换手率
        turnover_patterns = [
            (r'换手率\s*(?:大于|>|超过|高于)\s*(\d+(?:\.\d+)?)\s*%?', 'turnover_min'),
            (r'换手率\s*(?:小于|<|低于)\s*(\d+(?:\.\d+)?)\s*%?', 'turnover_max'),
            (r'(?:高换手|换手率高)', 'turnover_min'),
        ]

        # Parse price change/涨跌幅
        pct_chg_patterns = [
            (r'(?:涨幅|涨跌幅)\s*(?:大于|>|超过)\s*(\d+(?:\.\d+)?)\s*%?', 'pct_chg_min'),
            (r'(?:涨幅|涨跌幅)\s*(?:小于|<)\s*(\d+(?:\.\d+)?)\s*%?', 'pct_chg_max'),
            (r'上涨\s*(\d+(?:\.\d+)?)\s*%?', 'pct_chg_min'),
            (r'连续上涨', 'pct_chg_min'),
        ]

        # Parse volume/成交量
        volume_patterns = [
            (r'成交量\s*(?:放大|大于|>|超过)', 'volume_min'),
            (r'放量', 'volume_min'),
        ]

        # Parse days
        days_patterns = [
            (r'最近\s*(\d+)\s*天', 'days'),
            (r'(\d+)\s*天\s*内', 'days'),
        ]

        # Apply patterns
        all_patterns = [
            (price_patterns, float),
            (turnover_patterns, self._parse_turnover),
            (pct_chg_patterns, float),
            (volume_patterns, self._parse_volume),
            (days_patterns, int),
        ]

        for patterns, parser in all_patterns:
            for pattern, param_key in patterns:
                match = re.search(pattern, query)
                if match:
                    value = match.group(1) if match.groups() else None
                    if value is not None:
                        try:
                            params[param_key] = parser(value)
                        except (ValueError, IndexError):
                            params[param_key] = self._get_default_value(param_key)
                    else:
                        params[param_key] = self._get_default_value(param_key)

        return params

    def _parse_turnover(self, value):
        """Parse turnover value - convert percentage to decimal"""
        return float(value)

    def _parse_volume(self, value):
        """Parse volume value - return None to indicate 'any large volume'"""
        return None  # Will be handled as a minimum threshold

    def _get_default_value(self, param_key):
        """Get default value for a parameter"""
        defaults = {
            'price_min': None,
            'price_max': None,
            'turnover_min': None,
            'turnover_max': None,
            'pct_chg_min': None,
            'pct_chg_max': None,
            'volume_min': None,
            'volume_max': None,
            'days': 5,
        }
        return defaults.get(param_key)

    def _build_response(self, query, params, history):
        """Build a natural language response"""
        response_parts = ["好的，我来帮您筛选股票。"]

        if params.get('price_min'):
            response_parts.append(f"价格高于{params['price_min']}元")
        if params.get('price_max'):
            response_parts.append(f"价格低于{params['price_max']}元")
        if params.get('turnover_min'):
            response_parts.append(f"换手率大于{params['turnover_min']}%")
        if params.get('pct_chg_min'):
            response_parts.append(f"涨幅超过{params['pct_chg_min']}%")
        if params.get('pct_chg_min') == 'continuous':
            response_parts.append("连续上涨")

        if len(response_parts) > 1:
            return "，".join(response_parts) + "的股票。点击\"应用\"开始筛选。"
        else:
            return "好的，我来帮您分析。请稍等..."  # Default response


# Global processor instance
claude_processor = None


def init_claude_cli():
    """
    Initialize Claude CLI processor (called on app startup)

    Returns:
        bool: True if successful, False otherwise
    """
    global claude_processor
    try:
        claude_processor = MockClaudeProcessor()
        logger.info("[Claude CLI] Mock processor initialized successfully")
        return True
    except Exception as e:
        logger.error(f"[Claude CLI] Initialization failed: {e}")
        return False


def register_socketio_events(socketio):
    """
    Register WebSocket event handlers

    Args:
        socketio: Flask-SocketIO instance
    """

    @socketio.on('connect')
    def handle_connect():
        """Handle WebSocket connection"""
        logger.info(f"[WebSocket] Client connected: {request.sid}")

    @socketio.on('disconnect')
    def handle_disconnect():
        """Handle WebSocket disconnection"""
        logger.info(f"[WebSocket] Client disconnected: {request.sid}")

    @socketio.on('ai_screen_connect')
    def handle_ai_screen_connect():
        """Handle AI Screen WebSocket connection"""
        logger.info("[WebSocket] AI Screen client connected")
        emit('ai_screen_status', {'status': 'connected', 'message': 'AI 服务已连接'})

    @socketio.on('ai_screen_disconnect')
    def handle_ai_screen_disconnect():
        """Handle AI Screen WebSocket disconnection"""
        logger.info("[WebSocket] AI Screen client disconnected")

    @socketio.on('ai_screen_query')
    def handle_ai_screen_query(data):
        """
        Handle AI screening query

        Expected data format:
        {
            "query": "查找最近5天涨幅超过5%的股票"
        }
        """
        query = data.get('query', '').strip()

        if not query:
            emit('ai_screen_error', {'error': '查询不能为空'})
            return

        try:
            logger.info(f"[AI Screen] Processing query: {query}")
            emit('ai_screen_progress', {'message': '正在分析查询...'})

            if not claude_processor:
                emit('ai_screen_error', {'error': 'AI 服务未初始化'})
                return

            # Process the query
            params = claude_processor.send_query(query)

            # Check if we extracted any meaningful parameters
            has_params = any(v is not None and v != 5 for k, v in params.items() if k != 'days')

            if has_params:
                logger.info(f"[AI Screen] Extracted parameters: {params}")
                emit('ai_screen_result', {
                    'success': True,
                    'params': params,
                    'query': query
                })
            else:
                emit('ai_screen_error', {
                    'error': '未能提取到明确的筛选参数，请尝试更具体的描述'
                })

        except Exception as e:
            logger.error(f"[AI Screen] Query processing failed: {e}")
            emit('ai_screen_error', {'error': f'处理失败: {str(e)}'})

    @socketio.on('ai_screen_chat')
    def handle_ai_screen_chat(data):
        """
        Handle AI chat-based screening

        Expected data format:
        {
            "query": "帮我找一些科技股",
            "history": [
                {"role": "user", "content": "..."},
                {"role": "assistant", "content": "..."}
            ]
        }
        """
        query = data.get('query', '').strip()
        history = data.get('history', [])

        if not query:
            emit('ai_screen_error', {'error': '查询不能为空'})
            return

        try:
            logger.info(f"[AI Screen Chat] Processing query: {query}")
            emit('ai_screen_progress', {'message': '正在思考...'})

            if not claude_processor:
                emit('ai_screen_error', {'error': 'AI 服务未初始化'})
                return

            # Process with conversation context
            response_text, params = claude_processor.send_chat(query, history)

            logger.info(f"[AI Screen Chat] Response: {response_text[:50]}...")
            emit('ai_screen_chat_result', {
                'response': response_text,
                'params': params
            })

        except Exception as e:
            logger.error(f"[AI Screen Chat] Processing failed: {e}")
            emit('ai_screen_error', {'error': f'处理失败: {str(e)}'})


def build_screen_prompt(query):
    """Build prompt for single-shot screening"""
    # In production, this would use the actual Claude client
    return f"请从以下查询中提取股票筛选参数：\n\n{query}"


def build_chat_prompt(query, history):
    """Build prompt for conversational screening"""
    # In production, this would use the actual Claude client
    prompt = ""
    if history:
        prompt += "对话历史：\n"
        for msg in history[-5:]:
            role = "用户" if msg.get('role') == 'user' else "助手"
            prompt += f"{role}: {msg.get('content', '')}\n"
        prompt += "\n"

    prompt += f"用户查询: {query}\n\n你的回应:"
    return prompt
