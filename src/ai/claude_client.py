# src/ai/claude_client.py
"""
Claude Code CLI client for parsing natural language stock screening queries
Uses subprocess to call Claude CLI directly instead of API calls
"""
import subprocess
import json
import re
import shutil
from typing import Optional, Dict, Any
import os


class ClaudeClient:
    """Claude Code CLI client for natural language to structured parameters conversion"""

    # System prompt for parameter extraction
    SYSTEM_PROMPT = """你是一个股票筛选参数提取专家。从用户的自然语言描述中提取结构化参数。

可用参数：
- days: 筛选天数（1-365），表示最近N个交易日
- turnover_min/max: 换手率范围（0-100%）
- pct_chg_min/max: 涨跌幅范围（-100%到100%）
- price_min/max: 价格区间（0-10000元）
- volume_min/max: 成交量区间（手）

规则：
1. 只提取明确提到的参数，未提及的设为null
2. 识别中文表达：
   - "最近N天"/"N天内" → days=N
   - "20元以下"/"低于20元" → price_max=20
   - "5元以上" → price_min=5
   - "涨幅超过5%"/"涨幅大于5%" → pct_chg_min=5
   - "跌幅小于3%" → pct_chg_max=-3
   - "换手率大于3%"/"换手率超过3%" → turnover_min=3
   - "高换手率" → turnover_min=5
   - "低换手率" → turnover_max=3
3. 处理模糊表达：
   - "低价"/"便宜" → price_max=20
   - "高价"/"贵" → price_min=50
   - "高换手率" → turnover_min=5
   - "低换手率" → turnover_max=3
   - "放量" → volume_min=10000
4. 价格单位默认为元，成交量单位默认为手
5. 返回JSON格式，只返回JSON对象，不要有其他文字

示例：
输入: "查找最近5天涨幅超过5%的股票"
输出: {"days": 5, "pct_chg_min": 5}

输入: "价格低于20元且换手率大于3%的股票"
输出: {"price_max": 20, "turnover_min": 3}

输入: "低价高换手率的股票"
输出: {"price_max": 20, "turnover_min": 5}

输入: "查找连续上涨的股票"
输出: {}
"""

    def __init__(
        self,
        cli_path: Optional[str] = None,
        timeout: int = 60
    ):
        """
        Initialize Claude CLI client

        Args:
            cli_path: Path to claude CLI executable (default: auto-detect from PATH)
            timeout: Command timeout in seconds
        """
        self.cli_path = cli_path
        self.timeout = timeout

        # Auto-detect CLI path if not provided
        if self.cli_path is None:
            self.cli_path = self._find_claude_cli()

        if not self.cli_path:
            raise RuntimeError(
                "Claude CLI not found. Please ensure Claude Code CLI is installed "
                "and available in PATH, or provide cli_path parameter."
            )

    def _find_claude_cli(self) -> Optional[str]:
        """Find Claude CLI executable in PATH"""
        # Try common claude command names
        for cmd_name in ['claude', 'claude-cli']:
            path = shutil.which(cmd_name)
            if path:
                return path

        # Try default installation paths
        default_paths = [
            os.path.expanduser('~/.local/bin/claude'),
            '/usr/local/bin/claude',
            os.path.expanduser('~/bin/claude'),
        ]
        for path in default_paths:
            if os.path.exists(path) and os.access(path, os.X_OK):
                return path

        return None

    def _build_prompt(self, query: str, language: str = 'zh') -> str:
        """Build complete prompt for Claude CLI"""
        prompt = self.SYSTEM_PROMPT + "\n\n"
        prompt += f"请从以下查询中提取股票筛选参数（只返回JSON，不要有其他文字）：\n\n{query}"
        return prompt

    def parse_screening_query(self, query: str, language: str = 'zh') -> Dict[str, Any]:
        """
        Parse natural language query to extract structured parameters

        Args:
            query: Natural language query text
            language: Query language ('zh' for Chinese, 'en' for English)

        Returns:
            Dictionary with extracted parameters
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        try:
            # Build prompt
            prompt = self._build_prompt(query, language)

            # Call Claude CLI via subprocess
            result = subprocess.run(
                [self.cli_path, prompt],
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env={**os.environ, 'NO_COLOR': '1'}  # Disable color codes
            )

            # Check for errors
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                raise RuntimeError(f"Claude CLI error: {error_msg}")

            # Extract response text
            response_text = result.stdout.strip()

            # Parse JSON response
            # Clean up response text (remove markdown code blocks if present)
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.startswith("```"):
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()

            # Try to find JSON in response
            json_match = re.search(r'\{[^{}]*\}', response_text)
            if json_match:
                response_text = json_match.group(0)

            params = json.loads(response_text)

            # Validate and clean parameters
            return self.validate_params(params)

        except subprocess.TimeoutExpired:
            raise RuntimeError("Claude CLI request timed out. Please try again.")
        except FileNotFoundError:
            raise RuntimeError(f"Claude CLI not found at {self.cli_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse CLI response as JSON: {e}\nResponse was: {response_text}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error: {e}")

    def parse_screening_query_chat(self, query: str, history: list = None, language: str = 'zh') -> Dict[str, Any]:
        """
        Parse natural language query with conversational response support

        Args:
            query: Natural language query text
            history: Conversation history (list of {role, content} dicts)
            language: Query language ('zh' for Chinese, 'en' for English)

        Returns:
            Dictionary with:
                - response: AI conversational response
                - params: Extracted parameters (or None if not applicable)
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        try:
            # Build conversational prompt
            prompt = self._build_chat_prompt(query, history, language)

            # Call Claude CLI via subprocess
            result = subprocess.run(
                [self.cli_path, prompt],
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env={**os.environ, 'NO_COLOR': '1'}
            )

            # Check for errors
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                raise RuntimeError(f"Claude CLI error: {error_msg}")

            # Extract response text
            response_text = result.stdout.strip()

            # Parse conversational response and extract parameters
            return self._parse_chat_response(response_text, query)

        except subprocess.TimeoutExpired:
            raise RuntimeError("Claude CLI request timed out. Please try again.")
        except FileNotFoundError:
            raise RuntimeError(f"Claude CLI not found at {self.cli_path}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error: {e}")

    def _build_chat_prompt(self, query: str, history: list = None, language: str = 'zh') -> str:
        """Build conversational prompt for Claude CLI"""
        prompt = self.SYSTEM_PROMPT + "\n\n"

        # Add conversation context
        if history and len(history) > 0:
            prompt += "对话历史：\n"
            for msg in history[-5:]:  # Only include last 5 messages for context
                role = "用户" if msg.get('role') == 'user' else "助手"
                prompt += f"{role}: {msg.get('content', '')}\n"
            prompt += "\n"

        prompt += f"""现在，请以对话的方式回应用户。如果用户的查询包含股票筛选条件，请：
1. 用友好的对话语气回应
2. 总结你理解的筛选条件
3. 在回应的最后，用JSON格式提供提取的参数（只JSON，不要有其他文字）

用户查询: {query}

你的回应:"""

        return prompt

    def _parse_chat_response(self, response_text: str, original_query: str) -> Dict[str, Any]:
        """
        Parse conversational response and extract parameters

        Args:
            response_text: Full response from Claude
            original_query: Original user query

        Returns:
            Dictionary with response and optional params
        """
        # Clean up response
        cleaned_response = response_text.strip()

        # Try to extract JSON from the response
        params = None
        json_match = re.search(r'\{[^{}]*\}', cleaned_response)
        if json_match:
            try:
                json_text = json_match.group(0)
                params = json.loads(json_text)
                params = self.validate_params(params)

                # Remove JSON from response text for cleaner display
                cleaned_response = cleaned_response.replace(json_text, '').strip()
            except (json.JSONDecodeError, ValueError):
                pass  # Keep original response if JSON parsing fails

        # If no params found but query looks like a screening query, try to extract
        if params is None or len(params) == 0:
            try:
                params = self.parse_screening_query(original_query)
            except:
                params = None

        # Build conversational response
        if not cleaned_response:
            if params and len(params) > 0:
                cleaned_response = "好的，我来帮你筛选符合条件的股票。"
            else:
                cleaned_response = "请提供更具体的筛选条件，比如价格区间、换手率、涨跌幅等。"

        return {
            'response': cleaned_response,
            'params': params
        }

    def validate_params(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and clean extracted parameters

        Args:
            params: Raw parameters from AI

        Returns:
            Validated and cleaned parameters
        """
        validated = {}

        # Define parameter ranges and types
        param_specs = {
            'days': {'min': 1, 'max': 365, 'type': int},
            'turnover_min': {'min': 0, 'max': 100, 'type': float},
            'turnover_max': {'min': 0, 'max': 100, 'type': float},
            'pct_chg_min': {'min': -100, 'max': 100, 'type': float},
            'pct_chg_max': {'min': -100, 'max': 100, 'type': float},
            'price_min': {'min': 0, 'max': 10000, 'type': float},
            'price_max': {'min': 0, 'max': 10000, 'type': float},
            'volume_min': {'min': 0, 'max': 100000000, 'type': int},
            'volume_max': {'min': 0, 'max': 100000000, 'type': int},
        }

        for key, value in params.items():
            if value is None:
                validated[key] = None
                continue

            # Check if parameter is valid
            if key not in param_specs:
                continue  # Skip unknown parameters

            spec = param_specs[key]

            # Convert to correct type
            try:
                if spec['type'] == int:
                    value = int(float(value))  # Handle "5.0" as 5
                else:
                    value = float(value)
            except (ValueError, TypeError):
                continue  # Skip invalid values

            # Validate range
            if value < spec['min'] or value > spec['max']:
                continue  # Skip out-of-range values

            validated[key] = value

        # Validate min/max pairs
        # Ensure min <= max for paired parameters
        for base in ['turnover', 'pct_chg', 'price', 'volume']:
            min_key = f'{base}_min'
            max_key = f'{base}_max'

            if min_key in validated and max_key in validated:
                if validated[min_key] > validated[max_key]:
                    # Swap values if min > max
                    validated[min_key], validated[max_key] = validated[max_key], validated[min_key]

        return validated

    def health_check(self) -> Dict[str, Any]:
        """
        Check if Claude CLI is accessible

        Returns:
            Dictionary with health status
        """
        try:
            result = subprocess.run(
                [self.cli_path, '--version'],
                capture_output=True,
                text=True,
                timeout=10
            )

            version_info = result.stdout.strip() or result.stderr.strip()

            return {
                'status': 'ok',
                'cli_path': self.cli_path,
                'version': version_info
            }
        except FileNotFoundError:
            return {
                'status': 'error',
                'error': f'Claude CLI not found at {self.cli_path}'
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }


# Factory function for easy initialization
def create_claude_client() -> Optional[ClaudeClient]:
    """
    Create ClaudeClient from settings

    Returns:
        ClaudeClient instance or None if configuration is missing
    """
    try:
        from config.settings import CLAUDE_TIMEOUT, CLAUDE_CLI_PATH
    except ImportError:
        CLAUDE_TIMEOUT = 60
        CLAUDE_CLI_PATH = ""

    try:
        cli_path = CLAUDE_CLI_PATH if CLAUDE_CLI_PATH else None
        return ClaudeClient(
            cli_path=cli_path,
            timeout=CLAUDE_TIMEOUT
        )
    except RuntimeError:
        # CLI not found, return None
        return None
