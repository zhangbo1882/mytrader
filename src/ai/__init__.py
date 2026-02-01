# src/ai/__init__.py
"""
AI integration package for stock screening
"""
from .claude_client import ClaudeClient, create_claude_client

__all__ = ['ClaudeClient', 'create_claude_client']
