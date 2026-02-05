"""
Worker Service for Background Task Execution

This package provides a separate worker process that polls the database
for pending tasks and executes them independently from the Flask API.

Key components:
- TaskWorker: Main worker class that polls and executes tasks
- Task Handlers: Functions that execute specific task types
- Utilities: Helper functions for task execution
"""
