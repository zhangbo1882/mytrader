"""
自定义异常
"""


class TaskExistsError(Exception):
    """尝试创建任务时活动任务已存在"""
    def __init__(self, message, existing_task=None):
        super().__init__(message)
        self.message = message
        self.existing_task = existing_task
        self.error_type = 'task_exists'
