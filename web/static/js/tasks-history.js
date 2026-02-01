/**
 * 任务历史管理
 * 加载、显示和管理所有任务的历史记录
 */

class TasksHistory {
    constructor() {
        this.currentFilter = {
            status: '',
            type: ''
        };
        this.currentPage = 1;
        this.pageSize = 10;
        this.init();
    }

    init() {
        // 绑定刷新按钮
        document.getElementById('refreshTasksBtn').addEventListener('click', () => {
            this.loadTasks();
        });

        // 绑定清理旧任务按钮
        document.getElementById('clearOldTasksBtn').addEventListener('click', () => {
            this.clearOldTasks();
        });

        // 绑定筛选器
        document.getElementById('taskStatusFilter').addEventListener('change', (e) => {
            this.currentFilter.status = e.target.value;
            this.currentPage = 1;
            this.loadTasks();
        });

        document.getElementById('taskTypeFilter').addEventListener('change', (e) => {
            this.currentFilter.type = e.target.value;
            this.currentPage = 1;
            this.loadTasks();
        });

        // 当切换到任务历史tab时自动加载
        document.getElementById('tasks-tab').addEventListener('shown.bs.tab', () => {
            this.loadTasks();
        });

        // 初始加载
        this.loadTasks();
    }

    async loadTasks() {
        console.log('[loadTasks] Loading tasks...');
        try {
            const response = await fetch('/api/tasks');
            const data = await response.json();
            console.log('[loadTasks] Received data:', data);

            if (data.success) {
                let tasks = data.tasks;
                console.log('[loadTasks] Total tasks:', tasks.length);

                // 应用筛选器
                if (this.currentFilter.status) {
                    tasks = tasks.filter(t => t.status === this.currentFilter.status);
                }
                if (this.currentFilter.type) {
                    tasks = tasks.filter(t => t.task_type === this.currentFilter.type);
                }

                console.log('[loadTasks] Filtered tasks:', tasks.length);
                this.displayTasks(tasks);
            } else {
                this.showError('加载任务列表失败');
            }
        } catch (error) {
            console.error('Error loading tasks:', error);
            this.showError('加载任务列表失败: ' + error.message);
        }
    }

    displayTasks(tasks) {
        console.log('[displayTasks] Displaying', tasks.length, 'tasks');
        const tbody = document.getElementById('tasksBody');

        if (tasks.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="7" class="text-center text-muted">
                        <i class="bi bi-inbox" style="font-size: 2rem;"></i>
                        <p class="mt-2 mb-0">暂无任务记录</p>
                    </td>
                </tr>
            `;
            return;
        }

        // 分页
        const startIdx = (this.currentPage - 1) * this.pageSize;
        const endIdx = startIdx + this.pageSize;
        const pageTasks = tasks.slice(startIdx, endIdx);

        console.log('[displayTasks] Rendering page tasks:', pageTasks.length);
        tbody.innerHTML = pageTasks.map(task => this.renderTaskRow(task)).join('');

        // 渲染分页控件
        this.renderPagination(tasks.length);
    }

    renderTaskRow(task) {
        const statusBadge = this.getStatusBadge(task.status);
        const typeLabel = this.getTypeLabel(task.task_type);
        const progress = task.total_stocks > 0
            ? `${task.current_stock_index || 0}/${task.total_stocks}`
            : '-';
        const stats = task.stats
            ? `成功:${task.stats.success} 失败:${task.stats.failed} 跳过:${task.stats.skipped}`
            : '-';
        const createdAt = new Date(task.created_at).toLocaleString('zh-CN');

        return `
            <tr>
                <td>
                    <small class="font-monospace">${task.task_id.substring(0, 8)}...</small>
                </td>
                <td>${typeLabel}</td>
                <td>${statusBadge}</td>
                <td>
                    <div class="d-flex align-items-center">
                        <span>${progress}</span>
                        ${task.progress > 0 ? `
                            <div class="progress ms-2" style="width: 60px; height: 6px;">
                                <div class="progress-bar" style="width: ${task.progress}%"></div>
                            </div>
                        ` : ''}
                    </div>
                </td>
                <td><small>${stats}</small></td>
                <td><small>${createdAt}</small></td>
                <td>
                    ${this.renderActionButtons(task)}
                </td>
            </tr>
        `;
    }

    getStatusBadge(status) {
        const badges = {
            'pending': '<span class="badge bg-secondary">待执行</span>',
            'running': '<span class="badge bg-primary">运行中</span>',
            'paused': '<span class="badge bg-warning text-dark">已暂停</span>',
            'completed': '<span class="badge bg-success">已完成</span>',
            'failed': '<span class="badge bg-danger">失败</span>',
            'stopped': '<span class="badge bg-secondary">已停止</span>',
            'cancelled': '<span class="badge bg-secondary">已取消</span>'
        };
        return badges[status] || `<span class="badge bg-secondary">${status}</span>`;
    }

    getTypeLabel(type) {
        const labels = {
            'update_favorites': '股票更新',
            'update_all_stocks': '股票更新'
        };
        return labels[type] || type;
    }

    renderActionButtons(task) {
        let buttons = '';

        // 查看详情
        buttons += `
            <button class="btn btn-sm btn-outline-info" onclick="tasksHistory.showTaskDetails('${task.task_id}')" title="查看详情">
                <i class="bi bi-eye"></i>
            </button>
        `;

        // 运行中的任务可以暂停/停止
        if (task.status === 'running') {
            buttons += `
                <button class="btn btn-sm btn-outline-warning" onclick="tasksHistory.pauseTask('${task.task_id}')" title="暂停">
                    <i class="bi bi-pause"></i>
                </button>
                <button class="btn btn-sm btn-outline-danger" onclick="tasksHistory.stopTask('${task.task_id}')" title="停止">
                    <i class="bi bi-stop"></i>
                </button>
            `;
        }

        // 暂停的任务可以恢复/停止
        if (task.status === 'paused') {
            buttons += `
                <button class="btn btn-sm btn-outline-success" onclick="tasksHistory.resumeTask('${task.task_id}')" title="恢复">
                    <i class="bi bi-play"></i>
                </button>
                <button class="btn btn-sm btn-outline-danger" onclick="tasksHistory.stopTask('${task.task_id}')" title="停止">
                    <i class="bi bi-stop"></i>
                </button>
            `;
        }

        // 待执行的任务可以停止（取消）/删除
        if (task.status === 'pending') {
            buttons += `
                <button class="btn btn-sm btn-outline-danger" onclick="tasksHistory.stopTask('${task.task_id}')" title="取消">
                    <i class="bi bi-x-circle"></i>
                </button>
                <button class="btn btn-sm btn-outline-danger" onclick="tasksHistory.deleteTask('${task.task_id}')" title="删除">
                    <i class="bi bi-trash"></i>
                </button>
            `;
        }

        // 已完成、失败、停止、取消的任务可以删除
        if (['completed', 'failed', 'stopped', 'cancelled'].includes(task.status)) {
            console.log('[renderActionButtons] Adding delete button for task:', task.task_id.substring(0, 8), 'status:', task.status);
            buttons += `
                <button class="btn btn-sm btn-outline-danger" onclick="tasksHistory.deleteTask('${task.task_id}')" title="删除">
                    <i class="bi bi-trash"></i>
                </button>
            `;
        }

        return buttons;
    }

    renderPagination(totalItems) {
        const totalPages = Math.ceil(totalItems / this.pageSize);
        const pagination = document.getElementById('tasksPagination');

        if (totalPages <= 1) {
            pagination.innerHTML = '';
            return;
        }

        let html = '';

        // 上一页
        html += `
            <li class="page-item ${this.currentPage === 1 ? 'disabled' : ''}">
                <a class="page-link" href="#" onclick="tasksHistory.goToPage(${this.currentPage - 1}); return false;">上一页</a>
            </li>
        `;

        // 页码
        for (let i = 1; i <= totalPages; i++) {
            html += `
                <li class="page-item ${i === this.currentPage ? 'active' : ''}">
                    <a class="page-link" href="#" onclick="tasksHistory.goToPage(${i}); return false;">${i}</a>
                </li>
            `;
        }

        // 下一页
        html += `
            <li class="page-item ${this.currentPage === totalPages ? 'disabled' : ''}">
                <a class="page-link" href="#" onclick="tasksHistory.goToPage(${this.currentPage + 1}); return false;">下一页</a>
            </li>
        `;

        pagination.innerHTML = html;
    }

    goToPage(page) {
        this.currentPage = page;
        this.loadTasks();
    }

    async pauseTask(taskId) {
        try {
            const response = await fetch(`/api/tasks/${taskId}/pause`, {
                method: 'POST'
            });
            const data = await response.json();

            if (data.success) {
                this.showToast('任务暂停请求已发送', 'success');
                this.loadTasks();
            } else {
                this.showError(data.error || '暂停失败');
            }
        } catch (error) {
            this.showError('暂停失败: ' + error.message);
        }
    }

    async resumeTask(taskId) {
        try {
            const response = await fetch(`/api/tasks/${taskId}/resume`, {
                method: 'POST'
            });
            const data = await response.json();

            if (data.success) {
                this.showToast('任务已恢复', 'success');
                this.loadTasks();
            } else {
                this.showError(data.error || '恢复失败');
            }
        } catch (error) {
            this.showError('恢复失败: ' + error.message);
        }
    }

    async stopTask(taskId) {
        if (!confirm('确定要停止这个任务吗？')) {
            return;
        }

        try {
            const response = await fetch(`/api/tasks/${taskId}/stop`, {
                method: 'POST'
            });
            const data = await response.json();

            if (data.success) {
                this.showToast('任务停止请求已发送', 'success');
                this.loadTasks();
            } else {
                this.showError(data.error || '停止失败');
            }
        } catch (error) {
            this.showError('停止失败: ' + error.message);
        }
    }

    async deleteTask(taskId) {
        console.log('[deleteTask] Called with taskId:', taskId);
        if (!confirm('确定要删除这个任务记录吗？')) {
            console.log('[deleteTask] User cancelled');
            return;
        }

        try {
            console.log('[deleteTask] Sending DELETE request to:', `/api/tasks/${taskId}`);
            const response = await fetch(`/api/tasks/${taskId}`, {
                method: 'DELETE'
            });
            const data = await response.json();
            console.log('[deleteTask] Response:', data);

            if (data.success) {
                this.showToast('任务已删除', 'success');
                console.log('[deleteTask] Reloading tasks...');
                this.loadTasks();
            } else {
                this.showError(data.error || '删除失败');
            }
        } catch (error) {
            console.error('[deleteTask] Error:', error);
            this.showError('删除失败: ' + error.message);
        }
    }

    async clearOldTasks() {
        const hours = prompt('清理多少小时之前的已完成/失败任务？', '168'); // 默认7天

        if (!hours || isNaN(hours)) {
            return;
        }

        try {
            const response = await fetch(`/api/tasks/cleanup?max_age_hours=${hours}`, {
                method: 'POST'
            });
            const data = await response.json();

            if (data.success) {
                this.showToast(`已清理 ${data.deleted_count} 个旧任务`, 'success');
                this.loadTasks();
            } else {
                this.showError(data.error || '清理失败');
            }
        } catch (error) {
            this.showError('清理失败: ' + error.message);
        }
    }

    showTaskDetails(taskId) {
        // TODO: 实现任务详情模态框
        alert('任务详情功能开发中...\n任务ID: ' + taskId);
    }

    showToast(message, type = 'info') {
        // 使用现有的toast系统
        const toastContainer = document.getElementById('toastContainer');
        const toastEl = document.createElement('div');
        toastEl.className = `toast align-items-center text-white bg-${type === 'error' ? 'danger' : type === 'success' ? 'success' : 'primary'} border-0`;
        toastEl.setAttribute('role', 'alert');
        toastEl.innerHTML = `
            <div class="d-flex">
                <div class="toast-body">${message}</div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
            </div>
        `;
        toastContainer.appendChild(toastEl);

        const toast = new bootstrap.Toast(toastEl);
        toast.show();

        toastEl.addEventListener('hidden.bs.toast', () => {
            toastEl.remove();
        });
    }

    showError(message) {
        this.showToast(message, 'error');
    }
}

// 初始化任务历史管理器
let tasksHistory;
document.addEventListener('DOMContentLoaded', () => {
    tasksHistory = new TasksHistory();
});
