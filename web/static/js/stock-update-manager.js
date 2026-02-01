/**
 * Stock Update Manager - Frontend Logic
 */

// Global state
let currentTaskId = null;
let pollInterval = null;
let isPolling = false;

// Status badge classes
const statusClasses = {
    'pending': 'status-pending',
    'running': 'status-running',
    'paused': 'status-paused',
    'completed': 'status-completed',
    'failed': 'status-failed',
    'stopped': 'status-stopped',
    'cancelled': 'status-cancelled'
};

const statusLabels = {
    'pending': '等待中',
    'running': '运行中',
    'paused': '已暂停',
    'completed': '已完成',
    'failed': '失败',
    'stopped': '已停止',
    'cancelled': '已取消'
};

// Initialize on document ready
$(document).ready(function() {
    initializeEventHandlers();
    // Check for running tasks on page load
    checkForRunningTasks();
});

/**
 * Check for running tasks and auto-restore UI state
 */
function checkForRunningTasks() {
    fetch('/api/tasks')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.tasks) {
                // Find active tasks (running, paused, pending)
                const activeTasks = data.tasks.filter(t =>
                    ['running', 'paused', 'pending'].includes(t.status)
                );

                if (activeTasks.length > 0) {
                    // Get the most recent active task
                    const task = activeTasks[0];
                    console.log('[Auto-restore] Found active task:', task.task_id, 'status:', task.status);

                    // Restore UI state
                    currentTaskId = task.task_id;
                    updateTaskUI(task);

                    // Start polling if task is running or paused
                    if (task.status === 'running' || task.status === 'paused') {
                        startPolling();
                    }
                }
            }
        })
        .catch(error => {
            console.error('[Auto-restore] Failed to check tasks:', error);
        });
}

/**
 * Initialize event handlers - scoped to update-manager pane only
 */
function initializeEventHandlers() {
    // Stock range change handler
    $('#stockRange').on('change', function() {
        if ($(this).val() === 'custom') {
            $('#customStocksGroup').show();
        } else {
            $('#customStocksGroup').hide();
        }
    });

    // Update form submit
    $('#updateForm').on('submit', function(e) {
        e.preventDefault();
        startUpdate();
    });

    // Start update button
    $('#startBtn').on('click', function(e) {
        e.preventDefault();
        startUpdate();
    });

    // Control buttons
    $('#pauseBtn').on('click', pauseTask);
    $('#resumeBtn').on('click', resumeTask);
    $('#stopBtn').on('click', stopTask);

    // Cron preset buttons
    $('.cron-presets button').on('click', function() {
        const cron = $(this).data('cron');
        $('#cronExpression').val(cron);
    });

    // Save schedule button
    $('#saveScheduleBtn').on('click', saveScheduledJob);

    // Tab change handlers - load scheduled jobs when update tab is shown
    $('#update-tab').on('shown.bs.tab', function() {
        loadScheduledJobs();
    });
}

/**
 * Show toast notification (renamed to avoid conflict with main.js)
 */
function showUpdateToast(message, type = 'info') {
    const bgClass = {
        'success': 'bg-success',
        'error': 'bg-danger',
        'warning': 'bg-warning',
        'info': 'bg-info'
    }[type] || 'bg-info';

    const toast = $(`
        <div class="toast align-items-center text-white ${bgClass} border-0" role="alert">
            <div class="d-flex">
                <div class="toast-body">${message}</div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
            </div>
        </div>
    `);

    $('#toastContainer').append(toast);
    const bsToast = new bootstrap.Toast(toast[0], { delay: 3000 });
    bsToast.show();

    toast.on('hidden.bs.toast', function() {
        $(this).remove();
    });
}

/**
 * Start stock update
 */
async function startUpdate() {
    const mode = $('#updateMode').val();
    const stockRange = $('#stockRange').val();
    const customStocks = $('#customStocks').val()
        .split('\n')
        .map(s => s.trim())
        .filter(s => s);

    // Get favorites from localStorage
    let favoritesStocks = [];
    if (stockRange === 'favorites') {
        const favoritesData = JSON.parse(localStorage.getItem('stock_favorites') || '[]');
        favoritesStocks = favoritesData.map(f => f.code);
        console.log('Favorites from localStorage:', favoritesStocks);
    }

    // Validate
    if (stockRange === 'custom' && customStocks.length === 0) {
        showUpdateToast('请输入自定义股票代码', 'error');
        return;
    }
    if (stockRange === 'favorites' && favoritesStocks.length === 0) {
        showUpdateToast('收藏列表为空，请先添加收藏', 'error');
        return;
    }

    try {
        const requestBody = {
            mode: mode,
            stock_range: stockRange,
            custom_stocks: stockRange === 'favorites' ? favoritesStocks : customStocks
        };

        const response = await fetch('/api/stock/update-all', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });

        const data = await response.json();

        if (data.success) {
            currentTaskId = data.task_id;
            showUpdateToast('更新任务已启动', 'success');
            startPolling();
        } else {
            // 检查错误类型并显示详细对话框
            if (data.error_type === 'task_exists') {
                showTaskExistsErrorDialog(data);
            } else {
                showUpdateToast(data.error || '启动失败', 'error');
            }
        }
    } catch (error) {
        console.error('Error starting update:', error);
        showUpdateToast('请求失败: ' + error.message, 'error');
    }
}

/**
 * Start polling task status
 */
function startPolling() {
    if (isPolling) return;
    isPolling = true;
    pollTaskStatus();
    pollInterval = setInterval(pollTaskStatus, 2000);
}

/**
 * Stop polling task status
 */
function stopPolling() {
    isPolling = false;
    if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
    }
}

/**
 * Poll task status from server
 */
async function pollTaskStatus() {
    if (!currentTaskId) return;

    try {
        const response = await fetch(`/api/tasks/${currentTaskId}`);
        const data = await response.json();

        if (data.success && data.task) {
            updateTaskUI(data.task);

            // Stop polling if task is finished
            if (['completed', 'failed', 'stopped', 'cancelled'].includes(data.task.status)) {
                stopPolling();
                showUpdateToast('任务' + statusLabels[data.task.status], data.task.status === 'completed' ? 'success' : 'info');
            }
        }
    } catch (error) {
        console.error('Error polling task:', error);
    }
}

/**
 * Update task UI with current status
 */
function updateTaskUI(task) {
    const status = task.status;

    // Show/hide cards
    if (status === 'pending' || status === 'running' || status === 'paused') {
        $('#taskStatusCard').show();
        $('#noTaskCard').hide();
    } else {
        // Keep showing for a bit after completion
        if (!isPolling) {
            setTimeout(() => {
                $('#taskStatusCard').hide();
                $('#noTaskCard').show();
            }, 5000);
        }
    }

    // Update status badge
    const $badge = $('#taskStatusBadge');
    $badge.removeClass(Object.values(statusClasses).join(' '));
    $badge.addClass(statusClasses[status] || 'status-pending');
    $badge.text(statusLabels[status] || status);

    // Update message
    $('#taskMessage').text(task.message || '');

    // Update progress
    const current = task.current_stock_index || 0;
    const total = task.total_stocks || 0;
    const progress = task.progress || 0;

    $('#progressLabel').text(`进度: ${current}/${total}`);
    $('#progressPercent').text(`${progress}%`);
    $('#progressBar').css('width', `${progress}%`).text(`${progress}%`);

    // Update stats
    const stats = task.stats || { success: 0, failed: 0, skipped: 0 };
    $('#statTotal').text(total);
    $('#statSuccess').text(stats.success);
    $('#statFailed').text(stats.failed);
    $('#statSkipped').text(stats.skipped);

    // Update detailed results (show when task is completed or has failures/skips)
    updateDetailedResults(task);

    // Update task details
    updateTaskDetails(task);

    // Update button states
    updateButtonStates(status);
}

/**
 * Update control button states based on task status
 */
function updateButtonStates(status) {
    const $pauseBtn = $('#pauseBtn');
    const $resumeBtn = $('#resumeBtn');
    const $stopBtn = $('#stopBtn');

    // Reset
    $pauseBtn.prop('disabled', true).hide();
    $resumeBtn.prop('disabled', true).hide();
    $stopBtn.prop('disabled', true);

    if (status === 'running') {
        $pauseBtn.prop('disabled', false).show();
        $stopBtn.prop('disabled', false);
    } else if (status === 'paused') {
        $resumeBtn.prop('disabled', false).show();
        $stopBtn.prop('disabled', false);
    }
}

/**
 * Update detailed results (failed/skipped/success stock lists)
 */
function updateDetailedResults(task) {
    const $detailedResults = $('#detailedResults');
    const $failedSection = $('#failedStocksSection');
    const $skippedSection = $('#skippedStocksSection');
    const $successSection = $('#successStocksSection');

    // Get detailed results from task.result.details
    const details = (task.result && task.result.details) ? task.result.details : null;

    if (!details) {
        // No details yet, hide all sections
        $detailedResults.hide();
        $failedSection.hide();
        $skippedSection.hide();
        $successSection.hide();
        return;
    }

    // Show the detailed results container
    $detailedResults.show();

    // Update failed stocks
    const failedStocks = details.failed || [];
    if (failedStocks.length > 0) {
        $failedSection.show();
        $('#failedCount').text(failedStocks.length);
        $('#failedStocksList').html(failedStocks.map(stock =>
            `<span class="badge bg-danger me-1 mb-1" style="margin-right: 0.25rem; margin-bottom: 0.25rem; display: inline-block;">${escapeHtml(stock)}</span>`
        ).join(''));
    } else {
        $failedSection.hide();
    }

    // Update skipped stocks
    const skippedStocks = details.skipped || [];
    if (skippedStocks.length > 0) {
        $skippedSection.show();
        $('#skippedCount').text(skippedStocks.length);
        $('#skippedStocksList').html(skippedStocks.map(stock =>
            `<span class="badge bg-warning text-dark me-1 mb-1" style="margin-right: 0.25rem; margin-bottom: 0.25rem; display: inline-block;">${escapeHtml(stock)}</span>`
        ).join(''));
    } else {
        $skippedSection.hide();
    }

    // Update success stocks (only show if task is completed)
    const successStocks = details.success || [];
    const isCompleted = task.status === 'completed' || task.status === 'stopped';
    if (successStocks.length > 0 && isCompleted) {
        $successSection.show();
        $('#successCount').text(successStocks.length);
        $('#successStocksList').html(successStocks.map(stock =>
            `<span class="badge bg-success me-1 mb-1" style="margin-right: 0.25rem; margin-bottom: 0.25rem; display: inline-block;">${escapeHtml(stock)}</span>`
        ).join(''));
    } else {
        $successSection.hide();
    }
}

/**
 * Update task details section
 */
function updateTaskDetails(task) {
    // Update task ID
    $('#taskId').text(task.task_id || '-');

    // Update created time
    $('#taskCreatedAt').text(task.created_at ? formatDateTime(task.created_at) : '-');

    // Calculate and display duration
    const duration = calculateDuration(task.created_at, task.updated_at, task.completed_at);
    $('#taskDuration').text(duration);

    // Calculate and display speed
    const speed = calculateSpeed(task.current_stock_index, task.created_at, task.updated_at);
    $('#taskSpeed').text(speed);

    // Update task parameters
    const params = task.params || {};
    if (Object.keys(params).length > 0) {
        const paramsText = Object.entries(params)
            .map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
            .join('\n');
        $('#taskParams').html('<pre class="mb-0">' + escapeHtml(paramsText) + '</pre>');
    } else {
        $('#taskParams').text('-');
    }

    // Update error details
    if (task.error) {
        $('#errorDetails').show();
        $('#taskError').html('<pre class="mb-0">' + escapeHtml(task.error) + '</pre>');
    } else {
        $('#errorDetails').hide();
    }
}

/**
 * Calculate task duration
 */
function calculateDuration(createdAt, updatedAt, completedAt) {
    if (!createdAt) return '-';

    const start = new Date(createdAt);
    const end = completedAt ? new Date(completedAt) : (updatedAt ? new Date(updatedAt) : new Date());

    const diffMs = end - start;
    if (diffMs < 0) return '-';

    const diffSecs = Math.floor(diffMs / 1000);
    const diffMins = Math.floor(diffSecs / 60);
    const diffHours = Math.floor(diffMins / 60);

    if (diffHours > 0) {
        return `${diffHours}小时${diffMins % 60}分`;
    } else if (diffMins > 0) {
        return `${diffMins}分${diffSecs % 60}秒`;
    } else {
        return `${diffSecs}秒`;
    }
}

/**
 * Calculate task processing speed
 */
function calculateSpeed(currentIndex, createdAt, updatedAt) {
    if (!createdAt || !currentIndex || currentIndex === 0) return '-';

    const start = new Date(createdAt);
    const end = updatedAt ? new Date(updatedAt) : new Date();

    const diffMs = end - start;
    if (diffMs < 1000) return '-'; // Less than 1 second

    const diffMins = diffMs / 1000 / 60; // Convert to minutes
    const speed = currentIndex / diffMins;

    if (speed < 1) {
        // Less than 1 stock per minute, show stocks per hour
        const perHour = speed * 60;
        return `${perHour.toFixed(1)} 股/小时`;
    } else {
        return `${speed.toFixed(1)} 股/分钟`;
    }
}

/**
 * Pause task
 */
async function pauseTask() {
    if (!currentTaskId) return;

    try {
        const response = await fetch(`/api/tasks/${currentTaskId}/pause`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showUpdateToast('暂停请求已发送', 'info');
        } else {
            showUpdateToast(data.error || '暂停失败', 'error');
        }
    } catch (error) {
        console.error('Error pausing task:', error);
        showUpdateToast('请求失败: ' + error.message, 'error');
    }
}

/**
 * Resume task
 */
async function resumeTask() {
    if (!currentTaskId) return;

    try {
        const response = await fetch(`/api/tasks/${currentTaskId}/resume`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showUpdateToast('任务已恢复', 'success');
        } else {
            showUpdateToast(data.error || '恢复失败', 'error');
        }
    } catch (error) {
        console.error('Error resuming task:', error);
        showUpdateToast('请求失败: ' + error.message, 'error');
    }
}

/**
 * Stop task
 */
async function stopTask() {
    if (!currentTaskId) return;

    if (!confirm('确定要停止当前任务吗？进度将被保存，可以稍后恢复。')) {
        return;
    }

    try {
        const response = await fetch(`/api/tasks/${currentTaskId}/stop`, {
            method: 'POST'
        });
        const data = await response.json();

        if (data.success) {
            showUpdateToast('停止请求已发送', 'warning');
        } else {
            showUpdateToast(data.error || '停止失败', 'error');
        }
    } catch (error) {
        console.error('Error stopping task:', error);
        showUpdateToast('请求失败: ' + error.message, 'error');
    }
}

/**
 * Load scheduled jobs
 */
async function loadScheduledJobs() {
    try {
        const response = await fetch('/api/schedule/jobs');
        const data = await response.json();

        const $tbody = $('#scheduledJobsBody');

        if (data.success && data.jobs && data.jobs.length > 0) {
            $tbody.empty();

            data.jobs.forEach(job => {
                // Parse cron from trigger string
                const cronMatch = job.trigger.match(/cron\((.*?)\)/);
                const cron = cronMatch ? cronMatch[1] : job.trigger;

                const enabled = job.enabled !== false;

                const row = $(`
                    <tr>
                        <td>${escapeHtml(job.name)}</td>
                        <td><code>${escapeHtml(cron)}</code></td>
                        <td>-</td>
                        <td>${job.next_run_time ? formatDateTime(job.next_run_time) : '-'}</td>
                        <td>
                            <span class="badge ${enabled ? 'bg-success' : 'bg-secondary'}">
                                ${enabled ? '启用' : '禁用'}
                            </span>
                        </td>
                        <td>
                            <button class="btn btn-sm btn-outline-primary" onclick="toggleScheduledJob('${job.id}', ${!enabled})">
                                ${enabled ? '禁用' : '启用'}
                            </button>
                            <button class="btn btn-sm btn-outline-danger" onclick="deleteScheduledJob('${job.id}')">
                                删除
                            </button>
                        </td>
                    </tr>
                `);
                $tbody.append(row);
            });
        } else {
            $tbody.html('<tr><td colspan="6" class="text-center text-muted">暂无定时任务</td></tr>');
        }
    } catch (error) {
        console.error('Error loading scheduled jobs:', error);
        $('#scheduledJobsBody').html('<tr><td colspan="6" class="text-center text-danger">加载失败</td></tr>');
    }
}

/**
 * Save scheduled job
 */
async function saveScheduledJob() {
    const name = $('#scheduleName').val().trim();
    const cron = $('#cronExpression').val().trim();
    const mode = $('#scheduleMode').val();
    const stockRange = $('#scheduleStockRange').val();

    if (!name || !cron) {
        showUpdateToast('请填写任务名称和Cron表达式', 'error');
        return;
    }

    try {
        const response = await fetch('/api/schedule/jobs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                name: name,
                cron_expression: cron,
                mode: mode,
                stock_range: stockRange
            })
        });

        const data = await response.json();

        if (data.success) {
            showUpdateToast('定时任务已创建', 'success');
            bootstrap.Modal.getInstance($('#addScheduleModal')).hide();
            $('#scheduleForm')[0].reset();
            loadScheduledJobs();
        } else {
            showUpdateToast(data.error || '创建失败', 'error');
        }
    } catch (error) {
        console.error('Error saving scheduled job:', error);
        showUpdateToast('请求失败: ' + error.message, 'error');
    }
}

/**
 * Toggle scheduled job enable/disable
 */
async function toggleScheduledJob(jobId, enable) {
    const url = enable ?
        `/api/schedule/jobs/${jobId}/resume` :
        `/api/schedule/jobs/${jobId}/pause`;

    try {
        const response = await fetch(url, { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            showUpdateToast(enable ? '任务已启用' : '任务已禁用', 'success');
            loadScheduledJobs();
        } else {
            showUpdateToast(data.error || '操作失败', 'error');
        }
    } catch (error) {
        console.error('Error toggling job:', error);
        showUpdateToast('请求失败: ' + error.message, 'error');
    }
}

/**
 * Delete scheduled job
 */
async function deleteScheduledJob(jobId) {
    if (!confirm('确定要删除此定时任务吗？')) {
        return;
    }

    try {
        const response = await fetch(`/api/schedule/jobs/${jobId}`, {
            method: 'DELETE'
        });
        const data = await response.json();

        if (data.success) {
            showUpdateToast('定时任务已删除', 'success');
            loadScheduledJobs();
        } else {
            showUpdateToast(data.error || '删除失败', 'error');
        }
    } catch (error) {
        console.error('Error deleting job:', error);
        showUpdateToast('请求失败: ' + error.message, 'error');
    }
}

/**
 * Format datetime for display
 */
function formatDateTime(dateStr) {
    if (!dateStr) return '-';

    const date = new Date(dateStr);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');

    return `${year}-${month}-${day} ${hours}:${minutes}`;
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

/**
 * Show task exists error dialog with detailed information
 */
function showTaskExistsErrorDialog(data) {
    const task = data.existing_task;
    const statusLabel = {'running': '运行中', 'paused': '已暂停'}[task.status] || task.status;

    const message = `
        <div class="alert alert-warning mb-3">
            <h5><i class="bi bi-exclamation-triangle"></i> 无法创建新任务</h5>
            <p>系统中已有任务在<strong>${statusLabel}</strong>状态</p>
        </div>
        <div class="card mb-3">
            <div class="card-body">
                <table class="table table-sm">
                    <tr><th>任务ID:</th><td><code>${task.task_id.substring(0, 8)}</code>...</td></tr>
                    <tr><th>状态:</th><td><span class="badge bg-${task.status === 'running' ? 'primary' : 'warning'}">${statusLabel}</span></td></tr>
                    <tr><th>进度:</th><td>${task.current_stock_index}/${task.total_stocks} (${task.progress}%)</td></tr>
                    <tr><th>创建时间:</th><td>${formatDateTime(task.created_at)}</td></tr>
                </table>
            </div>
        </div>
        <div class="alert alert-info mb-0">
            <strong>建议操作：</strong>前往"任务历史"页面停止现有任务，然后再创建新任务。
        </div>
    `;

    const modalHtml = `
        <div class="modal fade" id="taskExistsModal" tabindex="-1">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header bg-warning">
                        <h5 class="modal-title"><i class="bi bi-exclamation-triangle"></i> 任务冲突</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                    </div>
                    <div class="modal-body">${message}</div>
                    <div class="modal-footer">
                        <button type="button" class="btn btn-primary" data-bs-dismiss="modal">我知道了</button>
                        <button type="button" class="btn btn-outline-primary" onclick="goToTasksHistory()">前往任务历史</button>
                    </div>
                </div>
            </div>
        </div>
    `;

    $('#taskExistsModal').remove();
    $('body').append(modalHtml);
    new bootstrap.Modal(document.getElementById('taskExistsModal')).show();
}

/**
 * Navigate to tasks history page
 */
function goToTasksHistory() {
    bootstrap.Modal.getInstance(document.getElementById('taskExistsModal')).hide();
    document.getElementById('tasks-tab')?.click();
}
