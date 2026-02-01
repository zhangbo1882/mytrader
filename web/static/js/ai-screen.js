/**
 * AI Stock Screening Module
 * Handles natural language query parsing and parameter extraction
 */

(function() {
    'use strict';

    // Example queries for user reference
    const EXAMPLE_QUERIES = [
        "查找最近5天涨幅超过5%的股票",
        "价格低于20元且换手率大于3%的股票",
        "最近10天成交量放大的股票",
        "低价高换手率的股票",
        "查找连续上涨的股票"
    ];

    // State
    let aiModal = null;
    let extractedParams = null;

    /**
     * Initialize AI screening functionality
     */
    function initAIScreen() {
        // Wait for DOM to be ready
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', setupAIScreen);
        } else {
            setupAIScreen();
        }
    }

    /**
     * Setup AI screening UI and event handlers
     */
    function setupAIScreen() {
        // Check if AI button exists
        const aiButton = document.getElementById('btnAIScreen');
        if (!aiButton) {
            console.warn('AI Screening button not found');
            return;
        }

        // Initialize modal
        aiModal = new bootstrap.Modal(document.getElementById('aiScreenModal'));

        // Add click handler to AI button
        aiButton.addEventListener('click', showAIModal);

        // Add example pill click handlers
        const examplePills = document.querySelectorAll('.ai-example-pill');
        examplePills.forEach(pill => {
            pill.addEventListener('click', function() {
                // Remove active class from all pills
                examplePills.forEach(p => p.classList.remove('active'));
                // Add active class to clicked pill
                this.classList.add('active');
                // Populate textarea
                const textarea = document.getElementById('aiQueryInput');
                if (textarea) {
                    textarea.value = this.textContent;
                    // Enable submit button
                    const submitBtn = document.getElementById('aiSubmitBtn');
                    if (submitBtn) {
                        submitBtn.disabled = false;
                    }
                }
            });
        });

        // Add submit button handler
        const submitBtn = document.getElementById('aiSubmitBtn');
        if (submitBtn) {
            submitBtn.addEventListener('click', submitAIQuery);
        }

        // Add apply button handler
        const applyBtn = document.getElementById('aiApplyBtn');
        if (applyBtn) {
            applyBtn.addEventListener('click', applyAIParams);
        }

        // Add textarea input handler
        const textarea = document.getElementById('aiQueryInput');
        if (textarea) {
            textarea.addEventListener('input', function() {
                // Remove active class from all pills when typing
                examplePills.forEach(p => p.classList.remove('active'));
                // Enable/disable submit button
                if (submitBtn) {
                    submitBtn.disabled = !this.value.trim();
                }
            });
        }

        // Reset state when modal closes
        const modalElement = document.getElementById('aiScreenModal');
        if (modalElement) {
            modalElement.addEventListener('hidden.bs.modal', resetAIState);
        }
    }

    /**
     * Show AI modal
     */
    function showAIModal() {
        // Reset state
        resetAIState();

        // Show modal
        if (aiModal) {
            aiModal.show();
        }

        // Focus on textarea
        setTimeout(() => {
            const textarea = document.getElementById('aiQueryInput');
            if (textarea) {
                textarea.focus();
            }
        }, 500);
    }

    /**
     * Submit AI query for processing
     */
    function submitAIQuery() {
        const textarea = document.getElementById('aiQueryInput');
        const query = textarea ? textarea.value.trim() : '';

        if (!query) {
            showMessage('请输入筛选条件', 'warning');
            return;
        }

        // Show thinking state
        showThinking();

        // Make API call
        fetch('/api/stock/ai-screen', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ query: query })
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(err => {
                    throw new Error(err.error || '处理失败');
                });
            }
            return response.json();
        })
        .then(data => {
            hideThinking();

            if (data.success && data.params) {
                extractedParams = data.params;
                displayExtractedParams(data.params, data.query);
            } else {
                showError(data.error || '未能提取筛选参数，请尝试更具体的描述');
            }
        })
        .catch(error => {
            hideThinking();
            showError('处理失败: ' + error.message);
        });
    }

    /**
     * Display extracted parameters
     */
    function displayExtractedParams(params, originalQuery) {
        const container = document.getElementById('aiParamsContainer');
        const emptyState = document.getElementById('aiEmptyState');
        const applyBtn = document.getElementById('aiApplyBtn');

        if (!container) return;

        // Hide empty state, show container
        if (emptyState) {
            emptyState.style.display = 'none';
        }
        container.style.display = 'block';

        // Build HTML for parameters
        let html = '<div class="ai-params-title"><i class="bi bi-check-circle-fill"></i> 已提取筛选条件</div>';
        html += '<div class="ai-params-grid">';

        // Parameter labels mapping
        const paramLabels = {
            'days': '筛选天数',
            'turnover_min': '换手率最小值 (%)',
            'turnover_max': '换手率最大值 (%)',
            'pct_chg_min': '涨跌幅最小值 (%)',
            'pct_chg_max': '涨跌幅最大值 (%)',
            'price_min': '最低价格 (元)',
            'price_max': '最高价格 (元)',
            'volume_min': '最小成交量 (手)',
            'volume_max': '最大成交量 (手)'
        };

        // Display each parameter
        let hasParams = false;
        for (const [key, value] of Object.entries(params)) {
            if (value !== null && value !== undefined) {
                hasParams = true;
                html += `
                    <div class="ai-param-item">
                        <div class="ai-param-label">${paramLabels[key] || key}</div>
                        <div class="ai-param-value highlight">${value}</div>
                    </div>
                `;
            }
        }

        html += '</div>';

        if (!hasParams) {
            html += `
                <div class="ai-empty-state">
                    <i class="bi bi-question-circle"></i>
                    <p>未能提取到明确的筛选参数</p>
                    <small>请尝试更具体的描述，例如："最近5天涨幅超过5%的股票"</small>
                </div>
            `;
            // Disable apply button if no params
            if (applyBtn) {
                applyBtn.disabled = true;
            }
        } else {
            // Enable apply button
            if (applyBtn) {
                applyBtn.disabled = false;
            }
        }

        container.innerHTML = html;
    }

    /**
     * Apply extracted parameters and execute screening directly
     */
    function applyAIParams() {
        if (!extractedParams) {
            showMessage('没有可应用的参数', 'warning');
            return;
        }

        // Populate hidden form fields
        if (extractedParams.days) {
            const field = document.getElementById('screenDays');
            if (field) field.value = extractedParams.days;
        }

        if (extractedParams.turnover_min !== null) {
            const field = document.getElementById('turnoverMin');
            if (field) field.value = extractedParams.turnover_min;
        }

        if (extractedParams.turnover_max !== null) {
            const field = document.getElementById('turnoverMax');
            if (field) field.value = extractedParams.turnover_max;
        }

        if (extractedParams.pct_chg_min !== null) {
            const field = document.getElementById('pctChgMin');
            if (field) field.value = extractedParams.pct_chg_min;
        }

        if (extractedParams.pct_chg_max !== null) {
            const field = document.getElementById('pctChgMax');
            if (field) field.value = extractedParams.pct_chg_max;
        }

        if (extractedParams.price_min !== null) {
            const field = document.getElementById('priceMin');
            if (field) field.value = extractedParams.price_min;
        }

        if (extractedParams.price_max !== null) {
            const field = document.getElementById('priceMax');
            if (field) field.value = extractedParams.price_max;
        }

        if (extractedParams.volume_min !== null) {
            const field = document.getElementById('volumeMin');
            if (field) field.value = extractedParams.volume_min;
        }

        if (extractedParams.volume_max !== null) {
            const field = document.getElementById('volumeMax');
            if (field) field.value = extractedParams.volume_max;
        }

        // Close modal
        if (aiModal) {
            aiModal.hide();
        }

        // Show loading status
        showScreenStatus('正在筛选股票...');

        // Execute screening directly
        executeScreening();
    }

    /**
     * Execute the screening with current parameters
     */
    function executeScreening() {
        // Build request data from hidden fields
        const data = {
            days: parseInt(document.getElementById('screenDays').value) || 10,
            turnover_min: parseFloat(document.getElementById('turnoverMin').value) || null,
            turnover_max: parseFloat(document.getElementById('turnoverMax').value) || null,
            pct_chg_min: parseFloat(document.getElementById('pctChgMin').value) || null,
            pct_chg_max: parseFloat(document.getElementById('pctChgMax').value) || null,
            price_min: parseFloat(document.getElementById('priceMin').value) || null,
            price_max: parseFloat(document.getElementById('priceMax').value) || null,
            volume_min: parseInt(document.getElementById('volumeMin').value) || null,
            volume_max: parseInt(document.getElementById('volumeMax').value) || null,
        };

        // Make API call
        fetch('/api/stock/screen', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(err => {
                    throw new Error(err.error || '筛选失败');
                });
            }
            return response.json();
        })
        .then(data => {
            hideScreenStatus();

            if (data.success) {
                showScreenResult(`找到 ${data.count} 只股票`);
                displayScreeningResults(data.symbols);
            } else {
                showMessage(data.error || '筛选失败', 'error');
            }
        })
        .catch(error => {
            hideScreenStatus();
            showMessage('筛选失败: ' + error.message, 'error');
        });
    }

    /**
     * Display screening results
     */
    function displayScreeningResults(symbols) {
        if (!symbols || symbols.length === 0) {
            showMessage('未找到符合条件的股票', 'warning');
            return;
        }

        // Check if DataTable is initialized on #dataTable
        const $table = $('#dataTable');
        if ($.fn.DataTable.isDataTable('#dataTable')) {
            // Destroy existing DataTable to avoid conflicts
            $table.DataTable().destroy();
        }

        // Clear existing results table
        const tableBody = document.querySelector('#dataTable tbody');
        if (tableBody) {
            tableBody.innerHTML = '';
        }

        // Populate results table
        symbols.forEach(symbol => {
            const row = document.createElement('tr');
            row.innerHTML = `
                <td>${symbol.code}</td>
                <td>${symbol.name}</td>
                <td>${symbol.latest_date}</td>
                <td>-</td>
                <td>-</td>
                <td>-</td>
                <td>${symbol.latest_close}</td>
                <td>-</td>
                <td>${symbol.avg_turnover}</td>
                <td>${symbol.avg_pct_chg}</td>
            `;
            tableBody.appendChild(row);
        });

        // Show results section
        const resultsSection = document.getElementById('resultsSection');
        if (resultsSection) {
            resultsSection.style.display = 'block';
        }

        // Update result title and count
        const resultTitle = document.getElementById('resultTitle');
        const recordCount = document.getElementById('recordCount');
        if (resultTitle) resultTitle.textContent = '筛选结果';
        if (recordCount) recordCount.textContent = `${symbols.length} 条记录`;

        // Log for debugging
        console.log(`AI筛选完成: 找到 ${symbols.length} 只股票`, symbols.map(s => s.code));
    }

    /**
     * Show screening status
     */
    function showScreenStatus(message) {
        const statusDiv = document.getElementById('screenStatus');
        const statusText = document.getElementById('screenStatusText');
        const resultStatus = document.getElementById('screenResultStatus');

        if (statusDiv) {
            statusDiv.style.display = 'block';
        }
        if (statusText) {
            statusText.textContent = message;
        }
        if (resultStatus) {
            resultStatus.style.display = 'none';
        }
    }

    /**
     * Hide screening status
     */
    function hideScreenStatus() {
        const statusDiv = document.getElementById('screenStatus');
        if (statusDiv) {
            statusDiv.style.display = 'none';
        }
    }

    /**
     * Show screening result badge
     */
    function showScreenResult(message) {
        const resultStatus = document.getElementById('screenResultStatus');
        const resultBadge = document.getElementById('screenResultBadge');

        if (resultStatus) {
            resultStatus.style.display = 'block';
        }
        if (resultBadge) {
            resultBadge.textContent = message;
        }
    }

    /**
     * Show thinking/loading state
     */
    function showThinking() {
        const thinkingContainer = document.getElementById('aiThinkingContainer');
        const paramsContainer = document.getElementById('aiParamsContainer');
        const submitBtn = document.getElementById('aiSubmitBtn');

        if (thinkingContainer) {
            thinkingContainer.style.display = 'flex';
        }
        if (paramsContainer) {
            paramsContainer.style.display = 'none';
        }
        if (submitBtn) {
            submitBtn.disabled = true;
        }
    }

    /**
     * Hide thinking/loading state
     */
    function hideThinking() {
        const thinkingContainer = document.getElementById('aiThinkingContainer');
        const submitBtn = document.getElementById('aiSubmitBtn');

        if (thinkingContainer) {
            thinkingContainer.style.display = 'none';
        }
        if (submitBtn) {
            const textarea = document.getElementById('aiQueryInput');
            submitBtn.disabled = !textarea || !textarea.value.trim();
        }
    }

    /**
     * Show error message
     */
    function showError(message) {
        const container = document.getElementById('aiParamsContainer');
        const emptyState = document.getElementById('aiEmptyState');

        if (emptyState) {
            emptyState.style.display = 'none';
        }
        container.style.display = 'block';

        container.innerHTML = `
            <div class="ai-error-container">
                <div class="ai-error-title">
                    <i class="bi bi-exclamation-triangle-fill"></i>
                    处理失败
                </div>
                <div class="ai-error-message">${escapeHtml(message)}</div>
            </div>
        `;

        // Disable apply button
        const applyBtn = document.getElementById('aiApplyBtn');
        if (applyBtn) {
            applyBtn.disabled = true;
        }
    }

    /**
     * Reset AI state
     */
    function resetAIState() {
        // Reset textarea
        const textarea = document.getElementById('aiQueryInput');
        if (textarea) {
            textarea.value = '';
        }

        // Reset buttons
        const submitBtn = document.getElementById('aiSubmitBtn');
        if (submitBtn) {
            submitBtn.disabled = true;
        }

        const applyBtn = document.getElementById('aiApplyBtn');
        if (applyBtn) {
            applyBtn.disabled = true;
        }

        // Reset example pills
        const examplePills = document.querySelectorAll('.ai-example-pill');
        examplePills.forEach(p => p.classList.remove('active'));

        // Hide containers
        const thinkingContainer = document.getElementById('aiThinkingContainer');
        if (thinkingContainer) {
            thinkingContainer.style.display = 'none';
        }

        const paramsContainer = document.getElementById('aiParamsContainer');
        if (paramsContainer) {
            paramsContainer.style.display = 'none';
            paramsContainer.innerHTML = '';
        }

        const emptyState = document.getElementById('aiEmptyState');
        if (emptyState) {
            emptyState.style.display = 'block';
        }

        // Reset extracted params
        extractedParams = null;
    }

    /**
     * Show toast message
     */
    function showMessage(message, type = 'info') {
        // Use existing toast if available
        const toast = document.getElementById('messageToast');
        const toastBody = document.getElementById('toastBody');
        const toastTitle = document.getElementById('toastTitle');

        if (toast && toastBody) {
            // Set toast type
            toast.className = 'toast';
            if (type === 'success') {
                toast.classList.add('bg-success', 'text-white');
            } else if (type === 'error' || type === 'danger') {
                toast.classList.add('bg-danger', 'text-white');
            } else if (type === 'warning') {
                toast.classList.add('bg-warning', 'text-dark');
            } else {
                toast.classList.add('bg-info', 'text-white');
            }

            // Set content
            if (toastTitle) {
                toastTitle.textContent = type === 'success' ? '成功' :
                                          type === 'error' ? '错误' :
                                          type === 'warning' ? '警告' : '提示';
            }
            toastBody.textContent = message;

            // Show toast
            const bsToast = new bootstrap.Toast(toast);
            bsToast.show();
        } else {
            // Fallback to alert
            alert(message);
        }
    }

    /**
     * Escape HTML to prevent XSS
     */
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Initialize on load
    initAIScreen();

})();
