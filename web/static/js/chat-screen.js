/**
 * ChatGPT风格的股票筛选对话界面
 */

(function() {
    'use strict';

    // 状态管理
    const state = {
        isOpen: false,
        messages: [],
        isLoading: false,
        conversationHistory: []
    };

    // DOM元素
    let elements = {};

    /**
     * 初始化聊天界面
     */
    function init() {
        // 等待DOM加载
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', setup);
        } else {
            setup();
        }
    }

    /**
     * 设置界面和事件监听
     */
    function setup() {
        // 检查使用哪种模式
        const inlineContainer = document.getElementById('chatInlineContainer');
        state.useInlineMode = !!inlineContainer;

        // 检查是否已有聊天界面，没有则创建
        const existingOverlay = document.getElementById('chatScreenOverlay');
        const existingInline = document.getElementById('chatInlineContainer');

        if (!existingOverlay && !existingInline.querySelector('.chat-container')) {
            createChatInterface();
        }

        // 缓存DOM元素
        cacheElements();

        // 绑定事件
        bindEvents();

        // 如果是inline模式，不需要打开按钮
        if (!state.useInlineMode) {
            const openBtn = document.getElementById('btnAIScreen');
            if (openBtn) {
                openBtn.addEventListener('click', open);
            }
        }
    }

    /**
     * 创建聊天界面DOM
     */
    function createChatInterface() {
        // 直接创建DOM，不加载外部模板
        createChatInterfaceDirectly();
    }

    /**
     * 直接创建聊天界面（备用方法）
     */
    function createChatInterfaceDirectly() {
        const container = state.useInlineMode
            ? document.getElementById('chatInlineContainer')
            : document.body;

        if (!container) return;

        // 创建容器（如果是overlay模式）
        const wrapper = state.useInlineMode ? container : document.createElement('div');
        if (!state.useInlineMode) {
            wrapper.id = 'chatScreenOverlay';
            wrapper.className = 'chat-overlay';
        }

        wrapper.innerHTML = `
            <div class="chat-container">
                <div class="chat-header">
                    <div class="chat-header-left">
                        <i class="bi bi-robot"></i>
                        <span>AI股票筛选助手</span>
                    </div>
                    <div class="chat-header-right">
                        <button class="btn-icon" id="chatClearBtn" title="清空对话">
                            <i class="bi bi-trash"></i>
                        </button>
                        ${!state.useInlineMode ? `
                        <button class="btn-icon" id="chatCloseBtn" title="关闭">
                            <i class="bi bi-x-lg"></i>
                        </button>
                        ` : ''}
                    </div>
                </div>
                <div class="chat-messages" id="chatMessages">
                    <div class="message assistant">
                        <div class="message-avatar"><i class="bi bi-robot"></i></div>
                        <div class="message-content">
                            <div class="message-text">
                                <p>你好！我是AI股票筛选助手。我可以帮你筛选股票，比如：</p>
                                <ul class="suggestion-list">
                                    <li><span class="suggestion-pill">查找最近5天涨幅超过5%的股票</span></li>
                                    <li><span class="suggestion-pill">价格低于20元且换手率大于3%的股票</span></li>
                                    <li><span class="suggestion-pill">最近10天成交量放大的股票</span></li>
                                    <li><span class="suggestion-pill">低价高换手率的股票</span></li>
                                </ul>
                                <p class="text-muted small mt-2">点击上方建议或直接输入你的筛选条件</p>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="chat-input-container">
                    <div class="chat-input-wrapper">
                        <textarea id="chatInput" placeholder="输入筛选条件，如：查找最近5天涨幅超过5%的股票" rows="1"></textarea>
                        <button class="send-button" id="chatSendBtn" disabled>
                            <i class="bi bi-send-fill"></i>
                        </button>
                    </div>
                    <div class="chat-footer">
                        <small class="text-muted">
                            <i class="bi bi-info-circle"></i> AI可能会出错，请仔细核对筛选结果
                        </small>
                    </div>
                </div>
            </div>
        `;

        if (!state.useInlineMode) {
            document.body.appendChild(wrapper);
        }
    }

    /**
     * 缓存DOM元素
     */
    function cacheElements() {
        elements = {
            overlay: document.getElementById('chatScreenOverlay'),
            messages: document.getElementById('chatMessages'),
            input: document.getElementById('chatInput'),
            sendBtn: document.getElementById('chatSendBtn'),
            closeBtn: document.getElementById('chatCloseBtn'),
            clearBtn: document.getElementById('chatClearBtn'),
            container: state.useInlineMode ? document.getElementById('chatInlineContainer') : null
        };
    }

    /**
     * 绑定事件监听
     */
    function bindEvents() {
        if (!elements.messages && !elements.overlay) return;

        // 关闭按钮（仅overlay模式）
        if (elements.closeBtn && !state.useInlineMode) {
            elements.closeBtn.addEventListener('click', close);
        }

        // 清空按钮
        if (elements.clearBtn) {
            elements.clearBtn.addEventListener('click', clearChat);
        }

        // 发送按钮
        if (elements.sendBtn) {
            elements.sendBtn.addEventListener('click', sendMessage);
        }

        // 输入框事件
        if (elements.input) {
            // 自动调整高度
            elements.input.addEventListener('input', function() {
                this.style.height = 'auto';
                this.style.height = Math.min(this.scrollHeight, 120) + 'px';
                updateSendButton();
            });

            // 键盘事件
            elements.input.addEventListener('keydown', function(e) {
                if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault();
                    sendMessage();
                }
            });
        }

        // 建议pill点击
        const pills = document.querySelectorAll('.suggestion-pill');
        pills.forEach(pill => {
            pill.addEventListener('click', function() {
                if (elements.input) {
                    elements.input.value = this.textContent;
                    updateSendButton();
                    elements.input.focus();
                }
            });
        });

        // 点击遮罩关闭（仅overlay模式）
        if (elements.overlay && !state.useInlineMode) {
            elements.overlay.addEventListener('click', function(e) {
                if (e.target === elements.overlay) {
                    close();
                }
            });
        }
    }

    /**
     * 打开聊天界面（仅overlay模式）
     */
    function open() {
        if (state.useInlineMode || !elements.overlay) return;
        state.isOpen = true;
        elements.overlay.classList.add('show');
        if (elements.input) {
            setTimeout(() => elements.input.focus(), 300);
        }
    }

    /**
     * 关闭聊天界面（仅overlay模式）
     */
    function close() {
        if (state.useInlineMode || !elements.overlay) return;
        state.isOpen = false;
        elements.overlay.classList.remove('show');
    }

    /**
     * 清空对话
     */
    function clearChat() {
        state.messages = [];
        state.conversationHistory = [];
        if (elements.messages) {
            // 保留欢迎消息
            const welcomeMsg = elements.messages.querySelector('.message.assistant');
            elements.messages.innerHTML = '';
            if (welcomeMsg) {
                elements.messages.appendChild(welcomeMsg);
                // 重新绑定建议pill
                const pills = welcomeMsg.querySelectorAll('.suggestion-pill');
                pills.forEach(pill => {
                    pill.addEventListener('click', function() {
                        if (elements.input) {
                            elements.input.value = this.textContent;
                            updateSendButton();
                            elements.input.focus();
                        }
                    });
                });
            }
        }
    }

    /**
     * 更新发送按钮状态
     */
    function updateSendButton() {
        if (elements.sendBtn && elements.input) {
            elements.sendBtn.disabled = !elements.input.value.trim();
        }
    }

    /**
     * 发送消息
     */
    async function sendMessage() {
        if (!elements.input || state.isLoading) return;

        const text = elements.input.value.trim();
        if (!text) return;

        // 添加用户消息到界面
        addMessage('user', text);
        state.conversationHistory.push({ role: 'user', content: text });

        // 清空输入框
        elements.input.value = '';
        elements.input.style.height = 'auto';
        updateSendButton();

        // 显示加载状态
        showTyping();

        try {
            // 调用后端API
            const response = await fetch('/api/stock/ai-screen-chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    query: text,
                    history: state.conversationHistory
                })
            });

            if (!response.ok) {
                // Try to get error message from response body
                let errorMsg = '网络请求失败';
                try {
                    const errData = await response.json();
                    errorMsg = errData.error || errorMsg;
                } catch (e) {
                    // If parsing fails, use status text
                    errorMsg = `请求失败 (${response.status})`;
                }
                throw new Error(errorMsg);
            }

            const data = await response.json();

            // 移除加载状态
            hideTyping();

            if (data.success) {
                // 显示AI回复
                addMessage('assistant', data.response, data);
                state.conversationHistory.push({ role: 'assistant', content: data.response });

                // 如果有筛选结果，执行筛选
                if (data.should_screen && data.params) {
                    await executeScreening(data.params);
                }
            } else {
                // 显示错误
                addErrorMessage(data.error || '处理失败，请重试');
            }

        } catch (error) {
            hideTyping();
            addErrorMessage('网络错误：' + error.message);
        }
    }

    /**
     * 添加消息到界面
     */
    function addMessage(role, text, data = null) {
        if (!elements.messages) return;

        const msgDiv = document.createElement('div');
        msgDiv.className = `message ${role}`;

        const avatar = document.createElement('div');
        avatar.className = 'message-avatar';
        avatar.innerHTML = role === 'user' ? '<i class="bi bi-person-fill"></i>' : '<i class="bi bi-robot"></i>';

        const content = document.createElement('div');
        content.className = 'message-content';

        const textDiv = document.createElement('div');
        textDiv.className = 'message-text';

        // 处理文本（支持换行）
        const formattedText = text
            .replace(/\n/g, '<br>')
            .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
            .replace(/\*(.*?)\*/g, '<em>$1</em>');

        textDiv.innerHTML = formattedText;
        content.appendChild(textDiv);

        // 如果有筛选结果，添加结果卡片
        if (data && data.params) {
            const resultCard = createResultCard(data);
            content.appendChild(resultCard);
        }

        msgDiv.appendChild(avatar);
        msgDiv.appendChild(content);
        elements.messages.appendChild(msgDiv);

        // 滚动到底部
        scrollToBottom();
    }

    /**
     * 创建结果卡片
     */
    function createResultCard(data) {
        const card = document.createElement('div');
        card.className = 'screen-result-card';

        const labels = {
            'days': '天数',
            'turnover_min': '换手率最小值',
            'turnover_max': '换手率最大值',
            'pct_chg_min': '涨幅最小值',
            'pct_chg_max': '涨幅最大值',
            'price_min': '最低价格',
            'price_max': '最高价格',
            'volume_min': '最小成交量',
            'volume_max': '最大成交量'
        };

        let paramsHtml = '';
        for (const [key, value] of Object.entries(data.params)) {
            if (value !== null && value !== undefined && labels[key]) {
                paramsHtml += `
                    <span class="param-tag">
                        <i class="bi bi-check-circle"></i>
                        ${labels[key]}: <strong>${value}</strong>
                    </span>
                `;
            }
        }

        card.innerHTML = `
            <div class="screen-result-header">
                <div class="screen-result-count">
                    <i class="bi bi-funnel-fill text-primary"></i>
                    已识别筛选条件
                </div>
                <div class="screen-result-actions">
                    <button class="primary" onclick="window.chatScreen.applyScreenParams()">
                        <i class="bi bi-check-lg"></i> 执行筛选
                    </button>
                    ${!state.useInlineMode ? `
                    <button onclick="window.chatScreen.closeChat()">
                        <i class="bi bi-x-lg"></i> 取消
                    </button>
                    ` : ''}
                </div>
            </div>
            <div class="screen-result-params">
                ${paramsHtml}
            </div>
        `;

        return card;
    }

    /**
     * 显示加载状态
     */
    function showTyping() {
        if (!elements.messages) return;
        state.isLoading = true;

        const typingDiv = document.createElement('div');
        typingDiv.className = 'message assistant typing';
        typingDiv.id = 'typingIndicator';
        typingDiv.innerHTML = `
            <div class="message-avatar"><i class="bi bi-robot"></i></div>
            <div class="message-content">
                <div class="typing-indicator">
                    <span></span>
                    <span></span>
                    <span></span>
                </div>
            </div>
        `;

        elements.messages.appendChild(typingDiv);
        scrollToBottom();
    }

    /**
     * 隐藏加载状态
     */
    function hideTyping() {
        state.isLoading = false;
        const typing = document.getElementById('typingIndicator');
        if (typing) {
            typing.remove();
        }
    }

    /**
     * 添加错误消息
     */
    function addErrorMessage(text) {
        if (!elements.messages) return;

        const msgDiv = document.createElement('div');
        msgDiv.className = 'message assistant';
        msgDiv.innerHTML = `
            <div class="message-avatar"><i class="bi bi-robot"></i></div>
            <div class="message-content">
                <div class="message-error">
                    <i class="bi bi-exclamation-triangle-fill"></i>
                    <span>${escapeHtml(text)}</span>
                </div>
            </div>
        `;

        elements.messages.appendChild(msgDiv);
        scrollToBottom();
    }

    /**
     * 执行筛选
     */
    async function executeScreening(params) {
        showStatusMessage('正在筛选股票...');

        try {
            const response = await fetch('/api/stock/screen', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(params)
            });

            const data = await response.json();

            if (data.success) {
                hideStatusMessage();
                addMessage('assistant', `✅ 筛选完成！找到 **${data.count}** 只符合条件的股票。`);

                // 显示结果表格
                if (data.symbols && data.symbols.length > 0) {
                    displayResults(data.symbols);
                }
            } else {
                hideStatusMessage();
                addErrorMessage('筛选失败：' + (data.error || '未知错误'));
            }
        } catch (error) {
            hideStatusMessage();
            addErrorMessage('筛选失败：' + error.message);
        }
    }

    /**
     * 显示状态消息
     */
    function showStatusMessage(text) {
        if (!elements.messages) return;

        const msgDiv = document.createElement('div');
        msgDiv.className = 'message assistant';
        msgDiv.id = 'statusMessage';
        msgDiv.innerHTML = `
            <div class="message-avatar"><i class="bi bi-robot"></i></div>
            <div class="message-content">
                <div class="message-text">
                    <div class="d-flex align-items-center gap-2">
                        <div class="spinner-border spinner-border-sm" role="status"></div>
                        <span>${escapeHtml(text)}</span>
                    </div>
                </div>
            </div>
        `;

        elements.messages.appendChild(msgDiv);
        scrollToBottom();
    }

    /**
     * 隐藏状态消息
     */
    function hideStatusMessage() {
        const statusMsg = document.getElementById('statusMessage');
        if (statusMsg) {
            statusMsg.remove();
        }
    }

    /**
     * 显示筛选结果
     */
    function displayResults(symbols) {
        if (!elements.messages) return;

        /**
         * 获取板块对应的样式类
         */
        function getBoardClass(board) {
            switch(board) {
                case '科创板':
                    return 'bg-danger';  // 红色
                case '创业板':
                    return 'bg-warning text-dark';  // 黄色
                case '上海主板':
                    return 'bg-primary';  // 蓝色
                case '深圳主板':
                    return 'bg-info text-dark';  // 青色
                case '北交所':
                    return 'bg-secondary';  // 灰色
                default:
                    return 'bg-light text-dark';  // 浅灰色
            }
        }

        // 在对话界面中添加结果消息
        const msgDiv = document.createElement('div');
        msgDiv.className = 'message assistant';

        const avatar = document.createElement('div');
        avatar.className = 'message-avatar';
        avatar.innerHTML = '<i class="bi bi-robot"></i>';

        const content = document.createElement('div');
        content.className = 'message-content';

        // 创建结果文本
        const textDiv = document.createElement('div');
        textDiv.className = 'message-text';
        textDiv.innerHTML = `
            <p>✅ 筛选完成！找到 <strong>${symbols.length}</strong> 只符合条件的股票。</p>
        `;
        content.appendChild(textDiv);

        // 创建结果表格卡片
        const resultTableCard = document.createElement('div');
        resultTableCard.className = 'screen-result-table-card';

        // 表格头部
        let tableHtml = `
            <div class="screen-result-header">
                <div class="screen-result-count">
                    <i class="bi bi-table"></i> 筛选结果
                </div>
                <div class="screen-result-actions">
                    <button class="primary" onclick="window.chatScreen.exportResults()">
                        <i class="bi bi-download"></i> 导出CSV
                    </button>
                    <button onclick="window.chatScreen.showDetails()">
                        <i class="bi bi-list-ul"></i> 详细信息
                    </button>
                </div>
            </div>
        `;

        // 表格内容
        tableHtml += '<div class="table-responsive"><table class="table table-sm table-hover">';
        tableHtml += `
            <thead>
                <tr>
                    <th>代码</th>
                    <th>名称</th>
                    <th>板块</th>
                    <th>最新日期</th>
                    <th>最新收盘价</th>
                    <th>平均换手率(%)</th>
                    <th>平均涨跌幅(%)</th>
                </tr>
            </thead>
            <tbody>
        `;

        symbols.forEach(symbol => {
            // 板块标签样式
            const boardClass = getBoardClass(symbol.board);

            tableHtml += `
                <tr>
                    <td><strong>${symbol.code}</strong></td>
                    <td>${symbol.name}</td>
                    <td><span class="badge ${boardClass}">${symbol.board || '未知'}</span></td>
                    <td>${symbol.latest_date}</td>
                    <td>${symbol.latest_close}</td>
                    <td>${symbol.avg_turnover}</td>
                    <td class="${parseFloat(symbol.avg_pct_chg) >= 0 ? 'text-success' : 'text-danger'}">
                        ${symbol.avg_pct_chg}
                    </td>
                </tr>
            `;
        });

        tableHtml += '</tbody></table></div>';

        resultTableCard.innerHTML = tableHtml;
        content.appendChild(resultTableCard);

        msgDiv.appendChild(avatar);
        msgDiv.appendChild(content);
        elements.messages.appendChild(msgDiv);

        // 保存结果供导出使用
        state.currentResults = symbols;

        // 滚动到底部
        scrollToBottom();
    }

    /**
     * 滚动到底部
     */
    function scrollToBottom() {
        if (elements.messages) {
            elements.messages.scrollTop = elements.messages.scrollHeight;
        }
    }

    /**
     * HTML转义
     */
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    /**
     * 导出结果
     */
    function exportResults() {
        if (!state.currentResults || state.currentResults.length === 0) {
            showMessage('没有可导出的结果', 'warning');
            return;
        }

        // 生成CSV内容
        let csv = '代码,名称,最新日期,最新收盘价,平均换手率(%),平均涨跌幅(%)\n';
        state.currentResults.forEach(symbol => {
            csv += `${symbol.code},${symbol.name},${symbol.latest_date},${symbol.latest_close},${symbol.avg_turnover},${symbol.avg_pct_chg}\n`;
        });

        // 创建下载链接
        const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `股票筛选结果_${new Date().toISOString().slice(0,10)}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        showMessage('已导出CSV文件', 'success');
    }

    /**
     * 显示详细信息（跳转到查询页面）
     */
    function showDetails() {
        if (!state.currentResults || state.currentResults.length === 0) {
            showMessage('没有可显示的结果', 'warning');
            return;
        }

        // 切换到查询tab并显示结果
        const queryTab = document.getElementById('query-tab');
        if (queryTab) {
            queryTab.click();
        }

        // 显示结果
        const resultsSection = document.getElementById('resultsSection');
        if (resultsSection) {
            resultsSection.style.display = 'block';
        }

        // 填充数据到表格
        const tableBody = document.querySelector('#dataTable tbody');
        if (tableBody) {
            tableBody.innerHTML = '';
            state.currentResults.forEach(symbol => {
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
        }

        // 更新计数
        const recordCount = document.getElementById('recordCount');
        if (recordCount) {
            recordCount.textContent = `${state.currentResults.length} 条记录`;
        }
    }

    /**
     * 显示toast消息
     */
    function showMessage(message, type = 'info') {
        // 使用现有toast或创建新toast
        const toast = document.getElementById('messageToast');
        const toastBody = document.getElementById('toastBody');
        const toastTitle = document.getElementById('toastTitle');

        if (toast && toastBody) {
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

            if (toastTitle) {
                toastTitle.textContent = type === 'success' ? '成功' :
                                          type === 'error' ? '错误' :
                                          type === 'warning' ? '警告' : '提示';
            }
            toastBody.textContent = message;

            const bsToast = new bootstrap.Toast(toast);
            bsToast.show();
        } else {
            // Fallback to alert
            alert(message);
        }
    }

    // 导出公开方法（供HTML中的onclick调用）
    window.chatScreen = {
        applyScreenParams: function() {
            console.log('Apply screen params - already executed');
        },
        closeChat: function() {
            close();
        },
        exportResults: exportResults,
        showDetails: showDetails
    };

    // 初始化
    init();

})();
