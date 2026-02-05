// 板块中心 JavaScript (用于主页 Tab)

(function() {
    // 全局变量
    let allBoards = [];
    let filteredBoards = [];
    let currentSearchQuery = '';
    let isInitialized = false;

    // 指标定义
    const metrics = [
        { key: 'pe', name: '市盈率(PE)', order: 'asc' },
        { key: 'pe_ttm', name: '市盈率TTM', order: 'asc' },
        { key: 'pb', name: '市净率(PB)', order: 'asc' },
        { key: 'ps', name: '市销率(PS)', order: 'asc' },
        { key: 'ps_ttm', name: '市销率TTM', order: 'asc' },
        { key: 'total_mv_yi', name: '总市值(亿)', order: 'desc' },
        { key: 'circ_mv_yi', name: '流通市值(亿)', order: 'desc' },
        { key: 'close', name: '股价', order: 'desc' }
    ];

    // 存储每个板块的成分股数据
    const boardStocksCache = {};

    // 初始化函数
    function init() {
        if (isInitialized) return;

        // 检查元素是否存在
        if ($('#boardsContainer').length === 0) return;

        setupEventListeners();
        isInitialized = true;

        // 当 tab 切换到板块中心时加载数据
        $('button[data-bs-target="#boards-pane"]').on('shown.bs.tab', function() {
            if (allBoards.length === 0) {
                loadBoards();
            }
        });
    }

    // 加载板块数据
    function loadBoards() {
        $.get('/api/boards')
            .done(function(data) {
                allBoards = data;
                filteredBoards = data;
                updateStats();
                renderBoards();
            })
            .fail(function() {
                $('#boardsContainer').html(`
                    <div class="col-12">
                        <div class="board-empty-state">
                            <i class="bi bi-exclamation-triangle"></i>
                            <p>加载板块数据失败</p>
                        </div>
                    </div>
                `);
            });
    }

    // 更新统计信息
    function updateStats() {
        const totalBoards = allBoards.length;
        const totalStocks = allBoards.reduce((sum, b) => sum + (b.stock_count || 0), 0);
        const avgStocks = totalBoards > 0 ? (totalStocks / totalBoards).toFixed(1) : 0;

        $('#boardTotalBoards').text(totalBoards);
        $('#boardTotalStocks').text(totalStocks);
        $('#boardAvgStocks').text(avgStocks);
    }

    // 渲染板块列表
    function renderBoards() {
        const container = $('#boardsContainer');

        if (filteredBoards.length === 0) {
            container.html(`
                <div class="col-12">
                    <div class="board-empty-state">
                        <i class="bi bi-search"></i>
                        <p>没有找到匹配的板块</p>
                    </div>
                </div>
            `);
            return;
        }

        let html = '';
        filteredBoards.forEach(board => {
            html += createBoardCard(board);
        });

        container.html(html);
    }

    // 创建板块卡片 HTML
    function createBoardCard(board) {
        const icon = board.board_name.substring(0, 2);
        const stockCount = board.stock_count || 0;

        return `
            <div class="col-lg-4 col-md-6">
                <div class="board-card" data-board-code="${board.board_code}">
                    <div class="board-header">
                        <div class="board-icon">${icon}</div>
                        <div class="board-info">
                            <div class="board-name">${highlightText(board.board_name, currentSearchQuery)}</div>
                            <div class="board-code">${board.board_code}</div>
                        </div>
                        <div class="board-meta">
                            <span class="stock-count">
                                <i class="bi bi-building"></i> ${stockCount} 只成分股
                            </span>
                            <i class="bi bi-chevron-down board-expand-icon"></i>
                        </div>
                    </div>
                    <div class="board-constituents">
                        <!-- 指标选择器 -->
                        <div class="constituents-toolbar" id="toolbar-${board.board_code}" style="display:none;">
                            <div class="row align-items-center g-2">
                                <div class="col-auto">
                                    <select class="form-select form-select-sm metric-select" style="width: 150px;">
                                        <option value="">查看全部</option>
                                        ${metrics.map(m => `<option value="${m.key}">${m.name}</option>`).join('')}
                                    </select>
                                </div>
                                <div class="col-auto">
                                    <span class="text-muted small">Top 10</span>
                                </div>
                            </div>
                        </div>
                        <div class="constituents-grid" id="constituents-${board.board_code}">
                            <div class="text-center w-100 py-3">
                                <div class="spinner-border spinner-border-sm"></div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    // 搜索高亮
    function highlightText(text, query = '') {
        if (!query) return text;
        const regex = new RegExp(`(${query})`, 'gi');
        return text.replace(regex, '<span class="board-highlight">$1</span>');
    }

    // 设置事件监听
    function setupEventListeners() {
        // 搜索功能
        $('#boardSearchInput').on('input', function() {
            const query = $(this).val().trim().toLowerCase();
            currentSearchQuery = query;
            filterBoards(query);
        });

        // 板块卡片点击
        $(document).on('click', '.board-card', function(e) {
            if ($(e.target).closest('.stock-item').length > 0) return;
            if ($(e.target).closest('.constituents-toolbar').length > 0) return;

            const card = $(this);
            const boardCode = card.data('board-code');
            toggleBoard(card, boardCode);
        });

        // 股票项点击
        $(document).on('click', '.stock-item', function(e) {
            e.stopPropagation();
            const stockCode = $(this).data('stock-code');
            viewStockDetail(stockCode);
        });

        // 指标选择变化
        $(document).on('change', '.metric-select', function() {
            const container = $(this).closest('.board-constituents').find('.constituents-grid');
            const metricKey = $(this).val();
            const boardCode = $(this).closest('.board-card').data('board-code');

            if (boardStocksCache[boardCode]) {
                renderConstituents(container, boardStocksCache[boardCode], metricKey);
            }
        });

        // 全部展开
        $('#expandAllBoardsBtn').on('click', function() {
            $('.board-card').each(function() {
                const card = $(this);
                if (!card.hasClass('expanded')) {
                    const boardCode = card.data('board-code');
                    toggleBoard(card, boardCode);
                }
            });
        });

        // 全部收起
        $('#collapseAllBoardsBtn').on('click', function() {
            $('.board-card.expanded').each(function() {
                $(this).removeClass('expanded');
            });
        });
    }

    // 过滤板块
    function filterBoards(query) {
        if (!query) {
            filteredBoards = allBoards;
        } else {
            filteredBoards = allBoards.filter(board =>
                board.board_name.toLowerCase().includes(query) ||
                board.board_code.toLowerCase().includes(query)
            );
        }
        renderBoards();
    }

    // 展开/收起板块
    function toggleBoard(card, boardCode) {
        const isExpanded = card.hasClass('expanded');

        if (isExpanded) {
            card.removeClass('expanded');
        } else {
            card.addClass('expanded');
            loadConstituents(boardCode);
        }
    }

    // 加载成分股
    function loadConstituents(boardCode) {
        const container = $(`#constituents-${boardCode}`);

        // 如果已经加载过，不再重复加载
        if (container.data('loaded')) {
            return;
        }

        // 显示加载状态
        container.html('<div class="text-center py-3"><div class="spinner-border spinner-border-sm text-primary"></div></div>');

        $.get(`/api/boards/${boardCode}/constituents`)
            .done(function(data) {
                // 标记为已加载
                container.data('loaded', true);

                // 检查数据有效性
                if (!Array.isArray(data)) {
                    console.error('[板块中心] API返回数据格式错误，期望数组，实际:', typeof data, data);
                    container.html('<div class="text-center text-danger p-3">数据格式错误</div>');
                    return;
                }

                if (data.length === 0) {
                    container.html('<div class="text-center text-muted p-3">暂无成分股数据</div>');
                    return;
                }

                // 缓存数据并渲染
                boardStocksCache[boardCode] = data;
                renderConstituents(container, data, '');
            })
            .fail(function(xhr, status, error) {
                console.error(`[板块中心] 加载板块 ${boardCode} 成分股失败:`, status, error);
                container.html('<div class="text-center text-danger p-3">加载失败，请稍后重试</div>');
            });
    }

    // 渲染成分股列表
    function renderConstituents(container, stocks, metricKey) {
        if (!stocks || stocks.length === 0) {
            container.html('<div class="text-center text-muted p-3">暂无成分股数据</div>');
            return;
        }

        // 如果选择了指标，进行排序和筛选
        let displayStocks = stocks;
        if (metricKey) {
            const metric = metrics.find(m => m.key === metricKey);
            if (metric) {
                // 过滤出有该指标数据的股票
                const validStocks = stocks.filter(s => s[metricKey] !== null && s[metricKey] !== undefined);

                // 排序
                validStocks.sort((a, b) => {
                    const aVal = a[metricKey];
                    const bVal = b[metricKey];
                    return metric.order === 'asc' ? aVal - bVal : bVal - aVal;
                });

                // 取前10
                displayStocks = validStocks.slice(0, 10);
            }
        }

        let html = '';
        displayStocks.forEach((stock, index) => {
            const rankBadge = metricKey ? `<span class="rank-badge rank-${index < 3 ? 'top' : 'normal'}">${index + 1}</span>` : '';
            const metricInfo = metricKey && stock[metricKey] !== null ?
                `<div class="stock-metric">${formatMetric(stock[metricKey], metricKey)}</div>` : '';

            html += `
                <div class="stock-item" data-stock-code="${stock.stock_code}" title="${stock.stock_name}">
                    ${rankBadge}
                    <div class="stock-code">${stock.stock_code}</div>
                    <div class="stock-name">${stock.stock_name}</div>
                    ${metricInfo}
                </div>
            `;
        });
        container.html(html);

        // 显示/隐藏工具栏
        const toolbar = container.siblings('.constituents-toolbar');
        if (toolbar.length > 0) {
            toolbar.toggle(stocks.length > 0 && stocks[0].pe !== undefined);
        }
    }

    // 格式化指标显示
    function formatMetric(value, key) {
        if (value === null || value === undefined) return '-';

        if (key.includes('mv')) {
            // 市值，保留2位小数
            return value.toFixed(2);
        } else if (key === 'close') {
            // 股价，保留2位小数
            return value.toFixed(2);
        } else {
            // PE/PB等，保留2位小数
            return value.toFixed(2);
        }
    }

    // 查看股票详情
    function viewStockDetail(stockCode) {
        // 获取股票名称
        const stockName = boardStocksCache[Object.keys(boardStocksCache)[0]]?.find(s => s.stock_code === stockCode)?.stock_name || stockCode;

        // 切换到查询tab
        const queryTab = $('button[data-bs-target="#query-pane"]');
        const boardsTab = $('button[data-bs-target="#boards-pane"]');

        if (queryTab.length && boardsTab.length) {
            // 使用Bootstrap tab API切换
            queryTab.tab('show');

            // 等待tab切换完成后设置股票并查询
            setTimeout(function() {
                // 清空现有选择
                const $select = $('#stockSelect');
                if ($select.data('select2')) {
                    $select.val(null).trigger('change');

                    // 创建新的选项
                    const newOption = new Option(`${stockName} (${stockCode})`, stockCode, true, true);
                    $select.append(newOption).trigger('change');

                    // 设置默认日期范围（1个月）
                    const dates = calculateDateRangeMain('1M');
                    $('#startDate').val(dates.start);
                    $('#endDate').val(dates.end);

                    // 自动提交查询
                    setTimeout(function() {
                        $('#queryForm').submit();
                    }, 300);
                }
            }, 200);
        }
    }

    // 辅助函数：计算日期范围（从main.js复用）
    function calculateDateRangeMain(range) {
        const today = new Date();
        const endDate = today.toISOString().split('T')[0];
        let startDate = endDate;

        switch (range) {
            case '1M':
                today.setMonth(today.getMonth() - 1);
                startDate = today.toISOString().split('T')[0];
                break;
            case '3M':
                today.setMonth(today.getMonth() - 3);
                startDate = today.toISOString().split('T')[0];
                break;
            case '6M':
                today.setMonth(today.getMonth() - 6);
                startDate = today.toISOString().split('T')[0];
                break;
            case '1Y':
                today.setFullYear(today.getFullYear() - 1);
                startDate = today.toISOString().split('T')[0];
                break;
            case 'YTD':
                startDate = today.getFullYear() + '-01-01';
                break;
            case 'ALL':
                startDate = '2020-01-01';
                break;
        }

        return { start: startDate, end: endDate };
    }

    // 显示提示消息
    function showToast(type, message) {
        // 使用现有的 toast 系统
        if (typeof window.showToast === 'function') {
            window.showToast(type, message);
        } else {
            alert(message);
        }
    }

    // 页面加载完成后初始化
    $(document).ready(function() {
        init();
    });
})();
