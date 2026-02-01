/**
 * Main JavaScript for Stock Query System
 */

// Stock name cache
const stockNameCache = {};

// Current query data
let currentQueryData = null;
let currentScreenResults = null;
let dataTableInstance = null;
let chartInstance = null;

// Favorites management
const FAVORITES_KEY = 'stock_favorites';
const LAST_TAB_KEY = 'last_active_tab';
let favoritesData = [];

/**
 * Save last active tab to localStorage
 */
function saveLastActiveTab(tabId) {
    localStorage.setItem(LAST_TAB_KEY, tabId);
}

/**
 * Get last active tab from localStorage
 */
function getLastActiveTab() {
    return localStorage.getItem(LAST_TAB_KEY) || '#query-pane';
}

/**
 * Activate tab by ID
 */
function activateTab(tabId) {
    const $tab = $('button[data-bs-target="' + tabId + '"]');
    if ($tab.length) {
        $tab.tab('show');
    }
}

/**
 * Load favorites from localStorage
 */
function loadFavorites() {
    const saved = localStorage.getItem(FAVORITES_KEY);
    if (saved) {
        try {
            favoritesData = JSON.parse(saved);
        } catch (e) {
            favoritesData = [];
        }
    }
    updateFavoritesBadge();
}

/**
 * Save favorites to localStorage
 */
function saveFavorites() {
    localStorage.setItem(FAVORITES_KEY, JSON.stringify(favoritesData));
    updateFavoritesBadge();
}

/**
 * Update favorites badge count
 */
function updateFavoritesBadge() {
    const badge = $('#favCountBadge');
    if (favoritesData.length > 0) {
        badge.text(favoritesData.length);
        badge.removeClass('d-none');
    } else {
        badge.addClass('d-none');
    }
}

/**
 * Add stock to favorites
 */
function addToFavorites(code, name) {
    // Check if already exists
    const exists = favoritesData.some(f => f.code === code);
    if (exists) {
        showToast('提示', '该股票已在收藏列表中', 'warning');
        return;
    }

    favoritesData.push({ code, name, addedAt: new Date().toISOString() });
    saveFavorites();
    renderFavoritesTable();
    showToast('成功', `已将 ${name}(${code}) 添加到收藏`, 'success');
}

/**
 * Remove stock from favorites
 */
function removeFromFavorites(code) {
    favoritesData = favoritesData.filter(f => f.code !== code);
    saveFavorites();
    renderFavoritesTable();
    showToast('成功', '已从收藏中移除', 'info');
}

/**
 * Check if stock is in favorites
 */
function isInFavorites(code) {
    return favoritesData.some(f => f.code === code);
}

/**
 * Render favorites table
 */
function renderFavoritesTable() {
    const tbody = $('#favoritesTable tbody');

    if (favoritesData.length === 0) {
        tbody.html('<tr><td colspan="4" class="text-center text-muted">暂无收藏，从筛选结果中添加股票</td></tr>');
        updateSelectedCount();
        return;
    }

    tbody.empty();
    favoritesData.forEach(function(fav) {
        tbody.append(`
            <tr data-code="${fav.code}">
                <td>
                    <input type="checkbox" class="form-check-input fav-checkbox" data-code="${fav.code}">
                </td>
                <td>${fav.code}</td>
                <td>${fav.name}</td>
                <td>
                    <button type="button" class="btn btn-sm btn-outline-danger btn-remove-fav" data-code="${fav.code}">
                        <i class="bi bi-trash"></i> 移除
                    </button>
                </td>
            </tr>
        `);
    });

    // Bind remove buttons
    $('.btn-remove-fav').click(function() {
        const code = $(this).data('code');
        removeFromFavorites(code);
    });

    // Bind checkbox changes
    $('.fav-checkbox').change(function() {
        updateSelectedCount();
    });

    // Reset select all checkbox
    $('#selectAllFavorites').prop('checked', false);
    updateSelectedCount();
}

/**
 * Update selected count and button state
 */
function updateSelectedCount() {
    const count = $('.fav-checkbox:checked').length;
    $('#selectedCount').text(count);
    $('#btnQuerySelectedFavorites').prop('disabled', count === 0);
}

/**
 * Query selected favorites
 */
function querySelectedFavorites() {
    const selectedCodes = [];
    $('.fav-checkbox:checked').each(function() {
        selectedCodes.push($(this).data('code'));
    });

    if (selectedCodes.length === 0) {
        showToast('提示', '请选择要查询的股票', 'warning');
        return;
    }

    const symbols = selectedCodes;

    // Fetch stock names first, then proceed
    fetchStockNames(symbols, function() {
        // Switch to query tab
        const queryTab = new bootstrap.Tab($('#query-tab')[0]);
        queryTab.show();

        // Wait for tab to complete transition, then set values
        setTimeout(function() {
            // Set default date range (3 months)
            const dates = calculateDateRange('3M');
            $('#startDate').val(dates.start);
            $('#endDate').val(dates.end);

            // For Select2 with AJAX, we need to create option elements first
            const $select = $('#stockSelect');
            if ($select.data('select2')) {
                // Clear existing selections
                $select.val(null).trigger('change');

                // Add new option elements for each stock
                symbols.forEach(function(code) {
                    const name = getStockName(code);
                    // Check if option already exists
                    if ($select.find(`option[value="${code}"]`).length === 0) {
                        // Create new option
                        const option = new Option(`${name} (${code})`, code, true, true);
                        $select.append(option);
                    } else {
                        // Option exists, just select it
                        $select.find(`option[value="${code}"]`).prop('selected', true);
                    }
                });

                // Trigger change to update Select2 UI
                $select.trigger('change');

                // Show toast with count
                showToast('提示', '已选择 ' + symbols.length + ' 只收藏股票', 'info');
            }

            // Submit query
            setTimeout(function() {
                $('#queryForm').submit();
            }, 500);
        }, 300);
    });
}

/**
 * Query all favorites (kept for compatibility but not used in UI)
 */
function queryFavorites() {
    querySelectedFavorites();
}

/**
 * Add all screen results to favorites
 */
function addAllScreenResultsToFavorites() {
    if (!currentScreenResults || currentScreenResults.length === 0) {
        showToast('提示', '没有可添加的筛选结果', 'warning');
        return;
    }

    let addedCount = 0;
    let skippedCount = 0;

    currentScreenResults.forEach(function(stock) {
        const exists = favoritesData.some(f => f.code === stock.code);
        if (!exists) {
            favoritesData.push({
                code: stock.code,
                name: stock.name,
                addedAt: new Date().toISOString()
            });
            addedCount++;
        } else {
            skippedCount++;
        }
    });

    saveFavorites();
    renderFavoritesTable();

    // Update table buttons
    $('.btn-add-fav').each(function() {
        $(this).replaceWith('<button type="button" class="btn btn-sm btn-outline-secondary" disabled><i class="bi bi-check"></i> 已收藏</button>');
    });

    if (addedCount > 0) {
        showToast('成功', `已添加 ${addedCount} 只股票到收藏${skippedCount > 0 ? `，跳过 ${skippedCount} 只已收藏的股票` : ''}`, 'success');
    } else {
        showToast('提示', '所有股票已在收藏列表中', 'info');
    }
}

/**
 * Manually add stock to favorites
 */
function addStockToFavorites() {
    const code = $('#addFavStockInput').val().trim();

    if (!code) {
        showToast('提示', '请输入股票代码', 'warning');
        return;
    }

    // Validate code format (6 digits)
    if (!/^\d{6}$/.test(code)) {
        showToast('错误', '股票代码格式不正确，请输入6位数字', 'error');
        return;
    }

    // Check if already exists
    if (isInFavorites(code)) {
        showToast('提示', '该股票已在收藏列表中', 'warning');
        $('#addFavStockInput').val('');
        return;
    }

    // Get stock name (try API first)
    $.ajax({
        url: '/api/stock/name/' + code,
        method: 'GET',
        success: function(response) {
            addToFavorites(code, response.name);
            $('#addFavStockInput').val('');
        },
        error: function() {
            // API failed, use code as name
            addToFavorites(code, code);
            $('#addFavStockInput').val('');
        }
    });
}

// Color palette for charts
const chartColors = [
    'rgb(54, 162, 235)',   // Blue
    'rgb(255, 99, 132)',   // Red
    'rgb(75, 192, 192)',   // Green
    'rgb(255, 206, 86)',   // Yellow
    'rgb(153, 102, 255)',  // Purple
    'rgb(255, 159, 64)',   // Orange
    'rgb(199, 199, 199)',  // Grey
    'rgb(83, 102, 255)',   // Indigo
    'rgb(255, 99, 255)',   // Pink
    'rgb(99, 255, 132)'    // Light Green
];

/**
 * Initialize date defaults
 */
function initDefaultDates() {
    const today = new Date();
    $('#endDate').val(today.toISOString().split('T')[0]);

    // 从API获取数据库中最早的日期
    $.ajax({
        url: '/api/stock/min-date',
        method: 'GET',
        success: function(response) {
            $('#startDate').val(response.date);
        },
        error: function() {
            // 如果API调用失败，使用默认的1个月前
            const oneMonthAgo = new Date();
            oneMonthAgo.setMonth(today.getMonth() - 1);
            $('#startDate').val(oneMonthAgo.toISOString().split('T')[0]);
        }
    });
}

/**
 * Initialize Select2 for stock selector
 */
function initStockSelector() {
    $('#stockSelect').select2({
        ajax: {
            url: '/api/stock/search',
            dataType: 'json',
            delay: 250,
            data: function(params) {
                return {
                    q: params.term
                };
            },
            processResults: function(data) {
                return {
                    results: data.stocks || []
                };
            }
        },
        minimumInputLength: 1,
        placeholder: '输入代码或名称搜索',
        language: 'zh-CN',
        width: '100%'
    });
}

/**
 * Calculate date range based on preset
 */
function calculateDateRange(range) {
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

/**
 * Format number for display
 */
function formatNumber(value) {
    if (value === null || value === undefined) {
        return '-';
    }
    if (typeof value === 'number') {
        return value.toFixed(2);
    }
    return value;
}

/**
 * Format percentage for display
 */
function formatPercent(value) {
    if (value === null || value === undefined) {
        return '-';
    }
    if (typeof value === 'number') {
        return value.toFixed(2) + '%';
    }
    return value;
}

/**
 * Format volume for display
 */
function formatVolume(value) {
    if (value === null || value === undefined) {
        return '-';
    }
    if (typeof value === 'number') {
        // Format as K (thousands) or M (millions)
        if (value >= 1000000) {
            return (value / 1000000).toFixed(2) + 'M';
        } else if (value >= 1000) {
            return (value / 1000).toFixed(2) + 'K';
        }
        return value.toFixed(0);
    }
    return value;
}

/**
 * Get stock name from API or cache
 */
function getStockName(code) {
    if (stockNameCache[code]) {
        return stockNameCache[code];
    }
    // Return code as fallback
    return code;
}

/**
 * Fetch stock names in batch
 */
function fetchStockNames(symbols, callback) {
    const uncachedSymbols = symbols.filter(s => !stockNameCache[s]);

    if (uncachedSymbols.length === 0) {
        callback();
        return;
    }

    let completed = 0;
    uncachedSymbols.forEach(function(symbol) {
        $.ajax({
            url: '/api/stock/name/' + symbol,
            method: 'GET',
            success: function(response) {
                stockNameCache[symbol] = response.name;
                completed++;
                if (completed === uncachedSymbols.length) {
                    callback();
                }
            },
            error: function() {
                // Keep using code as fallback
                completed++;
                if (completed === uncachedSymbols.length) {
                    callback();
                }
            }
        });
    });
}

/**
 * Show toast message
 */
function showToast(title, message, type = 'info') {
    const toastEl = $('#messageToast');
    const toastTitle = $('#toastTitle');
    const toastBody = $('#toastBody');

    toastTitle.text(title);
    toastBody.text(message);

    // Set color based on type
    toastEl.removeClass('text-bg-success text-bg-danger text-bg-warning text-bg-info');
    if (type === 'success') {
        toastEl.addClass('text-bg-success');
    } else if (type === 'error') {
        toastEl.addClass('text-bg-danger');
    } else if (type === 'warning') {
        toastEl.addClass('text-bg-warning');
    } else {
        toastEl.addClass('text-bg-info');
    }

    const toast = new bootstrap.Toast(toastEl);
    toast.show();
}

/**
 * Display query results
 */
function displayResults(data, symbols, source = 'query') {
    // source: 'query' | 'screen'

    // Hide loading
    $('#loadingSection').hide();

    // Update result title
    const title = source === 'screen' ? '筛选结果' : '查询结果';
    $('#resultTitle').text(title);

    // Prepare table data
    const tableData = [];
    let totalCount = 0;

    symbols.forEach(function(symbol) {
        const records = data[symbol] || [];
        totalCount += records.length;

        records.forEach(function(record) {
            tableData.push({
                code: symbol,
                name: getStockName(symbol),
                date: record.datetime,
                open: record.open,
                high: record.high,
                low: record.low,
                close: record.close,
                volume: record.volume,
                turnover: record.turnover,
                pct_chg: record.pct_chg
            });
        });
    });

    // Update record count
    $('#recordCount').text(totalCount + ' 条记录');

    // Destroy existing DataTable
    if (dataTableInstance) {
        dataTableInstance.destroy();
    }

    // Initialize DataTable
    dataTableInstance = $('#dataTable').DataTable({
        data: tableData,
        pageLength: 25,
        order: [[2, 'desc']],
        language: {
            url: '//cdn.datatables.net/plug-ins/1.13.7/i18n/zh.json'
        },
        columns: [
            {
                data: 'code',
                className: 'text-nowrap'
            },
            {
                data: 'name',
                className: 'text-nowrap'
            },
            {
                data: 'date',
                className: 'text-nowrap'
            },
            {
                data: 'open',
                render: formatNumber,
                className: 'text-end'
            },
            {
                data: 'high',
                render: formatNumber,
                className: 'text-end'
            },
            {
                data: 'low',
                render: formatNumber,
                className: 'text-end'
            },
            {
                data: 'close',
                render: formatNumber,
                className: 'text-end'
            },
            {
                data: 'volume',
                render: formatVolume,
                className: 'text-end'
            },
            {
                data: 'turnover',
                render: formatPercent,
                className: 'text-end'
            },
            {
                data: 'pct_chg',
                render: function(data, type) {
                    const val = formatPercent(data);
                    const num = parseFloat(data);
                    if (isNaN(num)) {
                        return '<span class="text-muted">' + val + '</span>';
                    } else if (num > 0) {
                        return '<span class="text-danger">+' + val + '</span>';
                    } else if (num < 0) {
                        return '<span class="text-success">' + val + '</span>';
                    } else {
                        return '<span>' + val + '</span>';
                    }
                },
                className: 'text-end'
            }
        ]
    });

    // Show results section
    $('#resultsSection').show();
    // Hide screen results section
    $('#screenResultsSection').hide();
    $('#exportCsv, #exportExcel').prop('disabled', false);

    // Show price trend chart if has data
    if (totalCount > 0) {
        drawComparisonChart(data, symbols);
    } else {
        $('#chartCard').hide();
    }
}

/**
 * Draw comparison chart
 */
function drawComparisonChart(data, symbols) {
    const ctx = document.getElementById('comparisonChart').getContext('2d');

    // Destroy existing chart
    if (chartInstance) {
        chartInstance.destroy();
    }

    // Prepare datasets
    const datasets = [];
    const allDates = new Set();

    symbols.forEach(function(symbol, index) {
        const records = data[symbol] || [];
        if (records.length === 0) return;

        const color = chartColors[index % chartColors.length];

        // Use actual close prices
        const dataPoints = records.map(function(r) {
            allDates.add(r.datetime);
            return {
                x: r.datetime,
                y: r.close
            };
        });

        datasets.push({
            label: getStockName(symbol) + ' (' + symbol + ')',
            data: dataPoints,
            borderColor: color,
            backgroundColor: color.replace('rgb', 'rgba').replace(')', ', 0.1)'),
            tension: 0.1,
            pointRadius: 0,
            pointHoverRadius: 5
        });
    });

    if (datasets.length === 0) {
        $('#chartCard').hide();
        return;
    }

    // Create chart
    chartInstance = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            aspectRatio: 2,
            interaction: {
                mode: 'index',
                intersect: false
            },
            scales: {
                x: {
                    type: 'category',
                    labels: Array.from(allDates).sort(),
                    title: {
                        display: true,
                        text: '日期'
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: '价格 (元)'
                    },
                    ticks: {
                        callback: function(value) {
                            return value.toFixed(2);
                        }
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.dataset.label + ': ' + formatNumber(context.parsed.y);
                        }
                    }
                },
                legend: {
                    position: 'top'
                }
            }
        }
    });

    $('#chartCard').show();
}

/**
 * Handle export
 */
function handleExport(format) {
    window.location.href = '/api/stock/export/' + format;
    showToast('导出中...', '文件将自动下载', 'info');
}

/**
 * Initialize the page
 */
$(document).ready(function() {
    // Initialize favorites
    loadFavorites();
    renderFavoritesTable();

    // Bind favorites buttons
    $('#btnQuerySelectedFavorites').click(querySelectedFavorites);
    $('#btnAddToFavorites').click(addStockToFavorites);
    $('#addFavStockInput').keypress(function(e) {
        if (e.which === 13) { // Enter key
            addStockToFavorites();
        }
    });

    // Bind select all checkbox
    $('#selectAllFavorites').change(function() {
        const checked = $(this).prop('checked');
        $('.fav-checkbox').prop('checked', checked).trigger('change');
    });

    // Initialize default dates
    initDefaultDates();

    // Initialize Select2 for stock search
    initStockSelector();

    // Tab切换事件处理
    $('button[data-bs-toggle="tab"]').on('show.bs.tab', function(e) {
        const targetId = $(e.target).attr('data-bs-target');
        console.log('切换到:', targetId);

        // 保存当前激活的tab
        saveLastActiveTab(targetId);

        // 根据不同的tab，隐藏相应的内容
        if (targetId === '#query-pane') {
            // 查询tab：隐藏筛选结果
            $('#screenResultsSection').hide();
        } else if (targetId === '#screen-pane') {
            // 筛选tab：隐藏查询结果
            $('#resultsSection').hide();
        } else if (targetId === '#favorites-pane') {
            // 收藏tab：隐藏所有结果区域
            $('#resultsSection').hide();
            $('#screenResultsSection').hide();
        } else if (targetId === '#update-pane' || targetId === '#tasks-pane' || targetId === '#financial-pane') {
            // 更新管理、任务历史、财务数据：隐藏所有结果区域
            $('#resultsSection').hide();
            $('#screenResultsSection').hide();
        }
    });

    $('button[data-bs-toggle="tab"]').on('shown.bs.tab', function(e) {
        const targetId = $(e.target).attr('data-bs-target');

        // 切换到查询tab时，确保Select2正确显示
        if (targetId === '#query-pane') {
            if ($('#stockSelect').data('select2')) {
                $('#stockSelect').select2('focus');
            }
        }
    });

    // Time range buttons
    $('[data-range]').click(function() {
        const range = $(this).data('range');
        const dates = calculateDateRange(range);
        $('#startDate').val(dates.start);
        $('#endDate').val(dates.end);

        // Update active state
        $('[data-range]').removeClass('active');
        $(this).addClass('active');
    });

    // Query form submission
    $('#queryForm').submit(function(e) {
        e.preventDefault();

        const symbols = $('#stockSelect').val();
        const startDate = $('#startDate').val();
        const endDate = $('#endDate').val();
        const priceType = $('#priceType').val();

        if (!symbols || symbols.length === 0) {
            showToast('错误', '请选择至少一只股票', 'error');
            return;
        }

        if (!startDate || !endDate) {
            showToast('错误', '请指定日期范围', 'error');
            return;
        }

        if (startDate > endDate) {
            showToast('错误', '开始日期不能晚于结束日期', 'error');
            return;
        }

        // Show loading
        $('#loadingSection').show();
        $('#resultsSection').hide();

        // Fetch stock names first, then query
        fetchStockNames(symbols, function() {
            // Send query request
            $.ajax({
                url: '/api/stock/query',
                method: 'POST',
                contentType: 'application/json',
                dataType: 'text',  // Get as text to handle large responses
                timeout: 60000,  // 60 seconds timeout for large queries
                data: JSON.stringify({
                    symbols: symbols,
                    start_date: startDate,
                    end_date: endDate,
                    price_type: priceType
                }),
                success: function(responseText) {
                    try {
                        // Manually parse JSON to handle large responses
                        const response = JSON.parse(responseText);
                        currentQueryData = response;
                        displayResults(response, symbols);
                        showToast('成功', '查询完成', 'success');
                    } catch (e) {
                        $('#loadingSection').hide();
                        console.error('JSON parse error:', e);
                        showToast('错误', '数据解析错误，请缩小查询范围后重试', 'error');
                    }
                },
                error: function(xhr) {
                    $('#loadingSection').hide();
                    let errorMsg = '查询失败，请稍后重试';
                    if (xhr.status === 0) {
                        errorMsg = '网络连接失败，请检查网络或缩小查询范围';
                    } else if (xhr.status === 408) {
                        errorMsg = '查询超时，请缩小查询范围后重试';
                    } else if (xhr.responseJSON && xhr.responseJSON.error) {
                        errorMsg = xhr.responseJSON.error;
                    }
                    showToast('错误', errorMsg, 'error');
                }
            });
        });
    });

    // Export buttons
    $('#exportCsv').click(function() {
        handleExport('csv');
    });

    $('#exportExcel').click(function() {
        handleExport('excel');
    });

    // Initialize screener
    initScreener();

    // 恢复上次访问的tab
    const lastTab = getLastActiveTab();
    if (lastTab && lastTab !== '#query-pane') {
        // 延迟执行，确保所有组件都已初始化
        setTimeout(function() {
            activateTab(lastTab);
        }, 100);
    }
});


// 股票筛选器功能

// 初始化筛选器
function initScreener() {
    // 绑定事件
    $('#btnScreen').click(executeScreen);
    $('#btnResetScreen').click(resetScreenForm);
}

// 执行筛选
function executeScreen() {
    const params = collectScreenParams();

    if (!validateScreenParams(params)) {
        return;
    }

    // 显示loading
    $('#screenLoading').removeClass('d-none');
    $('#btnScreen').prop('disabled', true);

    $.ajax({
        url: '/api/stock/screen',
        method: 'POST',
        contentType: 'application/json',
        data: JSON.stringify(params),
        success: function(response) {
            $('#screenLoading').addClass('d-none');
            $('#btnScreen').prop('disabled', false);

            if (response.success && response.count > 0) {
                showScreenResults(response);
            } else {
                showToast('筛选完成', '未找到符合条件的股票，请放宽筛选条件', 'warning');
            }
        },
        error: function(xhr) {
            $('#screenLoading').addClass('d-none');
            $('#btnScreen').prop('disabled', false);

            let errorMsg = '筛选失败';
            if (xhr.responseJSON && xhr.responseJSON.error) {
                errorMsg = xhr.responseJSON.error;
            }
            showToast('错误', errorMsg, 'error');
        }
    });
}

// 收集筛选参数
function collectScreenParams() {
    const params = {
        days: parseInt($('#screenDays').val()) || 5
    };

    // 只添加非空的数值参数
    const turnoverMin = parseFloat($('#turnoverMin').val());
    if (!isNaN(turnoverMin)) params.turnover_min = turnoverMin;

    const turnoverMax = parseFloat($('#turnoverMax').val());
    if (!isNaN(turnoverMax)) params.turnover_max = turnoverMax;

    const pctChgMin = parseFloat($('#pctChgMin').val());
    if (!isNaN(pctChgMin)) params.pct_chg_min = pctChgMin;

    const pctChgMax = parseFloat($('#pctChgMax').val());
    if (!isNaN(pctChgMax)) params.pct_chg_max = pctChgMax;

    const priceMin = parseFloat($('#priceMin').val());
    if (!isNaN(priceMin)) params.price_min = priceMin;

    const priceMax = parseFloat($('#priceMax').val());
    if (!isNaN(priceMax)) params.price_max = priceMax;

    const volumeMin = parseFloat($('#volumeMin').val());
    if (!isNaN(volumeMin)) params.volume_min = volumeMin;

    const volumeMax = parseFloat($('#volumeMax').val());
    if (!isNaN(volumeMax)) params.volume_max = volumeMax;

    return params;
}

// 验证筛选参数
function validateScreenParams(params) {
    if (params.days < 1 || params.days > 365) {
        showToast('错误', '筛选天数必须在1-365天之间', 'error');
        return false;
    }

    // 验证范围输入
    if (params.turnover_min !== null && params.turnover_max !== null) {
        if (params.turnover_min > params.turnover_max) {
            showToast('错误', '换手率最小值不能大于最大值', 'error');
            return false;
        }
    }

    return true;
}

// 显示筛选结果
function showScreenResults(response) {
    // 保存筛选结果
    currentScreenResults = response.symbols;

    // 更新徽章显示
    $('#screenResultBadge').text(response.count + ' 只');
    $('#screenResultBadge').removeClass('d-none');

    // 隐藏查询结果区域（如果存在）
    $('#resultsSection').hide();

    // 创建或更新筛选结果区域
    let screenResultsSection = $('#screenResultsSection');
    if (screenResultsSection.length === 0) {
        // 在筛选tab后插入筛选结果区域
        const html = `
            <div id="screenResultsSection">
                <div class="card mb-4">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <h5 class="mb-0"><i class="bi bi-funnel-fill"></i> 筛选结果</h5>
                        <div>
                            <span class="badge bg-primary me-2" id="screenResultCountBadge">${response.count} 只股票</span>
                            <button type="button" class="btn btn-outline-warning btn-sm" id="btnAddAllToFavorites">
                                <i class="bi bi-star-fill"></i> 全部收藏
                            </button>
                        </div>
                    </div>
                    <div class="card-body">
                        <div class="table-responsive">
                            <table id="screenResultTable" class="table table-striped table-hover">
                                <thead>
                                    <tr>
                                        <th>代码</th>
                                        <th>名称</th>
                                        <th>最新价</th>
                                        <th>平均换手率</th>
                                        <th>平均涨跌幅</th>
                                        <th>操作</th>
                                    </tr>
                                </thead>
                                <tbody></tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        `;
        $('#mainTabContent').after(html);

        // 重新绑定按钮事件
        $('#btnAddAllToFavorites').click(addAllScreenResultsToFavorites);
    } else {
        // 更新现有区域
        $('#screenResultCountBadge').text(response.count + ' 只股票');
        screenResultsSection.show();
    }

    // 填充结果表格（显示所有结果）
    const tbody = $('#screenResultTable tbody');
    tbody.empty();

    response.symbols.forEach(function(stock) {
        const isFav = isInFavorites(stock.code);
        const btnHtml = isFav
            ? `<button type="button" class="btn btn-sm btn-outline-secondary" disabled><i class="bi bi-check"></i> 已收藏</button>`
            : `<button type="button" class="btn btn-sm btn-warning btn-add-fav" data-code="${stock.code}" data-name="${stock.name}"><i class="bi bi-star-fill"></i> 收藏</button>`;

        tbody.append(`
            <tr>
                <td>${stock.code}</td>
                <td>${stock.name}</td>
                <td>${stock.latest_close}</td>
                <td>${stock.avg_turnover}%</td>
                <td>${stock.avg_pct_chg}%</td>
                <td>${btnHtml}</td>
            </tr>
        `);
    });

    // Bind add to favorites buttons
    $('.btn-add-fav').click(function() {
        const code = $(this).data('code');
        const name = $(this).data('name');
        addToFavorites(code, name);
        // Refresh the table row button state
        $(this).replaceWith('<button type="button" class="btn btn-sm btn-outline-secondary" disabled><i class="bi bi-check"></i> 已收藏</button>');
    });

    // 初始化DataTable（如果还未初始化）
    if (!$.fn.DataTable.isDataTable('#screenResultTable')) {
        $('#screenResultTable').DataTable({
            pageLength: 25,
            order: [[0, 'asc']],
            language: {
                url: '//cdn.datatables.net/plug-ins/1.13.7/i18n/zh.json'
            }
        });
    }

    showToast('筛选完成', `找到 ${response.count} 只符合条件的股票`, 'success');
}

// 重置筛选表单
function resetScreenForm() {
    $('#screenForm')[0].reset();
    $('#screenResultBadge').addClass('d-none');
}
