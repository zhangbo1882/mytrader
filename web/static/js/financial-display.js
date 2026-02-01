/**
 * Financial Data Display Module
 * Handles financial data display with summary and full report views
 */
class FinancialDisplay {
    constructor() {
        this.stockSelect = null;
        this.currentSymbol = null;
        this.currentView = 'summary';
        this.dataTables = {};

        // 12核心指标配置
        this.summaryMetrics = [
            // 基本信息
            { key: 'end_date', label: '报告期间', icon: 'bi-calendar3', category: 'basic', format: 'period' },
            { key: 'ann_date', label: '公告日期', icon: 'bi-bell', category: 'basic', format: 'date' },

            // 利润指标
            { key: 'total_operate_revenue', label: '营业收入', icon: 'bi-graph-up-arrow', category: 'profit', format: 'currency' },
            { key: 'operate_profit', label: '营业利润', icon: 'bi-graph-up', category: 'profit', format: 'currency' },
            { key: 'net_profit', label: '净利润', icon: 'bi-currency-dollar', category: 'profit', format: 'currency' },
            { key: 'basic_eps', label: '基本每股收益', icon: 'bi-piggy-bank', category: 'profit', format: 'number', decimals: 2 },

            // 资产负债
            { key: 'total_assets', label: '总资产', icon: 'bi-bank', category: 'balance', format: 'currency' },
            { key: 'total_liability', label: '总负债', icon: 'bi-credit-card', category: 'balance', format: 'currency' },
            { key: 'total_hldr_eqy_exc_min_int', label: '股东权益', icon: 'bi-wallet2', category: 'balance', format: 'currency' },

            // 现金流
            { key: 'n_cashflow_act', label: '经营活动现金流', icon: 'bi-cash-coin', category: 'cashflow', format: 'currency' },
            { key: 'free_cashflow', label: '自由现金流', icon: 'bi-cash-stack', category: 'cashflow', format: 'currency' },
            { key: 'sales_cash', label: '销售收现', icon: 'bi-cart-check', category: 'cashflow', format: 'currency' }
        ];

        this.init();
    }

    init() {
        this.initStockSelector();
        this.initEventListeners();
    }

    initStockSelector() {
        // 初始化股票选择下拉框
        this.stockSelect = $('#financialStockSelect').select2({
            placeholder: '输入股票代码或名称搜索',
            ajax: {
                url: '/api/stock/search',
                delay: 250,
                processResults: function (data) {
                    return {
                        results: data.map(item => ({
                            id: item.code,
                            text: `${item.code} - ${item.name}`
                        }))
                    };
                }
            },
            minimumInputLength: 1
        });
    }

    initEventListeners() {
        const self = this;

        // 表单提交
        $('#financialForm').on('submit', function(e) {
            e.preventDefault();
            self.loadFinancialData();
        });

        // 视图模式切换
        $('#financialViewMode').on('change', function() {
            self.currentView = $(this).val();
            if (self.currentSymbol) {
                self.loadFinancialData();
            }
        });
    }

    async loadFinancialData() {
        const symbol = this.stockSelect.val();
        if (!symbol) {
            this.showMessage('请选择股票', 'warning');
            return;
        }

        this.currentSymbol = symbol;
        this.showLoading(true);
        this.hideAllViews();

        try {
            // 先检查是否有数据
            const checkResponse = await fetch(`/api/financial/check/${symbol}`);
            const checkData = await checkResponse.json();

            if (!checkData.has_data) {
                this.showNoData();
                return;
            }

            // 根据视图模式加载数据
            if (this.currentView === 'summary') {
                await this.loadSummaryView(symbol);
            } else {
                await this.loadFullView(symbol);
            }
        } catch (error) {
            console.error('Failed to load financial data:', error);
            this.showMessage('加载财务数据失败: ' + error.message, 'danger');
        } finally {
            this.showLoading(false);
        }
    }

    async loadSummaryView(symbol) {
        try {
            const response = await fetch(`/api/financial/summary/${symbol}`);
            const result = await response.json();

            if (!result.success) {
                this.showNoData();
                return;
            }

            this.renderSummary(result.summary);
            this.showViewById('financialSummaryView');
        } catch (error) {
            console.error('Failed to load summary:', error);
            throw error;
        }
    }

    async loadFullView(symbol) {
        try {
            const response = await fetch(`/api/financial/full/${symbol}`);
            const result = await response.json();

            if (!result.success) {
                this.showNoData();
                return;
            }

            this.renderFullTables(result.data);
            this.showViewById('financialFullView');
        } catch (error) {
            console.error('Failed to load full report:', error);
            throw error;
        }
    }

    renderSummary(summary) {
        const grid = $('#financialSummaryGrid');
        grid.empty();

        // 定义4种渐变色
        const gradients = [
            'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',  // 紫色
            'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)',  // 粉色
            'linear-gradient(135deg, #4facfe 0%, #00f2fe 100%)',  // 蓝色
            'linear-gradient(135deg, #43e97b 0%, #38f9d7 100%)'   // 绿色
        ];

        this.summaryMetrics.forEach((metric, index) => {
            const value = summary[metric.key];
            const formattedValue = this.formatValue(value, metric.format, metric.decimals);
            const gradient = gradients[index % gradients.length];

            const card = `
                <div class="col-12 col-md-6 col-lg-3">
                    <div class="metric-card">
                        <div class="metric-icon" style="background: ${gradient}">
                            <i class="bi ${metric.icon}"></i>
                        </div>
                        <div class="metric-content">
                            <div class="metric-label">${metric.label}</div>
                            <div class="metric-value">${formattedValue}</div>
                        </div>
                    </div>
                </div>
            `;
            grid.append(card);
        });
    }

    renderFullTables(data) {
        // 渲染利润表
        this.renderDataTable('incomeTable', data.income || [], [
            { data: 'end_date', render: (data) => this.formatPeriod(data) },
            { data: 'ann_date', render: (data) => this.formatDate(data) },
            { data: 'total_operate_revenue', render: (data) => this.formatCurrency(data) },
            { data: 'operate_profit', render: (data) => this.formatCurrency(data) },
            { data: 'net_profit', render: (data) => this.formatCurrency(data) },
            { data: 'basic_eps', render: (data) => this.formatNumber(data, 2) },
            { data: 'n_income_attr_p', render: (data) => this.formatCurrency(data) }
        ]);

        // 渲染资产负债表
        this.renderDataTable('balanceTable', data.balancesheet || [], [
            { data: 'end_date', render: (data) => this.formatPeriod(data) },
            { data: 'ann_date', render: (data) => this.formatDate(data) },
            { data: 'total_assets', render: (data) => this.formatCurrency(data) },
            { data: 'total_liability', render: (data) => this.formatCurrency(data) },
            { data: 'total_hldr_eqy_exc_min_int', render: (data) => this.formatCurrency(data) },
            { data: 'total_share', render: (data) => this.formatNumber(data, 0) },
            { data: 'cash_equivalents', render: (data) => this.formatCurrency(data) }
        ]);

        // 渲染现金流量表
        this.renderDataTable('cashflowTable', data.cashflow || [], [
            { data: 'end_date', render: (data) => this.formatPeriod(data) },
            { data: 'ann_date', render: (data) => this.formatDate(data) },
            { data: 'n_cashflow_act', render: (data) => this.formatCurrency(data) },
            { data: 'free_cashflow', render: (data) => this.formatCurrency(data) },
            { data: 'sales_cash', render: (data) => this.formatCurrency(data) },
            { data: 'pay_staff_cash', render: (data) => this.formatCurrency(data) },
            { data: 'pay taxes', render: (data) => this.formatCurrency(data) }
        ]);
    }

    renderDataTable(tableId, data, columns) {
        const $table = $(`#${tableId}`);

        // 销毁已存在的DataTables实例
        if ($.fn.DataTable.isDataTable(`#${tableId}`)) {
            $table.DataTable().destroy();
        }

        $table.DataTable({
            data: data,
            columns: columns,
            pageLength: 10,
            lengthMenu: [[5, 10, 25, -1], [5, 10, 25, '全部']],
            language: {
                search: '搜索:',
                lengthMenu: '显示 _MENU_ 条记录',
                info: '显示第 _START_ 至 _END_ 条记录，共 _TOTAL_ 条',
                paginate: {
                    first: '首页',
                    previous: '上一页',
                    next: '下一页',
                    last: '末页'
                },
                emptyTable: '暂无数据',
                zeroRecords: '没有找到匹配的记录'
            },
            order: [[0, 'desc']]
        });
    }

    // 格式化工具方法
    formatCurrency(value) {
        if (value === null || value === undefined || isNaN(value)) {
            return '-';
        }

        const num = parseFloat(value);
        if (Math.abs(num) >= 100000000) {
            return (num / 100000000).toFixed(2) + '亿';
        } else if (Math.abs(num) >= 10000) {
            return (num / 10000).toFixed(2) + '万';
        } else {
            return num.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }
    }

    formatNumber(value, decimals = 2) {
        if (value === null || value === undefined || isNaN(value)) {
            return '-';
        }
        return parseFloat(value).toFixed(decimals);
    }

    formatDate(value) {
        if (!value) return '-';
        const str = value.toString();
        if (str.length === 8) {
            return `${str.substring(0, 4)}-${str.substring(4, 6)}-${str.substring(6, 8)}`;
        }
        return value;
    }

    formatPeriod(value) {
        if (!value) return '-';
        const str = value.toString();
        if (str.length === 8) {
            return `${str.substring(0, 4)}年${str.substring(4, 6)}月`;
        }
        return value;
    }

    formatValue(value, formatType, decimals = 2) {
        switch (formatType) {
            case 'currency':
                return this.formatCurrency(value);
            case 'number':
                return this.formatNumber(value, decimals);
            case 'date':
                return this.formatDate(value);
            case 'period':
                return this.formatPeriod(value);
            default:
                return value !== null && value !== undefined ? value : '-';
        }
    }

    // UI控制方法
    showLoading(show) {
        if (show) {
            $('#financialLoading').show();
        } else {
            $('#financialLoading').hide();
        }
    }

    showViewById(viewId) {
        this.hideAllViews();
        $(`#${viewId}`).show();
    }

    hideAllViews() {
        $('#financialSummaryView').hide();
        $('#financialFullView').hide();
        $('#financialNoData').hide();
    }

    showNoData() {
        this.hideAllViews();
        $('#financialNoData').show();
    }

    showMessage(message, type = 'info') {
        // 使用Toast显示消息
        const toastContainer = $('#toastContainer');
        const toastId = 'toast-' + Date.now();
        const toastHtml = `
            <div id="${toastId}" class="toast align-items-center text-white bg-${type} border-0" role="alert" aria-live="assertive" aria-atomic="true">
                <div class="d-flex">
                    <div class="toast-body">
                        ${message}
                    </div>
                    <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>
                </div>
            </div>
        `;
        toastContainer.append(toastHtml);
        const toastElement = document.getElementById(toastId);
        const toast = new bootstrap.Toast(toastElement, { delay: 3000 });
        toast.show();
        $(toastElement).on('hidden.bs.toast', function() {
            $(this).remove();
        });
    }
}

// 初始化财务数据展示模块
$(document).ready(function() {
    if ($('#financial-pane').length > 0) {
        window.financialDisplay = new FinancialDisplay();
    }
});
