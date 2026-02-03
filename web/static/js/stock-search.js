/**
 * Stock Search Component - Select2 with AJAX autocomplete
 * 支持股票和指数搜索
 */

// 检测资产类型
function detectAssetType(code) {
    // 指数模式: 000001.SH, 399001.SZ (上证指数、深证成指等)
    // 优先检查是否包含交易所后缀（指数使用完整 ts_code 格式）
    if (code && code.match(/\.(SH|SZ)$/i)) {
        return 'index';
    }
    // 兼容不带后缀的指数代码格式
    if (code && code.match(/^(000\d{3}|399\d{3}|000300|000905|000906|000903)$/)) {
        return 'index';
    }
    return 'stock';
}

$(document).ready(function() {
    $('#stockSelect').select2({
        ajax: {
            url: '/api/stock/search',
            dataType: 'json',
            delay: 250,
            data: function(params) {
                return {
                    q: params.term,
                    limit: 20,
                    type: 'all'  // 搜索股票和指数
                };
            },
            processResults: function(data) {
                return {
                    results: data.map(function(item) {
                        return {
                            id: item.code,
                            text: item.name + ' (' + item.code + ')',
                            type: item.type || detectAssetType(item.code)
                        };
                    })
                };
            },
            cache: true
        },
        placeholder: '输入股票代码或名称搜索...',
        minimumInputLength: 1,
        templateResult: function(result) {
            if (!result.id) return result.text;
            // 根据类型显示不同图标
            var icon = result.type === 'index' ? '📊' : '📈';
            return $('<span>' + icon + ' ' + result.text + '</span>');
        },
        templateSelection: function(selection) {
            if (!selection.id) return selection.text;
            var icon = selection.type === 'index' ? '📊' : '📈';
            return $('<span>' + icon + ' ' + selection.text + '</span>');
        },
        language: {
            inputTooShort: function() {
                return '请输入至少1个字符';
            },
            searching: function() {
                return '搜索中...';
            },
            noResults: function() {
                return '未找到匹配的股票或指数';
            }
        },
        theme: 'bootstrap-5'
    });
});
