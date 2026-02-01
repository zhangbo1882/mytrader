/**
 * Stock Search Component - Select2 with AJAX autocomplete
 */

$(document).ready(function() {
    $('#stockSelect').select2({
        ajax: {
            url: '/api/stock/search',
            dataType: 'json',
            delay: 250,
            data: function(params) {
                return {
                    q: params.term,
                    limit: 20
                };
            },
            processResults: function(data) {
                return {
                    results: data.map(function(item) {
                        return {
                            id: item.code,
                            text: item.name + ' (' + item.code + ')'
                        };
                    })
                };
            },
            cache: true
        },
        placeholder: '输入股票代码或名称搜索...',
        minimumInputLength: 1,
        language: {
            inputTooShort: function() {
                return '请输入至少1个字符';
            },
            searching: function() {
                return '搜索中...';
            },
            noResults: function() {
                return '未找到匹配的股票';
            }
        },
        theme: 'bootstrap-5'
    });
});
