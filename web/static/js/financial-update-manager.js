/**
 * Financial Update Manager - Financial data specific updates
 *
 * This module handles financial data update functionality, providing:
 * - Task type display names for financial updates
 * - Utility functions for financial-specific UI behavior
 * - Integration with stock-update-manager.js for shared polling logic
 */

// Task type display names
const FinancialTaskTypeNames = {
    'update_stock_prices': '股票价格更新',
    'update_financial_reports': '财务报表更新',
    'update_industry_classification': '申万行业分类更新',
    'update_index_data': '指数数据更新'
};

/**
 * Get display name for task type
 */
function getTaskDisplayName(taskType) {
    return FinancialTaskTypeNames[taskType] || taskType;
}

/**
 * Check if a task type is financial-related
 */
function isFinancialTask(taskType) {
    return taskType === 'update_financial_reports';
}

/**
 * Get CSS class for task type badge
 */
function getTaskTypeBadgeClass(taskType) {
    if (taskType === 'update_financial_reports') {
        return 'bg-info';
    } else if (taskType === 'update_stock_prices') {
        return 'bg-success';
    } else if (taskType === 'update_industry_classification') {
        return 'bg-warning';
    } else if (taskType === 'update_index_data') {
        return 'bg-secondary';
    }
    return 'bg-secondary';
}

// Initialize on document ready
$(document).ready(function() {
    // Log that the financial update manager has been loaded
    console.log('[Financial Update Manager] Loaded');
});

// Export functions for use in other modules
if (typeof window !== 'undefined') {
    window.FinancialUpdateManager = {
        getTaskDisplayName,
        isFinancialTask,
        getTaskTypeBadgeClass
    };
}
