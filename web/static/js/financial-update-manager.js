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
    'update_financial_data': '财务数据更新',
    'update_both': '股价+财务数据更新',
    'update_all_stocks': '股价数据更新',
    'update_favorites': '股票更新'
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
    return taskType === 'update_financial_data' || taskType === 'update_both';
}

/**
 * Get CSS class for task type badge
 */
function getTaskTypeBadgeClass(taskType) {
    if (taskType === 'update_financial_data') {
        return 'bg-info';
    } else if (taskType === 'update_both') {
        return 'bg-primary';
    } else if (taskType === 'update_all_stocks' || taskType === 'update_favorites') {
        return 'bg-success';
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
