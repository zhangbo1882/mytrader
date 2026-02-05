/**
 * E2E测试数据
 */

export const TEST_STOCKS = {
  valid: ['600382', '000001', '600519', '000002'],
  invalid: ['999999'],
};

export const TEST_AI_QUERIES = [
  '查找换手率大于5%的股票',
  '找出最近5天涨幅超过10%的股票',
  '筛选市盈率小于20的股票',
  '显示市净率小于1的股票',
];

export const TEST_DATE_RANGES = {
  short: '1M',
  medium: '3M',
  long: '1Y',
};

export const TEST_BOARD_CATEGORIES = [
  '金融',
  '地产',
  '科技',
  '医药',
  '消费',
  '能源',
];

export const PAGE_URLS = {
  query: '/query',
  aiScreen: '/ai-screen',
  prediction: '/prediction',
  favorites: '/favorites',
  update: '/update',
  tasks: '/tasks',
  financial: '/financial',
  boards: '/boards',
};

export const SELECTORS = {
  // Common
  menu: '[data-testid="main-menu"]',
  menuItem: (label: string) => `[data-testid="menu-item-${label}"]`,

  // Query Page
  stockSelector: '[data-testid="stock-selector"]',
  dateRangePicker: '[data-testid="date-range-picker"]',
  quickRangeBtn: (range: string) => `[data-range="${range}"]`,
  priceTypeSelect: '[data-testid="price-type-select"]',
  querySubmitBtn: '[data-testid="query-submit-btn"]',
  queryResults: '[data-testid="query-results"]',
  resultsTable: '[data-testid="results-table"]',
  priceChart: '[data-testid="price-chart"]',
  exportCsvBtn: '[data-testid="export-csv-btn"]',
  exportExcelBtn: '[data-testid="export-excel-btn"]',

  // AI Screen Page
  chatInput: '[data-testid="chat-input"]',
  sendBtn: '[data-testid="send-btn"]',
  suggestions: '[data-testid="suggestions"]',
  suggestionsTag: (index: number) => `[data-testid="suggestions"] .ant-tag:nth-child(${index})`,
  userMessage: '[data-testid="user-message"]',
  aiMessage: '[data-testid="ai-message"]',
  screenResults: '[data-testid="screen-results"]',
  addAllToFavoritesBtn: '[data-testid="add-all-to-favorites-btn"]',

  // Prediction Page
  stockCodeInput: '[data-testid="stock-code-input"]',
  startDateInput: '[data-testid="start-date-input"]',
  endDateInput: '[data-testid="end-date-input"]',
  targetSelect: '[data-testid="target-select"]',
  modelSelect: '[data-testid="model-select"]',
  trainBtn: '[data-testid="train-btn"]',
  trainingStatus: '[data-testid="training-status"]',
  modelsTab: '[data-testid="models-tab"]',
  modelsTable: '[data-testid="models-table"]',

  // Favorites Page
  favoritesTable: '[data-testid="favorites-table"]',
  addFavoriteInput: '[data-testid="add-favorite-input"]',
  addFavoriteBtn: '[data-testid="add-favorite-btn"]',
  removeFavoriteBtn: (code: string) => `[data-stock-code="${code}"] [data-testid="remove-btn"]`,
  checkbox: (code: string) => `[data-stock-code="${code}"] [data-testid="checkbox"]`,
  querySelectedBtn: '[data-testid="query-selected-btn"]',

  // Update Page
  createFavoritesTaskBtn: '[data-testid="create-favorites-task-btn"]',
  createAllTaskBtn: '[data-testid="create-all-task-btn"]',
  taskCard: '[data-testid="task-card"]',
  pauseBtn: '[data-testid="pause-btn"]',
  resumeBtn: '[data-testid="resume-btn"]',
  stopBtn: '[data-testid="stop-btn"]',
  taskStatus: '[data-testid="task-status"]',

  // Tasks Page
  tasksTable: '[data-testid="tasks-table"]',
  statusFilter: '[data-testid="status-filter"]',
  deleteTaskBtn: (id: string) => `[data-task-id="${id}"] [data-testid="delete-btn"]`,
  cleanupBtn: '[data-testid="cleanup-btn"]',

  // Financial Page
  financialStockCodeInput: '[data-testid="financial-stock-code-input"]',
  financialQueryBtn: '[data-testid="financial-query-btn"]',
  summaryCards: '[data-testid="summary-cards"]',
  metricCard: (key: string) => `[data-metric="${key}"]`,
  fullReportTab: '[data-testid="full-report-tab"]',
  reportTables: '[data-testid="report-tables"]',
  exportFinancialBtn: '[data-testid="export-financial-btn"]',

  // Boards Page
  boardSearchInput: '[data-testid="board-search-input"]',
  boardCards: '[data-testid="board-cards"]',
  boardCard: (index: number) => `[data-testid="board-card"]:nth-child(${index})`,
  constituentsModal: '[data-testid="constituents-modal"]',
  constituentsTable: '[data-testid="constituents-table"]',
  viewStockBtn: (code: string) => `[data-stock-code="${code}"] [data-testid="view-stock-btn"]`,
};

// 添加data-testid到页面元素（需要在组件中实现）
export const addTestIds = () => {
  // 这个函数用于在开发模式下添加测试ID
  if (process.env.NODE_ENV === 'test') {
    // 在测试环境中自动添加data-testid属性
  }
};
