// Barrel export for services with re-exports for better tree-shaking
export { default as api } from './api';
export { stockService } from './stockService';
export { taskService } from './taskService';
export { scheduleService } from './scheduleService';
export { boardService } from './boardService';
export { financialService } from './financialService';
export { mlService } from './mlService';

// Re-export types if needed
export type { ApiError } from './api';
