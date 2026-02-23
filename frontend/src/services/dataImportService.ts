// frontend/src/services/dataImportService.ts
import api from './api';

export interface TableInfo {
  interval: string;
  table_name: string;
  row_count: number;
  symbol_count: number;
  date_range: {
    start: string;
    end: string;
  };
}

export interface ImportResult {
  table_name: string;
  interval: string;
  detected_interval: string;
  rows_imported: number;
  rows_total: number;
  symbols: string[];
  symbol_count: number;
  date_range: {
    start: string;
    end: string;
  };
}

export interface ValidationResult {
  valid: boolean;
  error?: string;
  missing_columns?: string[];
  rows?: number;
  columns?: string[];
  detected_interval?: string | null;
  symbols?: string[];
}

export interface SheetInfo {
  sheets: string[];
  count: number;
}

export const dataImportService = {
  /**
   * Upload and import an Excel/CSV file
   */
  upload: async (
    file: File,
    options?: {
      tableName?: string;
      interval?: string;
      sheetName?: string;  // 指定 Excel 标签页名称
    }
  ): Promise<{ success: boolean; message?: string; data?: ImportResult; error?: string }> => {
    const formData = new FormData();
    formData.append('file', file);
    if (options?.tableName) {
      formData.append('table_name', options.tableName);
    }
    if (options?.interval) {
      formData.append('interval', options.interval);
    }
    if (options?.sheetName) {
      formData.append('sheet_name', options.sheetName);
    }

    const response = await api.post<{ success: boolean; message?: string; data?: ImportResult; error?: string }>(
      '/data-import/upload',
      formData,
      {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      }
    );
    return response;
  },

  /**
   * Get Excel sheet names
   */
  getSheetNames: async (
    file: File
  ): Promise<{ success: boolean; data?: SheetInfo; error?: string }> => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await api.post<{ success: boolean; data?: SheetInfo; error?: string }>(
      '/data-import/sheets',
      formData,
      {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      }
    );
    return response;
  },

  /**
   * Validate a file before importing
   */
  validate: async (
    file: File
  ): Promise<{ success: boolean; data?: ValidationResult; error?: string }> => {
    const formData = new FormData();
    formData.append('file', file);

    const response = await api.post<{ success: boolean; data?: ValidationResult; error?: string }>(
      '/data-import/validate',
      formData,
      {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      }
    );
    return response;
  },

  /**
   * List all available interval tables
   */
  listTables: async (): Promise<{ success: boolean; data?: TableInfo[]; error?: string }> => {
    return api.get('/data-import/tables');
  },

  /**
   * Get summary for a specific interval table
   */
  getTableSummary: async (interval: string): Promise<{ success: boolean; data?: any; error?: string }> => {
    return api.get(`/data-import/tables/${interval}/summary`);
  },

  /**
   * Get supported intervals
   */
  getSupportedIntervals: async (): Promise<{
    success: boolean;
    data?: Array<{ value: string; label: string }>;
    error?: string;
  }> => {
    return api.get('/data-import/intervals');
  },
};
