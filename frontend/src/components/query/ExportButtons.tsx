import { Button, Space, message, Modal } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { stockService } from '@/services';
import { useQueryStore } from '@/stores';
import type { ExportProps } from 'antd/es/button';

interface ExportButtonsProps {
  disabled?: boolean;
  style?: React.CSSProperties;
}

export function ExportButtons({ disabled, style }: ExportButtonsProps) {
  const { symbols, dateRange, priceType } = useQueryStore();

  const handleExport = async (format: 'csv' | 'excel') => {
    if (symbols.length === 0) {
      message.warning('请先选择股票');
      return;
    }

    if (!dateRange.start || !dateRange.end) {
      message.warning('请先选择日期范围');
      return;
    }

    try {
      const params = {
        symbols: symbols.map((s) => s.code),
        startDate: dateRange.start,
        endDate: dateRange.end,
        priceType,
      };

      let blob: Blob;
      let filename: string;

      if (format === 'csv') {
        blob = await stockService.exportCSV(params);
        filename = `stock_data_${dateRange.start}_${dateRange.end}.csv`;
      } else {
        blob = await stockService.exportExcel(params);
        filename = `stock_data_${dateRange.start}_${dateRange.end}.xlsx`;
      }

      // 创建下载链接
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', filename);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);

      message.success(`成功导出${format === 'csv' ? 'CSV' : 'Excel'}文件`);
    } catch (error) {
      message.error(`导出失败：${error instanceof Error ? error.message : '未知错误'}`);
    }
  };

  const handleClick = (format: 'csv' | 'excel') => {
    Modal.confirm({
      title: '确认导出',
      content: `确定要导出 ${symbols.length} 只股票的数据吗？`,
      okText: '确定',
      cancelText: '取消',
      onOk: () => handleExport(format),
    });
  };

  return (
    <Space style={style}>
      <Button
        icon={<DownloadOutlined aria-hidden="true" />}
        onClick={() => handleClick('csv')}
        disabled={disabled}
      >
        导出CSV
      </Button>
      <Button
        icon={<DownloadOutlined aria-hidden="true" />}
        onClick={() => handleClick('excel')}
        disabled={disabled}
      >
        导出Excel
      </Button>
    </Space>
  );
}
