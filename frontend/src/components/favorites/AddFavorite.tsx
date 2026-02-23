import { useState } from 'react';
import { Button, message, AutoComplete, Divider, Upload, Modal, Table, Alert, Space } from 'antd';
import { PlusOutlined, UploadOutlined, FileTextOutlined } from '@ant-design/icons';
import type { UploadFile } from 'antd/es/upload/interface';
import * as XLSX from 'xlsx';
import Papa from 'papaparse';
import { stockService, favoriteService } from '@/services';
import { useFavoriteStore } from '@/stores';
import type { StockImportData } from '@/services/favoriteService';

interface ParsedStock extends StockImportData {
  name?: string;
}

export function AddFavorite() {
  const [searchText, setSearchText] = useState('');
  const [options, setOptions] = useState<{ value: string; label: string | React.ReactNode; code: string; name: string }[]>(
    []
  );
  const [loading, setLoading] = useState(false);
  const { addFavorite, isInFavorites } = useFavoriteStore();
  const [importModalVisible, setImportModalVisible] = useState(false);
  const [parsedStocks, setParsedStocks] = useState<ParsedStock[]>([]);
  const [importing, setImporting] = useState(false);

  // 搜索股票
  const handleSearch = async (value: string) => {
    setSearchText(value);

    if (!value || value.length < 2) {
      setOptions([]);
      return;
    }

    setLoading(true);
    try {
      const response = await stockService.search(value);
      const stockList = response.stocks || [];

      setOptions(
        stockList.map((stock: any) => ({
          value: `${stock.code} ${stock.name}`,
          code: stock.code,
          name: stock.name,
          label: (
            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <span>
                <strong>{stock.code}</strong> {stock.name}
              </span>
              {!isInFavorites(stock.code) && (
                <Button size="small" type="primary" icon={<PlusOutlined aria-hidden="true" />}>
                  添加
                </Button>
              )}
            </div>
          ),
        }))
      );
    } catch (error) {
      console.error('Search error:', error);
    } finally {
      setLoading(false);
    }
  };

  // 选择股票
  const handleSelect = (_value: string, option: any) => {
    if (isInFavorites(option.code)) {
      message.warning('该股票已在收藏列表中');
      return;
    }

    addFavorite(option.code, option.name);
    message.success(`已添加收藏：${option.name} (${option.code})`);
    setSearchText('');
    setOptions([]);
  };

  // 手动输入添加
  const handleManualAdd = () => {
    const code = searchText.trim();
    if (!code) {
      message.warning('请输入股票代码');
      return;
    }

    if (isInFavorites(code)) {
      message.warning('该股票已在收藏列表中');
      return;
    }

    addFavorite(code, '');
    message.success(`已添加收藏：${code}`);
    setSearchText('');
  };

  // 处理文件上传
  const handleFileUpload = (file: UploadFile) => {
    console.log('File uploaded:', file);

    const fileType = file.name.substring(file.name.lastIndexOf('.')).toLowerCase();

    if (!['.xlsx', '.xls', '.csv'].includes(fileType)) {
      message.error('只支持 .xlsx, .xls, .csv 格式的文件');
      return false;
    }

    const fileToRead = file.originFileObj || (file as any);
    const reader = new FileReader();

    reader.onload = (e) => {
      const data = e.target?.result;
      if (!data) {
        console.error('No data from FileReader');
        return;
      }

      try {
        let stocks: ParsedStock[] = [];

        if (fileType === '.csv') {
          // 使用 papaparse 解析 CSV
          const parsed = Papa.parse(data as string, {
            skipEmptyLines: true,
          });

          const headers = (parsed.data[0] as string[]).map(h => h?.trim() || '');
          console.log('CSV headers:', headers);

          // 查找各列的索引
          const stockCodeIndex = headers.findIndex(h =>
            h && (h.includes('股票代码') || h.includes('证券代码') || h.includes('代码'))
          );
          const safetyIndex = headers.findIndex(h =>
            h && (h.includes('安全性') || h === '安全评级')
          );
          const fundamentalIndex = headers.findIndex(h =>
            h && (h.includes('基本面') || h === '基本面评级')
          );
          const entryPriceIndex = headers.findIndex(h =>
            h && (h.includes('进场价格') || h.includes('进场价') || h === '价格')
          );

          console.log('Column indices:', { stockCodeIndex, safetyIndex, fundamentalIndex, entryPriceIndex });

          if (stockCodeIndex >= 0) {
            stocks = (parsed.data as any[][])
              .slice(1)
              .map((row) => {
                let code = row[stockCodeIndex]?.toString().trim() || '';
                // 只去除A股后缀，保留港股 .HK 后缀
                if (/\.(SH|SZ|sh|sz)$/.test(code)) {
                  code = code.replace(/\.(SH|SZ|sh|sz)$/, '');
                }

                return {
                  code,
                  safety_rating: safetyIndex >= 0 ? row[safetyIndex]?.toString().trim() || undefined : undefined,
                  fundamental_rating: fundamentalIndex >= 0 ? row[fundamentalIndex]?.toString().trim() || undefined : undefined,
                  entry_price: entryPriceIndex >= 0 ? parseFloat(row[entryPriceIndex]) || undefined : undefined,
                };
              })
              .filter((s) => s.code && s.code.length > 0);
          } else {
            // 没有找到表头，使用第一列
            stocks = (parsed.data as any[][])
              .map((row) => {
                let code = row[0]?.toString().trim() || '';
                if (/\.(SH|SZ|sh|sz)$/.test(code)) {
                  code = code.replace(/\.(SH|SZ|sh|sz)$/, '');
                }
                return { code };
              })
              .filter((s) => s.code && s.code.length > 0);
          }
        } else {
          // 使用 xlsx 解析 Excel
          const workbook = XLSX.read(data, { type: 'binary' });
          const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
          const jsonData = XLSX.utils.sheet_to_json(firstSheet, {
            header: 1,
            defval: ''
          }) as any[][];

          if (jsonData.length === 0) {
            message.error('文件为空');
            return;
          }

          const headers = (jsonData[0] as string[]).map(h => h?.toString().trim() || '');
          console.log('Excel headers:', headers);

          // 查找各列的索引
          const stockCodeIndex = headers.findIndex(h =>
            h && (h.includes('股票代码') || h.includes('证券代码') || h.includes('代码'))
          );
          const safetyIndex = headers.findIndex(h =>
            h && (h.includes('安全性') || h === '安全评级')
          );
          const fundamentalIndex = headers.findIndex(h =>
            h && (h.includes('基本面') || h === '基本面评级')
          );
          const entryPriceIndex = headers.findIndex(h =>
            h && (h.includes('进场价格') || h.includes('进场价') || h === '价格')
          );

          console.log('Column indices:', { stockCodeIndex, safetyIndex, fundamentalIndex, entryPriceIndex });

          if (stockCodeIndex >= 0) {
            stocks = jsonData
              .slice(1)
              .map((row) => {
                let code = row[stockCodeIndex]?.toString().trim() || '';
                // 只去除A股后缀，保留港股 .HK 后缀
                if (/\.(SH|SZ|sh|sz)$/.test(code)) {
                  code = code.replace(/\.(SH|SZ|sh|sz)$/, '');
                }

                return {
                  code,
                  safety_rating: safetyIndex >= 0 && row[safetyIndex] ? row[safetyIndex].toString().trim() : undefined,
                  fundamental_rating: fundamentalIndex >= 0 && row[fundamentalIndex] ? row[fundamentalIndex].toString().trim() : undefined,
                  entry_price: entryPriceIndex >= 0 && row[entryPriceIndex] ? parseFloat(row[entryPriceIndex]) : undefined,
                };
              })
              .filter((s) => s.code && s.code.length > 0);
          } else {
            // 没有找到表头，使用第一列
            stocks = jsonData
              .map((row) => {
                let code = row[0]?.toString().trim() || '';
                if (/\.(SH|SZ|sh|sz)$/.test(code)) {
                  code = code.replace(/\.(SH|SZ|sh|sz)$/, '');
                }
                return { code };
              })
              .filter((s) => s.code && s.code.length > 0);
          }
        }

        console.log('Extracted stocks (before dedup):', stocks.slice(0, 5));

        // 去重（按代码）
        const seen = new Set<string>();
        stocks = stocks.filter(s => {
          if (seen.has(s.code)) return false;
          seen.add(s.code);
          return true;
        });

        console.log('Extracted stocks (after dedup):', stocks.length);

        if (stocks.length === 0) {
          message.error('文件中没有找到有效的股票代码');
          return;
        }

        setParsedStocks(stocks);
        setImportModalVisible(true);
      } catch (error) {
        console.error('Parse file error:', error);
        message.error('文件解析失败，请检查文件格式');
      }
    };

    reader.onerror = (error) => {
      console.error('FileReader error:', error);
      message.error('文件读取失败');
    };

    if (fileType === '.csv') {
      reader.readAsText(fileToRead as File);
    } else {
      reader.readAsBinaryString(fileToRead as File);
    }

    return false; // 阻止自动上传
  };

  // 确认导入
  const handleConfirmImport = async () => {
    if (parsedStocks.length === 0) return;

    setImporting(true);
    try {
      const response = await favoriteService.batchAdd(parsedStocks);
      const data = response;

      setImportModalVisible(false);
      setParsedStocks([]);

      // 刷新收藏列表
      useFavoriteStore.getState().loadFavorites();

      // 处理不同的情况
      const totalCount = (data.success || 0) + (data.updated || 0);
      if (data.failed === 0) {
        message.success(`导入完成：新增 ${data.success} 只，更新 ${data.updated || 0} 只`);
      } else if (totalCount === 0 && data.failed > 0) {
        const failedResults = data.results.filter((r: any) => !r.success);
        const errorDetails = failedResults.slice(0, 3).map((r: any) =>
          `${r.stock_code}: ${r.error || '未知错误'}`
        ).join('；');

        message.error(`导入失败：${errorDetails}${failedResults.length > 3 ? '...' : ''}`);
      } else {
        message.warning(
          `导入完成：新增 ${data.success} 只，更新 ${data.updated || 0} 只，失败 ${data.failed} 只`
        );
      }
    } catch (error) {
      message.error(error instanceof Error ? error.message : '导入失败');
    } finally {
      setImporting(false);
    }
  };

  return (
    <div>
      <Space.Compact style={{ width: '100%' }}>
        <AutoComplete
          value={searchText}
          options={options}
          onSearch={handleSearch}
          onSelect={handleSelect}
          placeholder="输入股票代码或名称搜索（如：600382 或 茅台）"
          style={{ flex: 1 }}
          filterOption={false}
          notFoundContent={loading ? '搜索中...' : '未找到股票'}
        />
        <Button
          type="primary"
          icon={<PlusOutlined aria-hidden="true" />}
          onClick={handleManualAdd}
          disabled={!searchText.trim()}
        >
          添加
        </Button>
      </Space.Compact>

      <div style={{ marginTop: 8 }}>
        <span style={{ color: '#999', fontSize: 12 }}>
          提示：可以从搜索结果中选择，或直接输入股票代码添加
        </span>
      </div>

      <Divider style={{ margin: '12px 0' }} />

      <Upload
        beforeUpload={handleFileUpload}
        showUploadList={false}
        accept=".xlsx,.xls,.csv"
      >
        <Button icon={<UploadOutlined />} block>
          批量导入（Excel/CSV）
        </Button>
      </Upload>

      <div style={{ marginTop: 8 }}>
        <span style={{ color: '#999', fontSize: 12 }}>
          支持导入：股票代码、安全性评级、基本面评级、进场价格
        </span>
      </div>

      {/* 导入预览弹窗 */}
      <Modal
        title={
          <span>
            <FileTextOutlined style={{ marginRight: 8 }} />
            确认导入股票
          </span>
        }
        open={importModalVisible}
        onOk={handleConfirmImport}
        onCancel={() => {
          setImportModalVisible(false);
          setParsedStocks([]);
        }}
        confirmLoading={importing}
        width={800}
        okText="确认导入"
        cancelText="取消"
      >
        <Alert
          message={`共解析到 ${parsedStocks.length} 只股票`}
          description="确认后将批量添加到收藏列表，包含安全性评级、基本面评级、进场价格等字段"
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />

        <Table
          dataSource={parsedStocks.map((stock, index) => ({
            key: index,
            ...stock,
          }))}
          columns={[
            {
              title: '序号',
              key: 'index',
              width: 60,
              render: (_, __, index) => index + 1,
            },
            {
              title: '股票代码',
              dataIndex: 'code',
              key: 'code',
              width: 100,
            },
            {
              title: '安全性',
              dataIndex: 'safety_rating',
              key: 'safety_rating',
              width: 80,
              render: (v) => v || '-',
            },
            {
              title: '基本面',
              dataIndex: 'fundamental_rating',
              key: 'fundamental_rating',
              width: 80,
              render: (v) => v || '-',
            },
            {
              title: '进场价格',
              dataIndex: 'entry_price',
              key: 'entry_price',
              width: 100,
              render: (v) => v ? v.toFixed(2) : '-',
            },
          ]}
          pagination={{
            pageSize: 10,
            showTotal: (total) => `共 ${total} 条`,
          }}
          size="small"
          scroll={{ y: 300 }}
        />
      </Modal>
    </div>
  );
}
