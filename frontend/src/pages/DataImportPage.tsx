// frontend/src/pages/DataImportPage.tsx
import { useState, useCallback, useEffect } from 'react';
import {
  Card,
  Upload,
  Button,
  Alert,
  Table,
  Space,
  Typography,
  Tag,
  Modal,
  Form,
  Select,
  Divider,
  message,
  Spin,
} from 'antd';
import {
  UploadOutlined,
  CheckCircleOutlined,
  LoadingOutlined,
  InboxOutlined,
  FileExcelOutlined,
  SyncOutlined,
} from '@ant-design/icons';
import type { UploadFile } from 'antd/es/upload/interface';
import { dataImportService, type ImportResult, type TableInfo } from '@/services/dataImportService';

const { Title, Text, Paragraph } = Typography;
const { Dragger } = Upload;

function DataImportPage() {
  const [fileList, setFileList] = useState<UploadFile[]>([]);
  const [uploading, setUploading] = useState(false);
  const [importResult, setImportResult] = useState<ImportResult | null>(null);
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [loadingTables, setLoadingTables] = useState(false);
  const [optionsVisible, setOptionsVisible] = useState(false);
  const [customInterval, setCustomInterval] = useState<string | undefined>();
  const [sheetNames, setSheetNames] = useState<string[]>([]);
  const [selectedSheet, setSelectedSheet] = useState<string | undefined>();
  const [loadingSheets, setLoadingSheets] = useState(false);

  // 加载表列表
  const loadTables = useCallback(async () => {
    setLoadingTables(true);
    try {
      const response = await dataImportService.listTables();
      if (response.success && response.data) {
        setTables(response.data);
      }
    } catch (error) {
      console.error('Failed to load tables:', error);
    } finally {
      setLoadingTables(false);
    }
  }, []);

  // 页面加载时获取表列表
  useEffect(() => {
    loadTables();
  }, [loadTables]);

  // 处理文件选择
  const handleFileChange = useCallback(async (info: any) => {
    setFileList(info.fileList);
    setImportResult(null);
    setSheetNames([]);
    setSelectedSheet(undefined);

    const file = info.fileList[0]?.originFileObj;
    if (!file) return;

    // 如果是 Excel 文件，获取标签页列表
    const fileName = file.name.toLowerCase();
    if (fileName.endsWith('.xlsx') || fileName.endsWith('.xls')) {
      setLoadingSheets(true);
      try {
        const response = await dataImportService.getSheetNames(file);
        if (response.success && response.data) {
          setSheetNames(response.data.sheets);
          // 默认选择第一个标签页
          if (response.data.sheets.length > 0) {
            setSelectedSheet(response.data.sheets[0]);
          }
        }
      } catch (error) {
        console.error('Failed to get sheet names:', error);
      } finally {
        setLoadingSheets(false);
      }
    }
  }, []);

  // 上传并导入文件
  const handleUpload = useCallback(async () => {
    if (fileList.length === 0) {
      message.warning('请先选择文件');
      return;
    }

    const file = fileList[0].originFileObj;
    if (!file) {
      message.error('文件无效');
      return;
    }

    setUploading(true);
    setImportResult(null);

    try {
      const result = await dataImportService.upload(file, {
        interval: customInterval,
        sheetName: selectedSheet,
      });

      if (result.success && result.data) {
        setImportResult(result.data);
        message.success(result.message || '导入成功');
        // 刷新表列表
        loadTables();
      } else {
        message.error(result.error || '导入失败');
      }
    } catch (error: any) {
      message.error(error.message || '导入失败');
    } finally {
      setUploading(false);
    }
  }, [fileList, customInterval, selectedSheet, loadTables]);

  // 显示导入选项对话框
  const showOptions = () => {
    setOptionsVisible(true);
  };

  // 处理选项确认
  const handleOptionsOk = () => {
    setOptionsVisible(false);
    handleUpload();
  };

  // 表格列定义
  const tableColumns = [
    {
      title: '时间周期',
      dataIndex: 'interval',
      key: 'interval',
      render: (interval: string) => {
        const labels: Record<string, string> = {
          '1d': '日线',
          '5m': '5分钟',
          '15m': '15分钟',
          '30m': '30分钟',
          '60m': '60分钟',
        };
        return <Tag color="blue">{labels[interval] || interval}</Tag>;
      },
    },
    {
      title: '表名',
      dataIndex: 'table_name',
      key: 'table_name',
      render: (name: string) => <Text code>{name}</Text>,
    },
    {
      title: '数据行数',
      dataIndex: 'row_count',
      key: 'row_count',
      render: (count: number) => <Text strong>{count?.toLocaleString()}</Text>,
    },
    {
      title: '股票数量',
      dataIndex: 'symbol_count',
      key: 'symbol_count',
    },
    {
      title: '日期范围',
      dataIndex: 'date_range',
      key: 'date_range',
      render: (range: { start: string; end: string }) => (
        <Text>
          {range.start && range.end
            ? `${range.start.split('T')[0]} ~ ${range.end.split('T')[0]}`
            : '-'}
        </Text>
      ),
    },
  ];

  const intervalOptions = [
    { label: '自动检测', value: undefined },
    { label: '5分钟', value: '5m' },
    { label: '15分钟', value: '15m' },
    { label: '30分钟', value: '30m' },
    { label: '60分钟', value: '60m' },
    { label: '日线', value: '1d' },
  ];

  // 判断是否是 Excel 文件
  const isExcelFile = fileList.length > 0 &&
    (fileList[0].name?.toLowerCase().endsWith('.xlsx') ||
     fileList[0].name?.toLowerCase().endsWith('.xls'));

  return (
    <div style={{ padding: '0 0 24px 0' }}>
      <Title level={2}>数据导入</Title>
      <Paragraph type="secondary">
        上传 Excel 或 CSV 文件，自动识别时间周期并导入到数据库
      </Paragraph>

      <Divider />

      <Space direction="vertical" style={{ width: '100%' }} size="large">
        {/* 上传区域 */}
        <Card title="上传文件">
          <Dragger
            fileList={fileList}
            onChange={handleFileChange}
            beforeUpload={() => false}
            accept=".xlsx,.xls,.csv"
            maxCount={1}
          >
            <p className="ant-upload-drag-icon">
              <InboxOutlined />
            </p>
            <p className="ant-upload-text">点击或拖拽文件到此区域上传</p>
            <p className="ant-upload-hint">
              支持 .xlsx, .xls, .csv 格式
              <br />
              文件需包含列: symbol（股票代码）, datetime（日期时间）, close（收盘价）
            </p>
          </Dragger>

          {/* 显示标签页选择 */}
          {isExcelFile && sheetNames.length > 0 && (
            <div style={{ marginTop: 16 }}>
              <Space>
                <Text strong>检测到 {sheetNames.length} 个标签页：</Text>
                <Select
                  value={selectedSheet}
                  onChange={setSelectedSheet}
                  options={sheetNames.map(name => ({ label: name, value: name }))}
                  style={{ width: 200 }}
                />
                {loadingSheets && <Spin size="small" />}
              </Space>
            </div>
          )}

          <Space style={{ marginTop: 16 }}>
            <Button
              type="primary"
              icon={uploading ? <LoadingOutlined /> : <UploadOutlined />}
              onClick={showOptions}
              disabled={fileList.length === 0 || uploading}
              loading={uploading}
            >
              {uploading ? '导入中...' : '开始导入'}
            </Button>
            <Button
              icon={<FileExcelOutlined />}
              href="/templates/data_import_template.xlsx"
              disabled
            >
              下载模板
            </Button>
          </Space>
        </Card>

        {/* 导入选项对话框 */}
        <Modal
          title="导入选项"
          open={optionsVisible}
          onOk={handleOptionsOk}
          onCancel={() => setOptionsVisible(false)}
          okText="开始导入"
          cancelText="取消"
        >
          <Form layout="vertical">
            {/* 标签页选择 */}
            {isExcelFile && sheetNames.length > 1 && (
              <Form.Item label="选择标签页">
                <Select
                  value={selectedSheet}
                  onChange={setSelectedSheet}
                  options={sheetNames.map(name => ({ label: name, value: name }))}
                />
                <Paragraph type="secondary" style={{ marginBottom: 0, marginTop: 8 }}>
                  Excel 文件包含 {sheetNames.length} 个标签页，请选择要导入的数据表
                </Paragraph>
              </Form.Item>
            )}
            {isExcelFile && sheetNames.length === 1 && (
              <Form.Item label="标签页">
                <Text>{selectedSheet}</Text>
                <Paragraph type="secondary" style={{ marginBottom: 0, marginTop: 8 }}>
                  此 Excel 文件只有 1 个标签页
                </Paragraph>
              </Form.Item>
            )}
            <Form.Item label="时间周期">
              <Select
                value={customInterval}
                onChange={setCustomInterval}
                options={intervalOptions}
                placeholder="默认自动检测"
              />
            </Form.Item>
            <Paragraph type="secondary" style={{ marginBottom: 0 }}>
              <Text type="secondary">
                💡 提示：选择"自动检测"将根据数据中的时间间隔自动判断周期
              </Text>
            </Paragraph>
          </Form>
        </Modal>

        {/* 导入结果 */}
        {importResult && (
          <Card
            title={
              <Space>
                <CheckCircleOutlined style={{ color: '#52c41a' }} />
                <span>导入成功</span>
              </Space>
            }
          >
            <Alert
              message={`成功导入 ${importResult.rows_imported} 条数据`}
              description={
                <Space direction="vertical" style={{ width: '100%' }}>
                  <div>
                    <Text strong>目标表：</Text>
                    <Text code>{importResult.table_name}</Text>
                  </div>
                  <div>
                    <Text strong>检测周期：</Text>
                    <Tag color="blue">{importResult.detected_interval}</Tag>
                  </div>
                  <div>
                    <Text strong>股票数量：</Text>
                    <Text>{importResult.symbol_count}</Text>
                  </div>
                  <div>
                    <Text strong>时间范围：</Text>
                    <Text>
                      {importResult.date_range.start?.split('T')[0]} ~{' '}
                      {importResult.date_range.end?.split('T')[0]}
                    </Text>
                  </div>
                  {selectedSheet && (
                    <div>
                      <Text strong>标签页：</Text>
                      <Text>{selectedSheet}</Text>
                    </div>
                  )}
                </Space>
              }
              type="success"
              showIcon
            />
          </Card>
        )}

        {/* 数据库表列表 */}
        <Card
          title="已导入数据"
          extra={
            <Button
              size="small"
              icon={<SyncOutlined />}
              onClick={loadTables}
              loading={loadingTables}
            >
              刷新
            </Button>
          }
        >
          <Table
            columns={tableColumns}
            dataSource={tables}
            rowKey="table_name"
            loading={loadingTables}
            pagination={false}
            size="small"
            locale={{
              emptyText: (
                <div style={{ padding: '40px 0', textAlign: 'center' }}>
                  <Text type="secondary">暂无数据，请上传文件导入</Text>
                </div>
              ),
            }}
          />
        </Card>

        {/* 使用说明 */}
        <Card title="使用说明" type="inner">
          <Space direction="vertical" size="small">
            <div>
              <Text strong>1. 文件格式：</Text>
              <Text>
                支持 Excel (.xlsx, .xls) 和 CSV 文件，编码支持 UTF-8、GBK、GB2312
              </Text>
            </div>
            <div>
              <Text strong>2. Excel 多标签页：</Text>
              <Text>
                上传后会自动检测所有标签页，可在导入前选择要导入的标签页
              </Text>
            </div>
            <div>
              <Text strong>3. 必需列：</Text>
              <Text code>symbol</Text>
              <Text>、</Text>
              <Text code>datetime</Text>
              <Text>、</Text>
              <Text code>close</Text>
            </div>
            <div>
              <Text strong>4. 可选列：</Text>
              <Text code>open</Text>
              <Text>、</Text>
              <Text code>high</Text>
              <Text>、</Text>
              <Text code>low</Text>
              <Text>、</Text>
              <Text code>volume</Text>
              <Text>、</Text>
              <Text code>amount</Text>
            </div>
            <div>
              <Text strong>5. 列名支持：</Text>
              <Text>
                中英文列名均可，如：代码/symbol、日期/datetime、开/open、收/close
              </Text>
            </div>
            <div>
              <Text strong>6. 时间周期：</Text>
              <Text>
                系统会根据 datetime 列的时间格式自动识别：日线（无时间）或分钟线（有时分秒）
              </Text>
            </div>
            <div>
              <Text strong>7. 股票代码：</Text>
              <Text>
                支持 600000、600000.SH 等格式，会自动去除交易所后缀
              </Text>
            </div>
          </Space>
        </Card>
      </Space>
    </div>
  );
}

export default DataImportPage;
