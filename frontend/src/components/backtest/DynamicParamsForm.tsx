import { Form, InputNumber, Switch, Select, Input } from 'antd';
import type { StrategySchema } from '@/types';

interface DynamicParamsFormProps {
  schema: StrategySchema['params_schema'];
  value: Record<string, any>;
  onChange: (value: Record<string, any>) => void;
}

export function DynamicParamsForm({ schema, value, onChange }: DynamicParamsFormProps) {
  const handleChange = (key: string, val: any) => {
    onChange({ ...value, [key]: val });
  };

  const renderField = (key: string, fieldSchema: any) => {
    const currentValue = value[key];

    switch (fieldSchema.type) {
      case 'integer':
      case 'number':
        return (
          <InputNumber
            min={fieldSchema.minimum}
            max={fieldSchema.maximum}
            value={currentValue}
            onChange={(val) => handleChange(key, val)}
            style={{ width: '100%' }}
          />
        );
      case 'boolean':
        return (
          <Switch
            checked={currentValue}
            onChange={(val) => handleChange(key, val)}
          />
        );
      case 'string':
        // 如果有 enum 选项，使用下拉选择框
        if (fieldSchema.enum && fieldSchema.enum.length > 0) {
          return (
            <Select
              value={currentValue ?? fieldSchema.default}
              onChange={(val) => handleChange(key, val)}
              style={{ width: '100%' }}
              options={fieldSchema.enum.map((opt: string) => ({
                value: opt,
                label: opt,
              }))}
            />
          );
        }
        // 普通 string 类型使用输入框
        return (
          <Input
            value={currentValue}
            onChange={(e) => handleChange(key, e.target.value)}
            style={{ width: '100%' }}
          />
        );
      default:
        return null;
    }
  };

  return (
    <Form layout="vertical">
      {Object.entries(schema.properties || {}).map(([key, fieldSchema]) => (
        <Form.Item
          key={key}
          label={fieldSchema.description}
          tooltip={`参数名: ${key}`}
        >
          {renderField(key, fieldSchema)}
        </Form.Item>
      ))}
    </Form>
  );
}
