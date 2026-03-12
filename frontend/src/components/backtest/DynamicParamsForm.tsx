import { Form, Input, InputNumber, Select, Switch } from 'antd';
import type { StrategyParamField, StrategyParamSchema, StrategyParams } from '@/types';

interface DynamicParamsFormProps {
  schema: StrategyParamSchema;
  value: StrategyParams;
  onChange: (value: StrategyParams) => void;
}

export function DynamicParamsForm({ schema, value, onChange }: DynamicParamsFormProps) {
  const handleChange = (key: string, val: string | number | boolean | null) => {
    onChange({ ...value, [key]: val });
  };

  const renderField = (key: string, fieldSchema: StrategyParamField) => {
    const currentValue = value[key];

    switch (fieldSchema.type) {
      case 'integer':
      case 'number':
        return (
          <InputNumber
            min={fieldSchema.minimum}
            max={fieldSchema.maximum}
            value={typeof currentValue === 'number' ? currentValue : undefined}
            onChange={(val) => handleChange(key, val)}
            style={{ width: '100%' }}
          />
        );
      case 'boolean':
        return (
          <Switch
            checked={Boolean(currentValue)}
            onChange={(val) => handleChange(key, val)}
          />
        );
      case 'string':
        if (fieldSchema.enum && fieldSchema.enum.length > 0) {
          return (
            <Select
              value={typeof currentValue === 'string' ? currentValue : fieldSchema.default}
              onChange={(val) => handleChange(key, val)}
              style={{ width: '100%' }}
              options={fieldSchema.enum.map((opt) => ({
                value: opt,
                label: opt,
              }))}
            />
          );
        }
        return (
          <Input
            value={typeof currentValue === 'string' ? currentValue : ''}
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
          label={fieldSchema.description || fieldSchema.title || key}
          tooltip={`参数名: ${key}`}
        >
          {renderField(key, fieldSchema)}
        </Form.Item>
      ))}
    </Form>
  );
}
