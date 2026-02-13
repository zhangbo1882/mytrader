import { Form, InputNumber, Switch } from 'antd';
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
