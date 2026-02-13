import { useState, useEffect } from 'react';
import { Select, Spin, Alert } from 'antd';
import { backtestService } from '@/services';
import type { StrategySchema, StrategySelectorValue } from '@/types';
import { DynamicParamsForm } from './DynamicParamsForm';

interface StrategySelectorProps {
  value: StrategySelectorValue;
  onChange: (value: StrategySelectorValue) => void;
}

export function StrategySelector({ value, onChange }: StrategySelectorProps) {
  const [strategies, setStrategies] = useState<StrategySchema[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadStrategies = async () => {
      try {
        const response = await backtestService.getStrategies() as any;
        setStrategies(response.strategies);
      } catch (err) {
        setError(err instanceof Error ? err.message : '加载策略列表失败');
      } finally {
        setLoading(false);
      }
    };
    loadStrategies();
  }, []);

  const selectedStrategy = strategies.find(s => s.strategy_type === value.strategy);

  if (loading) {
    return <Spin tip="加载策略列表..." />;
  }

  if (error) {
    return <Alert message="加载失败" description={error} type="error" showIcon />;
  }

  return (
    <div>
      <Select
        value={value.strategy}
        onChange={(strategy) => {
          const schema = strategies.find(s => s.strategy_type === strategy);
          const defaultParams: Record<string, any> = {};
          if (schema?.params_schema?.properties) {
            Object.entries(schema.params_schema.properties).forEach(([key, prop]) => {
              defaultParams[key] = prop.default;
            });
          }
          onChange({ strategy, strategy_params: defaultParams });
        }}
        options={strategies.map(s => ({
          value: s.strategy_type,
          label: s.name,
          title: s.description,
        }))}
        style={{ width: '100%' }}
        placeholder="选择策略"
      />

      {selectedStrategy && (
        <>
          <div style={{ marginTop: 12, marginBottom: 16, color: '#666' }}>
            {selectedStrategy.description}
          </div>
          <DynamicParamsForm
            schema={selectedStrategy.params_schema}
            value={value.strategy_params}
            onChange={(strategy_params) => onChange({ ...value, strategy_params })}
          />
        </>
      )}
    </div>
  );
}
