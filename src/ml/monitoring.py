"""
Model Monitoring and Tracking Module

Provides functionality to track model performance over time,
detect concept drift, and trigger retraining when needed.
"""
import numpy as np
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import logging
from pathlib import Path

from .utils import MLDatabase

logger = logging.getLogger(__name__)


class ModelMonitor:
    """
    模型监控器

    跟踪模型性能、检测概念漂移、触发重训练
    """

    def __init__(
        self,
        db: Optional[MLDatabase] = None,
        drift_threshold: float = 0.1,
        performance_window: int = 30
    ):
        """
        初始化监控器

        Args:
            db: ML数据库实例
            drift_threshold: 漂移阈值（MAE增加百分比）
            performance_window: 性能窗口（天数）
        """
        self.db = db
        self.drift_threshold = drift_threshold
        self.performance_window = performance_window

    def log_prediction(
        self,
        model_id: str,
        symbol: str,
        date: str,
        prediction: float,
        confidence_lower: Optional[float] = None,
        confidence_upper: Optional[float] = None,
        features_snapshot: Optional[Dict] = None
    ) -> bool:
        """
        记录预测结果

        Args:
            model_id: 模型ID
            symbol: 股票代码
            date: 日期
            prediction: 预测值
            confidence_lower: 置信区间下限
            confidence_upper: 置信区间上限
            features_snapshot: 特征快照

        Returns:
            是否成功
        """
        if self.db is None:
            logger.warning("No database configured, skipping prediction logging")
            return False

        return self.db.save_prediction(
            model_id=model_id,
            symbol=symbol,
            date=date,
            prediction=prediction,
            confidence_lower=confidence_lower,
            confidence_upper=confidence_upper,
            features_snapshot=features_snapshot
        )

    def log_performance(
        self,
        model_id: str,
        evaluation_date: str,
        test_period_start: str,
        test_period_end: str,
        metrics: Dict[str, float],
        notes: Optional[str] = None
    ) -> bool:
        """
        记录模型性能

        Args:
            model_id: 模型ID
            evaluation_date: 评估日期
            test_period_start: 测试期开始
            test_period_end: 测试期结束
            metrics: 性能指标
            notes: 备注

        Returns:
            是否成功
        """
        if self.db is None:
            logger.warning("No database configured, skipping performance logging")
            return False

        return self.db.save_performance(
            model_id=model_id,
            evaluation_date=evaluation_date,
            test_period_start=test_period_start,
            test_period_end=test_period_end,
            metrics=metrics,
            notes=notes
        )

    def detect_drift(
        self,
        model_id: str,
        current_metrics: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        检测概念漂移

        Args:
            model_id: 模型ID
            current_metrics: 当前性能指标

        Returns:
            漂移检测结果
        """
        if self.db is None:
            return {'drift_detected': False, 'reason': 'No database configured'}

        # 获取历史性能
        performance_history = self.db.get_model_performance(model_id)

        if not performance_history or len(performance_history) < 2:
            return {
                'drift_detected': False,
                'reason': 'Insufficient history',
                'history_length': len(performance_history) if performance_history else 0
            }

        # 获取最近的性能
        recent = performance_history[0]
        baseline = performance_history[min(4, len(performance_history) - 1)]

        # 比较MAE
        current_mae = current_metrics.get('mae', 0)
        baseline_mae = recent.get('mae', 0)

        if baseline_mae > 0:
            mae_increase = (current_mae - baseline_mae) / baseline_mae
        else:
            mae_increase = 0

        drift_detected = mae_increase > self.drift_threshold

        result = {
            'drift_detected': drift_detected,
            'mae_increase': float(mae_increase),
            'current_mae': float(current_mae),
            'baseline_mae': float(baseline_mae),
            'threshold': self.drift_threshold,
            'recommendation': 'retrain' if drift_detected else 'monitor'
        }

        if drift_detected:
            logger.warning(f"Concept drift detected for model {model_id}: MAE increased by {mae_increase:.2%}")

        return result

    def should_retrain(
        self,
        model_id: str,
        current_metrics: Dict[str, float],
        max_days_since_training: int = 30
    ) -> Tuple[bool, str]:
        """
        判断是否需要重训练

        Args:
            model_id: 模型ID
            current_metrics: 当前性能指标
            max_days_since_training: 最大训练天数

        Returns:
            (should_retrain, reason)
        """
        if self.db is None:
            return False, 'No database configured'

        # 获取模型信息
        model = self.db.get_model(model_id)
        if not model:
            return True, 'Model not found in database'

        # 检查训练时间
        trained_at = datetime.fromisoformat(model['trained_at'])
        days_since_training = (datetime.now() - trained_at).days

        if days_since_training > max_days_since_training:
            return True, f'Model is {days_since_training} days old (threshold: {max_days_since_training})'

        # 检查概念漂移
        drift_result = self.detect_drift(model_id, current_metrics)
        if drift_result['drift_detected']:
            return True, f"Concept drift detected: MAE increased by {drift_result['mae_increase']:.2%}"

        # 检查性能阈值
        current_mae = current_metrics.get('mae', 0)
        if current_mae > 0.05:  # 5% MAE阈值
            return True, f'MAE {current_mae:.4f} exceeds threshold 0.05'

        return False, 'Model performing adequately'

    def get_model_summary(self, model_id: str) -> Dict[str, Any]:
        """
        获取模型摘要

        Args:
            model_id: 模型ID

        Returns:
            模型摘要
        """
        if self.db is None:
            return {'error': 'No database configured'}

        model = self.db.get_model(model_id)
        if not model:
            return {'error': 'Model not found'}

        performance = self.db.get_model_performance(model_id)
        predictions = self.db.get_predictions(model_id, limit=100)

        # 计算统计
        summary = {
            'model_id': model_id,
            'symbol': model['symbol'],
            'model_type': model['model_type'],
            'created_at': model['created_at'],
            'trained_at': model['trained_at'],
            'training_period': f"{model['training_start']} ~ {model['training_end']}",
            'n_features': model['n_features'],
            'train_samples': model['train_samples'],
            'test_samples': model['test_samples'],

            # 初始性能
            'initial_metrics': model['metrics'],

            # 预测统计
            'total_predictions': len(predictions),
            'recent_predictions': predictions[:10] if predictions else [],

            # 性能趋势
            'performance_records': len(performance),
        }

        if performance:
            # 计算平均性能
            avg_mae = np.mean([p.get('mae', 0) for p in performance if p.get('mae')])
            avg_sharpe = np.mean([p.get('sharpe_ratio', 0) for p in performance if p.get('sharpe_ratio')])

            summary['avg_mae'] = float(avg_mae)
            summary['avg_sharpe'] = float(avg_sharpe)
            summary['latest_performance'] = performance[0]

        return summary

    def compare_models(
        self,
        model_ids: List[str]
    ) -> pd.DataFrame:
        """
        比较多个模型

        Args:
            model_ids: 模型ID列表

        Returns:
            比较结果DataFrame
        """
        if self.db is None:
            return pd.DataFrame()

        summaries = []
        for model_id in model_ids:
            summary = self.get_model_summary(model_id)
            if 'error' not in summary:
                summaries.append({
                    'model_id': model_id,
                    'symbol': summary['symbol'],
                    'type': summary['model_type'],
                    'mae': summary['initial_metrics'].get('mae', 0),
                    'sharpe': summary['initial_metrics'].get('sharpe_ratio', 0),
                    'predictions': summary['total_predictions'],
                    'trained_at': summary['trained_at']
                })

        df = pd.DataFrame(summaries)
        if not df.empty:
            df = df.sort_values('mae')

        return df


class PerformanceLogger:
    """
    性能日志记录器

    记录训练和评估过程中的详细指标
    """

    def __init__(self, log_dir: str = "data/ml_logs"):
        """
        初始化日志记录器

        Args:
            log_dir: 日志目录
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.session_logs = {}

    def start_session(self, model_id: str) -> str:
        """
        开始新的日志会话

        Args:
            model_id: 模型ID

        Returns:
            会话ID
        """
        session_id = f"{model_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        self.session_logs[session_id] = {
            'model_id': model_id,
            'start_time': datetime.now().isoformat(),
            'events': []
        }

        logger.info(f"Started logging session {session_id}")

        return session_id

    def log_event(
        self,
        session_id: str,
        event_type: str,
        data: Dict[str, Any]
    ) -> None:
        """
        记录事件

        Args:
            session_id: 会话ID
            event_type: 事件类型
            data: 事件数据
        """
        if session_id not in self.session_logs:
            logger.warning(f"Session {session_id} not found")
            return

        event = {
            'timestamp': datetime.now().isoformat(),
            'type': event_type,
            'data': data
        }

        self.session_logs[session_id]['events'].append(event)

    def log_training_progress(
        self,
        session_id: str,
        epoch: int,
        train_loss: float,
        val_loss: Optional[float] = None,
        **extra_data
    ) -> None:
        """
        记录训练进度

        Args:
            session_id: 会话ID
            epoch: 轮次
            train_loss: 训练损失
            val_loss: 验证损失
            **extra_data: 额外数据
        """
        data = {
            'epoch': epoch,
            'train_loss': train_loss,
            'val_loss': val_loss,
            **extra_data
        }

        self.log_event(session_id, 'training_progress', data)

    def end_session(
        self,
        session_id: str,
        final_metrics: Dict[str, Any]
    ) -> None:
        """
        结束日志会话

        Args:
            session_id: 会话ID
            final_metrics: 最终指标
        """
        if session_id not in self.session_logs:
            logger.warning(f"Session {session_id} not found")
            return

        self.session_logs[session_id]['end_time'] = datetime.now().isoformat()
        self.session_logs[session_id]['final_metrics'] = final_metrics

        # 保存到文件
        log_file = self.log_dir / f"{session_id}.json"
        import json

        with open(log_file, 'w') as f:
            json.dump(self.session_logs[session_id], f, indent=2)

        logger.info(f"Saved session log to {log_file}")

    def get_session_summary(self, session_id: str) -> Dict[str, Any]:
        """
        获取会话摘要

        Args:
            session_id: 会话ID

        Returns:
            会话摘要
        """
        if session_id not in self.session_logs:
            return {'error': 'Session not found'}

        session = self.session_logs[session_id]

        # 计算训练时长
        if 'end_time' in session:
            start = datetime.fromisoformat(session['start_time'])
            end = datetime.fromisoformat(session['end_time'])
            duration = (end - start).total_seconds()
        else:
            duration = None

        # 统计事件
        event_types = {}
        for event in session['events']:
            event_type = event['type']
            event_types[event_type] = event_types.get(event_type, 0) + 1

        summary = {
            'session_id': session_id,
            'model_id': session['model_id'],
            'start_time': session['start_time'],
            'end_time': session.get('end_time'),
            'duration_seconds': duration,
            'total_events': len(session['events']),
            'event_types': event_types,
            'final_metrics': session.get('final_metrics')
        }

        return summary


# 全局监控器实例
_global_monitor = None


def get_monitor(db: Optional[MLDatabase] = None) -> ModelMonitor:
    """
    获取全局监控器实例

    Args:
        db: ML数据库实例

    Returns:
        ModelMonitor实例
    """
    global _global_monitor
    if _global_monitor is None:
        _global_monitor = ModelMonitor(db=db)
    return _global_monitor
