"""
ML Utilities Module

Provides database utilities and helper functions for ML models.
"""
import sqlite3
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MLDatabase:
    """
    ML数据库管理类

    负责ML模型和预测的数据库操作
    """

    def __init__(self, db_path: str = "data/tushare_data.db"):
        """
        初始化ML数据库

        Args:
            db_path: 数据库路径
        """
        self.db_path = db_path
        self._init_tables()

    def _init_tables(self):
        """创建ML相关的数据库表"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # ML模型表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ml_models (
                model_id TEXT PRIMARY KEY,
                model_type TEXT NOT NULL,
                symbol TEXT,
                version TEXT DEFAULT '1.0',
                created_at TEXT NOT NULL,
                trained_at TEXT,
                training_start TEXT,
                training_end TEXT,
                hyperparameters TEXT,
                metrics TEXT,
                feature_importance TEXT,
                model_path TEXT,
                n_features INTEGER,
                train_samples INTEGER,
                val_samples INTEGER,
                test_samples INTEGER,
                target_type TEXT,
                status TEXT DEFAULT 'active',
                metadata TEXT
            )
        ''')

        # ML预测表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ml_predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                prediction REAL NOT NULL,
                confidence_lower REAL,
                confidence_upper REAL,
                actual REAL,
                prediction_type TEXT DEFAULT 'price_return',
                created_at TEXT NOT NULL,
                features_snapshot TEXT,
                FOREIGN KEY (model_id) REFERENCES ml_models(model_id)
            )
        ''')

        # ML训练历史表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ml_training_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id TEXT NOT NULL,
                epoch INTEGER,
                train_loss REAL,
                val_loss REAL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (model_id) REFERENCES ml_models(model_id)
            )
        ''')

        # ML模型性能追踪表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ml_performance_tracker (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_id TEXT NOT NULL,
                evaluation_date TEXT NOT NULL,
                test_period_start TEXT,
                test_period_end TEXT,
                mae REAL,
                rmse REAL,
                mape REAL,
                r2 REAL,
                direction_accuracy REAL,
                sharpe_ratio REAL,
                max_drawdown REAL,
                total_return REAL,
                buy_hold_return REAL,
                notes TEXT,
                FOREIGN KEY (model_id) REFERENCES ml_models(model_id)
            )
        ''')

        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_models_symbol ON ml_models(symbol)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_models_type ON ml_models(model_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_models_created ON ml_models(created_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_predictions_model ON ml_predictions(model_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_predictions_symbol_date ON ml_predictions(symbol, date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_perf_model ON ml_performance_tracker(model_id)')

        conn.commit()
        conn.close()

        logger.info("ML database tables initialized")

    def save_model_metadata(
        self,
        model_id: str,
        model_type: str,
        symbol: str,
        hyperparameters: Dict[str, Any],
        metrics: Dict[str, float],
        feature_importance: Dict[str, float],
        model_path: str,
        training_start: str,
        training_end: str,
        n_features: int,
        train_samples: int,
        val_samples: int,
        test_samples: int,
        target_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        保存模型元数据

        Args:
            model_id: 模型ID
            model_type: 模型类型 (lgb, lstm, ensemble)
            symbol: 股票代码
            hyperparameters: 超参数字典
            metrics: 评估指标字典
            feature_importance: 特征重要性字典
            model_path: 模型文件路径
            training_start: 训练开始日期
            training_end: 训练结束日期
            n_features: 特征数量
            train_samples: 训练样本数
            val_samples: 验证样本数
            test_samples: 测试样本数
            target_type: 目标类型
            metadata: 额外元数据

        Returns:
            是否保存成功
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            now = datetime.now().isoformat()

            cursor.execute('''
                INSERT INTO ml_models (
                    model_id, model_type, symbol, created_at, trained_at,
                    training_start, training_end, hyperparameters, metrics,
                    feature_importance, model_path, n_features, train_samples,
                    val_samples, test_samples, target_type, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                model_id,
                model_type,
                symbol,
                now,
                now,
                training_start,
                training_end,
                json.dumps(hyperparameters),
                json.dumps(metrics),
                json.dumps(feature_importance),
                model_path,
                n_features,
                train_samples,
                val_samples,
                test_samples,
                target_type,
                json.dumps(metadata) if metadata else None
            ))

            conn.commit()
            conn.close()

            logger.info(f"Saved model metadata for {model_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to save model metadata: {e}")
            return False

    def get_model(self, model_id: str) -> Optional[Dict[str, Any]]:
        """
        获取模型元数据

        Args:
            model_id: 模型ID

        Returns:
            模型元数据字典
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM ml_models WHERE model_id = ?', (model_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None

        model = dict(row)

        # 解析JSON字段
        if model.get('hyperparameters'):
            try:
                model['hyperparameters'] = json.loads(model['hyperparameters'])
            except:
                model['hyperparameters'] = {}

        if model.get('metrics'):
            try:
                model['metrics'] = json.loads(model['metrics'])
            except:
                model['metrics'] = {}

        if model.get('feature_importance'):
            try:
                model['feature_importance'] = json.loads(model['feature_importance'])
            except:
                model['feature_importance'] = {}

        if model.get('metadata'):
            try:
                model['metadata'] = json.loads(model['metadata'])
            except:
                model['metadata'] = {}

        return model

    def list_models(
        self,
        symbol: Optional[str] = None,
        model_type: Optional[str] = None,
        status: str = 'active',
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        列出模型

        Args:
            symbol: 股票代码过滤
            model_type: 模型类型过滤
            status: 状态过滤
            limit: 返回数量限制

        Returns:
            模型列表
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        conditions = []
        params = []

        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)

        if model_type:
            conditions.append("model_type = ?")
            params.append(model_type)

        if status:
            conditions.append("status = ?")
            params.append(status)

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f'''
            SELECT model_id, model_type, symbol, created_at, trained_at,
                   training_start, training_end, target_type, status
            FROM ml_models
            WHERE {where_clause}
            ORDER BY created_at DESC
            LIMIT ?
        '''
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def save_prediction(
        self,
        model_id: str,
        symbol: str,
        date: str,
        prediction: float,
        confidence_lower: Optional[float] = None,
        confidence_upper: Optional[float] = None,
        actual: Optional[float] = None,
        prediction_type: str = 'price_return',
        features_snapshot: Optional[Dict] = None
    ) -> bool:
        """
        保存预测结果

        Args:
            model_id: 模型ID
            symbol: 股票代码
            date: 日期
            prediction: 预测值
            confidence_lower: 置信区间下限
            confidence_upper: 置信区间上限
            actual: 实际值（事后更新）
            prediction_type: 预测类型
            features_snapshot: 特征快照

        Returns:
            是否保存成功
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            now = datetime.now().isoformat()

            cursor.execute('''
                INSERT INTO ml_predictions (
                    model_id, symbol, date, prediction, confidence_lower,
                    confidence_upper, actual, prediction_type, created_at,
                    features_snapshot
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                model_id,
                symbol,
                date,
                prediction,
                confidence_lower,
                confidence_upper,
                actual,
                prediction_type,
                now,
                json.dumps(features_snapshot) if features_snapshot else None
            ))

            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"Failed to save prediction: {e}")
            return False

    def get_predictions(
        self,
        model_id: str,
        symbol: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取预测记录

        Args:
            model_id: 模型ID
            symbol: 股票代码过滤
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            预测记录列表
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        conditions = ["model_id = ?"]
        params = [model_id]

        if symbol:
            conditions.append("symbol = ?")
            params.append(symbol)

        if start_date:
            conditions.append("date >= ?")
            params.append(start_date)

        if end_date:
            conditions.append("date <= ?")
            params.append(end_date)

        where_clause = " AND ".join(conditions)

        query = f'''
            SELECT * FROM ml_predictions
            WHERE {where_clause}
            ORDER BY date DESC
        '''

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

        predictions = []
        for row in rows:
            pred = dict(row)
            if pred.get('features_snapshot'):
                try:
                    pred['features_snapshot'] = json.loads(pred['features_snapshot'])
                except:
                    pred['features_snapshot'] = {}
            predictions.append(pred)

        return predictions

    def save_performance(
        self,
        model_id: str,
        evaluation_date: str,
        test_period_start: str,
        test_period_end: str,
        metrics: Dict[str, float],
        notes: Optional[str] = None
    ) -> bool:
        """
        保存模型性能追踪记录

        Args:
            model_id: 模型ID
            evaluation_date: 评估日期
            test_period_start: 测试期开始
            test_period_end: 测试期结束
            metrics: 性能指标字典
            notes: 备注

        Returns:
            是否保存成功
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO ml_performance_tracker (
                    model_id, evaluation_date, test_period_start, test_period_end,
                    mae, rmse, mape, r2, direction_accuracy, sharpe_ratio,
                    max_drawdown, total_return, buy_hold_return, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                model_id,
                evaluation_date,
                test_period_start,
                test_period_end,
                metrics.get('mae'),
                metrics.get('rmse'),
                metrics.get('mape'),
                metrics.get('r2'),
                metrics.get('direction_accuracy'),
                metrics.get('sharpe_ratio'),
                metrics.get('max_drawdown'),
                metrics.get('total_return'),
                metrics.get('buy_hold_return'),
                notes
            ))

            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"Failed to save performance record: {e}")
            return False

    def get_model_performance(self, model_id: str) -> List[Dict[str, Any]]:
        """
        获取模型性能记录

        Args:
            model_id: 模型ID

        Returns:
            性能记录列表
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM ml_performance_tracker
            WHERE model_id = ?
            ORDER BY evaluation_date DESC
        ''', (model_id,))

        rows = cursor.fetchall()
        conn.close()

        return [dict(row) for row in rows]

    def delete_model(self, model_id: str) -> bool:
        """
        删除模型及相关记录

        Args:
            model_id: 模型ID

        Returns:
            是否删除成功
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # 删除预测记录
            cursor.execute('DELETE FROM ml_predictions WHERE model_id = ?', (model_id,))

            # 删除性能记录
            cursor.execute('DELETE FROM ml_performance_tracker WHERE model_id = ?', (model_id,))

            # 删除模型元数据
            cursor.execute('DELETE FROM ml_models WHERE model_id = ?', (model_id,))

            conn.commit()
            conn.close()

            logger.info(f"Deleted model {model_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete model: {e}")
            return False


# 全局实例
_ml_db = None


def get_ml_db(db_path: str = "data/tushare_data.db") -> MLDatabase:
    """
    获取ML数据库实例（单例）

    Args:
        db_path: 数据库路径

    Returns:
        MLDatabase实例
    """
    global _ml_db
    if _ml_db is None:
        _ml_db = MLDatabase(db_path)
    return _ml_db
