"""
SQLite database for storing optimized REGIME_PARAMS per stock.
"""

import sqlite3
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path


class RegimeParamsDB:
    """
    Database for storing optimized parameters per stock.

    Each stock gets its own optimized parameters based on historical performance.
    """

    def __init__(self, db_path: str = 'data/regime_params.db'):
        """
        Initialize database connection.

        Args:
            db_path: Path to SQLite database file
        """
        # Ensure directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_tables()

    def _init_tables(self):
        """Create database tables if they don't exist."""
        # Table for storing optimized parameters per stock
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS best_params (
                stock_code TEXT PRIMARY KEY,
                market_config TEXT,
                regime_params TEXT,
                metrics TEXT,
                score REAL,
                phase TEXT,
                start_date TEXT,
                end_date TEXT,
                updated_at TEXT
            )
        """)

        # Table for storing default strategy parameters (fixed multipliers)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS default_strategy_params (
                strategy_name TEXT PRIMARY KEY,
                bull_buy_multiplier REAL NOT NULL,
                bull_sell_multiplier REAL NOT NULL,
                bull_stop_multiplier REAL NOT NULL,
                bear_buy_multiplier REAL NOT NULL,
                bear_sell_multiplier REAL NOT NULL,
                bear_stop_multiplier REAL NOT NULL,
                neutral_buy_multiplier REAL NOT NULL,
                neutral_sell_multiplier REAL NOT NULL,
                neutral_stop_multiplier REAL NOT NULL,
                bull_threshold INTEGER NOT NULL,
                bear_threshold INTEGER NOT NULL,
                description TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_score ON best_params(score DESC)
        """)

        self.conn.commit()

    def save_best_params(self, stock_code: str, params: Dict[str, Any],
                        metrics: Dict[str, Any], score: float,
                        phase: str = 'final', start_date: str = None,
                        end_date: str = None):
        """
        Save or update best parameters for a stock.

        Args:
            stock_code: Stock symbol (e.g., '00941.HK')
            params: Optimized parameters (market_config + regime_params)
            metrics: Backtest metrics
            score: Composite optimization score
            phase: Optimization phase ('coarse', 'fine', 'final')
            start_date: Backtest start date
            end_date: Backtest end date
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO best_params
            (stock_code, market_config, regime_params, metrics, score, phase,
             start_date, end_date, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """, (
            stock_code,
            json.dumps(params.get('market_config', {})),
            json.dumps(params.get('regime_params', {})),
            json.dumps(metrics),
            score,
            phase,
            start_date,
            end_date
        ))
        self.conn.commit()

    def get_best_params(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        Get best parameters for a stock.

        Args:
            stock_code: Stock symbol

        Returns:
            Dictionary with parameters and metrics, or None if not found
        """
        cursor = self.conn.execute("""
            SELECT market_config, regime_params, metrics, score, phase, start_date, end_date, updated_at
            FROM best_params
            WHERE stock_code = ?
            ORDER BY score DESC
            LIMIT 1
        """, (stock_code,))
        row = cursor.fetchone()

        if row:
            return {
                'stock_code': stock_code,
                'market_config': json.loads(row[0]),
                'regime_params': json.loads(row[1]),
                'metrics': json.loads(row[2]),
                'score': row[3],
                'phase': row[4],
                'start_date': row[5],
                'end_date': row[6],
                'updated_at': row[7]
            }
        return None

    def list_all_stocks(self, order_by: str = 'score',
                       limit: int = None) -> List[Dict[str, Any]]:
        """
        List all optimized stocks.

        Args:
            order_by: Sort field ('score', 'updated_at', 'stock_code')
            limit: Maximum number of results

        Returns:
            List of stock summaries
        """
        order_clause = {
            'score': 'score DESC',
            'updated_at': 'updated_at DESC',
            'stock_code': 'stock_code ASC'
        }.get(order_by, 'score DESC')

        limit_clause = f'LIMIT {limit}' if limit else ''

        cursor = self.conn.execute(f"""
            SELECT stock_code, score, phase, updated_at
            FROM best_params
            ORDER BY {order_clause}
            {limit_clause}
        """)

        return [
            {
                'stock_code': row[0],
                'score': row[1],
                'phase': row[2],
                'updated_at': row[3]
            }
            for row in cursor.fetchall()
        ]

    def get_top_performers(self, n: int = 10) -> List[Dict[str, Any]]:
        """
        Get top N performing stocks.

        Args:
            n: Number of top stocks to return

        Returns:
            List of top performers
        """
        cursor = self.conn.execute("""
            SELECT stock_code, score, metrics, regime_params
            FROM best_params
            ORDER BY score DESC
            LIMIT ?
        """, (n,))

        return [
            {
                'stock_code': row[0],
                'score': row[1],
                'metrics': json.loads(row[2]),
                'regime_params': json.loads(row[3])
            }
            for row in cursor.fetchall()
        ]

    def delete_stock(self, stock_code: str):
        """Remove parameters for a stock."""
        self.conn.execute("""
            DELETE FROM best_params WHERE stock_code = ?
        """, (stock_code,))
        self.conn.commit()

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dictionary with statistics
        """
        cursor = self.conn.execute("""
            SELECT
                COUNT(*) as total_stocks,
                AVG(score) as avg_score,
                MAX(score) as max_score,
                MIN(score) as min_score
            FROM best_params
        """)
        row = cursor.fetchone()

        return {
            'total_stocks': row[0],
            'avg_score': row[1] if row[1] else 0,
            'max_score': row[2] if row[2] else 0,
            'min_score': row[3] if row[3] else 0
        }

    def get_all_content(self) -> Dict[str, Any]:
        """
        Get all content from the database in a structured format.

        Returns:
            Dictionary with all tables and their content
        """
        content = {
            'best_params': [],
            'default_strategy_params': [],
            'statistics': {}
        }

        # Get all optimized stocks with their parameters
        cursor = self.conn.execute("""
            SELECT stock_code, market_config, regime_params, metrics, score, phase, start_date, end_date, updated_at
            FROM best_params
            ORDER BY score DESC
        """)
        for row in cursor.fetchall():
            import json
            market_config = json.loads(row[1])
            regime_params = json.loads(row[2])
            metrics = json.loads(row[3])
            strategy_params = metrics.get('strategy_info', {}).get('strategy_params', {})

            content['best_params'].append({
                'stock_code': row[0],
                'score': row[4],
                'phase': row[5],
                'start_date': row[6],
                'end_date': row[7],
                'updated_at': row[8],
                'market_config': market_config,
                'regime_params': regime_params,
                'strategy_params': strategy_params,
                'total_return': metrics.get('basic_info', {}).get('total_return', 0),
                'sharpe_ratio': metrics.get('health_metrics', {}).get('sharpe_ratio', 0),
                'max_drawdown': metrics.get('health_metrics', {}).get('max_drawdown', 0),
                'win_rate': metrics.get('trade_stats', {}).get('win_rate', 0),
                'total_trades': metrics.get('trade_stats', {}).get('total_trades', 0),
            })

        # Get all default strategy params
        cursor = self.conn.execute("""
            SELECT strategy_name, bull_buy_multiplier, bull_sell_multiplier, bull_stop_multiplier,
                   bear_buy_multiplier, bear_sell_multiplier, bear_stop_multiplier,
                   neutral_buy_multiplier, neutral_sell_multiplier, neutral_stop_multiplier,
                   bull_threshold, bear_threshold, description, created_at, updated_at
            FROM default_strategy_params
            ORDER BY strategy_name
        """)
        for row in cursor.fetchall():
            content['default_strategy_params'].append({
                'strategy_name': row[0],
                'bull': {
                    'buy_multiplier': row[1],
                    'sell_multiplier': row[2],
                    'stop_multiplier': row[3]
                },
                'bear': {
                    'buy_multiplier': row[4],
                    'sell_multiplier': row[5],
                    'stop_multiplier': row[6]
                },
                'neutral': {
                    'buy_multiplier': row[7],
                    'sell_multiplier': row[8],
                    'stop_multiplier': row[9]
                },
                'bull_threshold': row[10],
                'bear_threshold': row[11],
                'description': row[12],
                'created_at': row[13],
                'updated_at': row[14]
            })

        # Get statistics
        content['statistics'] = self.get_statistics()

        return content

    def save_default_strategy_params(self, strategy_name: str,
                                   bull_buy: float, bull_sell: float, bull_stop: float,
                                   bear_buy: float, bear_sell: float, bear_stop: float,
                                   bull_threshold: int = 70, bear_threshold: int = 40,
                                   description: str = None):
        """
        Save default strategy parameters (fixed multipliers).

        Args:
            strategy_name: Strategy identifier (e.g., 'price_breakout_v2_default')
            bull_buy: Bull market buy multiplier
            bull_sell: Bull market sell multiplier
            bull_stop: Bull market stop loss multiplier
            bear_buy: Bear market buy multiplier
            bear_sell: Bear market sell multiplier
            bear_stop: Bear market stop loss multiplier
            bull_threshold: Bull market threshold (0-100)
            bear_threshold: Bear market threshold (0-100)
            description: Parameter description
        """
        self.conn.execute("""
            INSERT OR REPLACE INTO default_strategy_params
            (strategy_name, bull_buy_multiplier, bull_sell_multiplier, bull_stop_multiplier,
             bear_buy_multiplier, bear_sell_multiplier, bear_stop_multiplier,
             neutral_buy_multiplier, neutral_sell_multiplier, neutral_stop_multiplier,
             bull_threshold, bear_threshold, description, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1.0, 1.0, 1.0, ?, ?, ?, datetime('now'), datetime('now'))
        """, (strategy_name, bull_buy, bull_sell, bull_stop, bear_buy, bear_sell, bear_stop,
              bull_threshold, bear_threshold, description))
        self.conn.commit()

    def get_default_strategy_params(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        """
        Get default strategy parameters.

        Args:
            strategy_name: Strategy identifier

        Returns:
            Dictionary with default parameters or None
        """
        cursor = self.conn.execute("""
            SELECT * FROM default_strategy_params WHERE strategy_name = ?
        """, (strategy_name,))
        row = cursor.fetchone()

        if row:
            return {
                'strategy_name': row[0],
                'bull_buy_multiplier': row[1],
                'bull_sell_multiplier': row[2],
                'bull_stop_multiplier': row[3],
                'bear_buy_multiplier': row[4],
                'bear_sell_multiplier': row[5],
                'bear_stop_multiplier': row[6],
                'neutral_buy_multiplier': row[7],
                'neutral_sell_multiplier': row[8],
                'neutral_stop_multiplier': row[9],
                'bull_threshold': row[10],
                'bear_threshold': row[11],
                'description': row[12],
                'created_at': row[13],
                'updated_at': row[14]
            }
        return None

    def list_default_strategies(self) -> List[Dict[str, Any]]:
        """List all default strategy configurations."""
        cursor = self.conn.execute("""
            SELECT strategy_name, description, updated_at FROM default_strategy_params
        """)
        return [
            {
                'strategy_name': row[0],
                'description': row[1],
                'updated_at': row[2]
            }
            for row in cursor.fetchall()
        ]

    def close(self):
        """Close database connection."""
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def show_database_content(db_path: str = 'data/regime_params.db'):
    """
    显示数据库所有内容的便捷函数

    Args:
        db_path: 数据库路径

    Returns:
        包含所有数据库内容的字典
    """
    db = RegimeParamsDB(db_path)
    content = db.get_all_content()
    db.close()
    return content


def print_database_content(db_path: str = 'data/regime_params.db'):
    """
    打印数据库所有内容到控制台

    Args:
        db_path: 数据库路径
    """
    content = show_database_content(db_path)

    print("="*80)
    print("REGIME_PARAMS.DATABASE - 完整内容")
    print("="*80)

    # 1. Best Params (已优化股票)
    print("\n【1】已优化股票 (best_params)")
    print("-"*80)
    if content['best_params']:
        for i, item in enumerate(content['best_params'], 1):
            print(f"\n  {i}. {item['stock_code']}")
            print(f"     优化得分: {item['score']:.4f}")
            print(f"     数据期间: {item['start_date']} ~ {item['end_date']}")
            print(f"     更新时间: {item['updated_at']}")

            print(f"\n     性能指标:")
            print(f"       总收益: {item['total_return']*100:.2f}%")
            print(f"       夏普比率: {item['sharpe_ratio']:.2f}")
            print(f"       最大回撤: {item['max_drawdown']*100:.2f}%")
            print(f"       胜率: {item['win_rate']*100:.1f}%")
            print(f"       交易次数: {item['total_trades']}")

            print(f"\n     最优参数:")
            regime = item['regime_params']
            print(f"       牛市: buy×{regime['bull']['buy_threshold_multiplier']} "
                  f"sell×{regime['bull']['sell_threshold_multiplier']} "
                  f"stop×{regime['bull']['stop_loss_multiplier']}")
            print(f"       熊市: buy×{regime['bear']['buy_threshold_multiplier']} "
                  f"sell×{regime['bear']['sell_threshold_multiplier']} "
                  f"stop×{regime['bear']['stop_loss_multiplier']}")
            print(f"       震荡: buy×{regime['neutral']['buy_threshold_multiplier']} "
                  f"sell×{regime['neutral']['sell_threshold_multiplier']} "
                  f"stop×{regime['neutral']['stop_loss_multiplier']}")

            print(f"     基础阈值:")
            strategy = item['strategy_params']
            print(f"       买入: {strategy.get('base_buy_threshold', 0)}%")
            print(f"       止盈: {strategy.get('base_sell_threshold', 0)}%")
            print(f"       止损: {strategy.get('base_stop_loss_threshold', 0)}%")
            print(f"       自适应: {'是' if strategy.get('enable_adaptive_thresholds') else '否'}")
    else:
        print("暂无数据")

    # 2. Default Strategy Params (默认策略参数)
    print("\n【2】默认策略参数 (default_strategy_params)")
    print("-"*80)
    if content['default_strategy_params']:
        for item in content['default_strategy_params']:
            print(f"\n策略名称: {item['strategy_name']}")
            print(f"  描述: {item['description'][:80]}...")
            print(f"  牛市: buy×{item['bull']['buy_multiplier']} sell×{item['bull']['sell_multiplier']} stop×{item['bull']['stop_multiplier']}")
            print(f"  熊市: buy×{item['bear']['buy_multiplier']} sell×{item['bear']['sell_multiplier']} stop×{item['bear']['stop_multiplier']}")
            print(f"  震荡: buy×{item['neutral']['buy_multiplier']} sell×{item['neutral']['sell_multiplier']} stop×{item['neutral']['stop_multiplier']}")
            print(f"  阈值: Bull>={item['bull_threshold']}, Bear<={item['bear_threshold']}")
            print(f"  更新: {item['updated_at']}")
    else:
        print("暂无数据")

    # 3. Statistics
    print("\n【3】数据库统计")
    print("-"*80)
    stats = content['statistics']
    print(f"已优化股票数: {stats['total_stocks']}")
    print(f"平均得分: {stats['avg_score']:.4f}")
    print(f"最高得分: {stats['max_score']:.4f}")
    print(f"最低得分: {stats['min_score']:.4f}")

    print("\n" + "="*80)
