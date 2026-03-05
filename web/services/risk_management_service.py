"""
风险管理服务
"""
import os
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# 数据库路径
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'tushare_data.db')


# ============================================================================
# 数据类定义
# ============================================================================

@dataclass
class Position:
    """持仓信息"""
    symbol: str
    shares: int
    cost_price: float
    current_price: float
    stop_loss_base: float
    stop_loss_percent: float
    name: str = ''
    price_date: Optional[str] = None

    @property
    def stop_loss_price(self) -> float:
        """止损价"""
        return self.stop_loss_base * (1 - self.stop_loss_percent / 100)

    @property
    def risk_per_share(self) -> float:
        """每股风险"""
        return self.cost_price - self.stop_loss_price

    @property
    def current_potential_risk(self) -> float:
        """当前潜在风险：从当前价格到止损价的风险"""
        return max(0, self.current_price - self.stop_loss_price) * self.shares

    @property
    def total_risk(self) -> float:
        """初始风险：买入时设定的风险（用于兼容）"""
        return self.shares * (self.cost_price - self.stop_loss_price)

    @property
    def unrealized_loss(self) -> float:
        """未实现亏损（正数表示亏损）"""
        return max(0, self.cost_price - self.current_price) * self.shares

    @property
    def market_value(self) -> float:
        """市值"""
        return self.shares * self.current_price

    @property
    def profit_per_share(self) -> float:
        """每股盈亏"""
        return self.current_price - self.cost_price

    @property
    def total_profit(self) -> float:
        """总盈亏"""
        return self.shares * self.profit_per_share

    @property
    def locked_profit(self) -> float:
        """锁定利润"""
        return self.stop_loss_price - self.cost_price

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'symbol': self.symbol,
            'name': self.name,
            'shares': self.shares,
            'cost_price': self.cost_price,
            'current_price': self.current_price,
            'price_date': self.price_date,
            'stop_loss_base': self.stop_loss_base,
            'stop_loss_percent': self.stop_loss_percent,
            'stop_loss_price': round(self.stop_loss_price, 2),
            'risk_per_share': round(self.risk_per_share, 4),
            'total_risk': round(self.total_risk, 2),
            'current_potential_risk': round(self.current_potential_risk, 2),
            'unrealized_loss': round(self.unrealized_loss, 2),
            'market_value': round(self.market_value, 2),
            'profit_per_share': round(self.profit_per_share, 2),
            'total_profit': round(self.total_profit, 2),
            'locked_profit': round(self.locked_profit, 2)
        }


@dataclass
class Portfolio:
    """投资组合"""
    total_capital: float  # 当前资金
    initial_capital: float = None  # 初始资金
    positions: List[Position] = field(default_factory=list)
    max_total_risk_percent: float = 6.0
    max_single_risk_percent: float = 2.0

    def __post_init__(self):
        if self.initial_capital is None:
            self.initial_capital = self.total_capital

    @property
    def total_risk_budget(self) -> float:
        """总风险预算"""
        return self.initial_capital * (self.max_total_risk_percent / 100)

    @property
    def single_risk_budget(self) -> float:
        """单笔风险预算"""
        return self.initial_capital * (self.max_single_risk_percent / 100)

    @property
    def used_risk(self) -> float:
        """已用风险 = 已亏损部分 + 潜在风险

        已亏损部分 = 初始资金 - 当前资金（正数表示亏损）
        潜在风险 = Σ (当前价格 - 止损价) × 股数
        """
        # 1. 已亏损部分（正数表示亏损）
        unrealized_loss = max(0, self.initial_capital - self.total_capital)

        # 2. 潜在风险：每只股票从当前价格到止损价的风险
        potential_risk = sum(p.current_potential_risk for p in self.positions)

        return unrealized_loss + potential_risk

    @property
    def available_risk(self) -> float:
        """可用风险"""
        return max(0, self.total_risk_budget - self.used_risk)

    @property
    def positions_value(self) -> float:
        """持仓市值"""
        return sum(p.market_value for p in self.positions)

    @property
    def remaining_cash(self) -> float:
        """剩余资金"""
        return self.total_capital - self.positions_value

    def get_state(self) -> Dict[str, Any]:
        """获取投资组合状态"""
        risk_usage = self.used_risk / self.total_risk_budget * 100 if self.total_risk_budget > 0 else 0

        # 计算已亏损部分（用于显示）
        unrealized_loss = max(0, self.initial_capital - self.total_capital)
        # 计算潜在风险（用于显示）
        potential_risk = sum(p.current_potential_risk for p in self.positions)

        return {
            'initial_capital': round(self.initial_capital, 2),
            'total_capital': self.total_capital,
            'total_risk_budget': round(self.total_risk_budget, 2),
            'single_risk_budget': round(self.single_risk_budget, 2),
            'used_risk': round(self.used_risk, 2),
            'unrealized_loss': round(unrealized_loss, 2),
            'potential_risk': round(potential_risk, 2),
            'available_risk': round(self.available_risk, 2),
            'positions_value': round(self.positions_value, 2),
            'remaining_cash': round(self.remaining_cash, 2),
            'risk_usage_percent': round(risk_usage, 2),
            'positions': [p.to_dict() for p in self.positions]
        }


# ============================================================================
# 辅助函数
# ============================================================================

def create_position_from_dict(data: Dict[str, Any]) -> Position:
    """从字典创建Position对象"""
    return Position(
        symbol=data['symbol'],
        shares=data['shares'],
        cost_price=data['cost_price'],
        current_price=data['current_price'],
        stop_loss_base=data['stop_loss_base'],
        stop_loss_percent=data['stop_loss_percent'],
        name=data.get('name', ''),
        price_date=data.get('price_date')
    )


def create_portfolio_from_dict(data: Dict[str, Any]) -> Portfolio:
    """从字典创建Portfolio对象"""
    positions = [create_position_from_dict(p) for p in data.get('positions', [])]
    return Portfolio(
        total_capital=data['total_capital'],
        initial_capital=data.get('initial_capital'),
        positions=positions,
        max_total_risk_percent=data.get('max_total_risk_percent', 6.0),
        max_single_risk_percent=data.get('max_single_risk_percent', 2.0)
    )


def get_portfolio_state(portfolio: Portfolio) -> Dict[str, Any]:
    """获取投资组合状态"""
    return portfolio.get_state()


# ============================================================================
# 业务逻辑函数
# ============================================================================

def calculate_new_position(
    portfolio_data: Dict[str, Any],
    buy_price: float,
    stop_loss_percent: float
) -> Dict[str, Any]:
    """计算新股可买股数"""
    portfolio = create_portfolio_from_dict(portfolio_data)

    if buy_price <= 0:
        return {'success': False, 'error': '买入价格必须大于0'}

    if stop_loss_percent <= 0:
        return {'success': False, 'error': '止损比例必须大于0'}

    loss_per_share = buy_price * (stop_loss_percent / 100)

    # 按总风险限制计算
    max_by_total_risk = int(portfolio.available_risk / loss_per_share) if loss_per_share > 0 else 0

    # 按单笔风险限制计算
    max_by_single_risk = int(portfolio.single_risk_budget / loss_per_share) if loss_per_share > 0 else 0

    # 按剩余现金计算
    max_by_cash = int(portfolio.remaining_cash / buy_price) if buy_price > 0 else 0

    # 取最小值
    max_shares = max(0, min(max_by_total_risk, max_by_single_risk, max_by_cash))

    required_capital = max_shares * buy_price
    max_loss = max_shares * loss_per_share

    # 限制因素
    limiting_factors = []
    if max_shares == max_by_total_risk:
        limiting_factors.append('总风险限制')
    if max_shares == max_by_single_risk:
        limiting_factors.append('单笔风险限制')
    if max_shares == max_by_cash:
        limiting_factors.append('剩余现金不足')

    # 模拟买入后的状态
    new_total_risk = portfolio.used_risk + max_loss
    new_risk_usage = new_total_risk / portfolio.total_risk_budget * 100 if portfolio.total_risk_budget > 0 else 0
    new_positions_value = portfolio.positions_value + required_capital
    new_remaining_cash = portfolio.remaining_cash - required_capital

    return {
        'success': True,
        'portfolio': portfolio.get_state(),
        'new_position': {
            'buy_price': buy_price,
            'stop_loss_percent': stop_loss_percent,
            'loss_per_share': round(loss_per_share, 4),
            'max_by_total_risk': max_by_total_risk,
            'max_by_single_risk': max_by_single_risk,
            'max_by_cash': max_by_cash,
            'max_shares': max_shares,
            'required_capital': round(required_capital, 2),
            'max_loss': round(max_loss, 2),
            'limiting_factors': limiting_factors,
            'limiting_factor_names': limiting_factors,
            'after_buy': {
                'new_total_risk': round(new_total_risk, 2),
                'new_risk_usage': round(new_risk_usage, 2),
                'new_positions_value': round(new_positions_value, 2),
                'new_remaining_cash': round(new_remaining_cash, 2)
            }
        }
    }


def sell_position(
    portfolio_data: Dict[str, Any],
    symbol: str,
    sell_shares: int,
    sell_price: float
) -> Dict[str, Any]:
    """卖出股票"""
    portfolio = create_portfolio_from_dict(portfolio_data)

    position = next((p for p in portfolio.positions if p.symbol == symbol), None)
    if not position:
        return {'success': False, 'error': f'未找到持仓: {symbol}'}

    if sell_shares > position.shares:
        return {'success': False, 'error': '卖出数量超过持仓数量'}

    sell_value = sell_shares * sell_price
    realized_profit = sell_shares * (sell_price - position.cost_price)
    released_risk = sell_shares * position.risk_per_share

    if sell_shares == position.shares:
        portfolio.positions.remove(position)
        remaining_shares = 0
    else:
        position.shares -= sell_shares
        remaining_shares = position.shares

    return {
        'success': True,
        'sell_info': {
            'symbol': symbol,
            'sell_shares': sell_shares,
            'sell_price': sell_price,
            'sell_value': round(sell_value, 2),
            'realized_profit': round(realized_profit, 2),
            'released_risk': round(released_risk, 2)
        },
        'remaining_shares': remaining_shares,
        'portfolio': portfolio.get_state()
    }


def adjust_stop_loss(
    position_data: Dict[str, Any],
    new_stop_loss_base: float,
    new_stop_loss_percent: float
) -> Dict[str, Any]:
    """调整止损参数"""
    position = create_position_from_dict(position_data)

    old_risk = position.total_risk
    old_stop_loss_price = position.stop_loss_price

    position.stop_loss_base = new_stop_loss_base
    position.stop_loss_percent = new_stop_loss_percent

    new_risk = position.total_risk
    released_risk = old_risk - new_risk

    return {
        'success': True,
        'adjustment': {
            'old_risk': round(old_risk, 2),
            'new_risk': round(new_risk, 2),
            'released_risk': round(released_risk, 2),
            'old_stop_loss_price': round(old_stop_loss_price, 2),
            'new_stop_loss_price': round(position.stop_loss_price, 2),
            'locked_profit': round(position.locked_profit, 2)
        },
        'position': position.to_dict()
    }


def add_position(
    portfolio_data: Dict[str, Any],
    symbol: str,
    shares: int,
    cost_price: float,
    current_price: float,
    stop_loss_percent: float
) -> Dict[str, Any]:
    """添加新持仓"""
    portfolio = create_portfolio_from_dict(portfolio_data)

    # 检查是否已存在
    existing = next((p for p in portfolio.positions if p.symbol == symbol), None)
    if existing:
        return {'success': False, 'error': f'持仓已存在: {symbol}'}

    new_position = Position(
        symbol=symbol,
        shares=shares,
        cost_price=cost_price,
        current_price=current_price,
        stop_loss_base=cost_price,
        stop_loss_percent=stop_loss_percent
    )

    portfolio.positions.append(new_position)

    return {
        'success': True,
        'position': new_position.to_dict(),
        'portfolio': portfolio.get_state()
    }


# ============================================================================
# 数据库持久化函数
# ============================================================================

def get_db_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_risk_tables(conn=None):
    """初始化风险管理数据库表"""
    close_conn = False
    if conn is None:
        conn = get_db_connection()
        close_conn = True

    try:
        cursor = conn.cursor()

        # 投资组合设置表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS risk_portfolio (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_capital REAL NOT NULL DEFAULT 0,
                max_total_risk_percent REAL NOT NULL DEFAULT 6.0,
                max_single_risk_percent REAL NOT NULL DEFAULT 2.0,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        ''')

        # 检查并添加新字段
        cursor.execute("PRAGMA table_info(risk_portfolio)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'initial_capital' not in columns:
            cursor.execute('ALTER TABLE risk_portfolio ADD COLUMN initial_capital REAL NOT NULL DEFAULT 650000')
        if 'cumulative_pnl' not in columns:
            cursor.execute('ALTER TABLE risk_portfolio ADD COLUMN cumulative_pnl REAL NOT NULL DEFAULT 0')
        if 'cash' not in columns:
            cursor.execute('ALTER TABLE risk_portfolio ADD COLUMN cash REAL NOT NULL DEFAULT 0')

        # 持仓表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS risk_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE,
                shares INTEGER NOT NULL,
                cost_price REAL NOT NULL,
                stop_loss_base REAL NOT NULL,
                stop_loss_percent REAL NOT NULL DEFAULT 8.0,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        ''')

        # 月度快照表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS risk_monthly_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                year_month TEXT NOT NULL UNIQUE,
                month_start_capital REAL NOT NULL,
                month_start_positions_value REAL NOT NULL,
                month_end_capital REAL NOT NULL,
                month_end_positions_value REAL NOT NULL,
                month_end_cash REAL NOT NULL,
                month_pnl REAL NOT NULL DEFAULT 0,
                month_pnl_percent REAL NOT NULL DEFAULT 0,
                capital_change REAL NOT NULL DEFAULT 0,
                capital_change_reason TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        ''')

        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_risk_positions_symbol ON risk_positions(symbol)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_risk_monthly_snapshots_year_month ON risk_monthly_snapshots(year_month)')

        conn.commit()
    finally:
        if close_conn:
            conn.close()


def is_hk_stock(symbol: str) -> bool:
    """判断是否为港股"""
    if not symbol:
        return False
    symbol = symbol.strip()
    if len(symbol) == 6 and symbol.isdigit() and symbol[0] in ('0', '3', '6'):
        return False
    return True


def get_current_price(symbol: str) -> Optional[float]:
    """从 DuckDB 行情表获取最新收盘价"""
    result = get_current_price_with_date(symbol)
    return result['price'] if result else None


def get_current_price_with_date(symbol: str) -> Optional[Dict[str, Any]]:
    """从 DuckDB 行情表获取最新收盘价和日期"""
    try:
        from src.db.duckdb_manager import get_duckdb_manager

        db_manager = get_duckdb_manager(read_only=True)

        with db_manager.get_connection() as conn:
            table_name = 'bars_1d' if is_hk_stock(symbol) else 'bars_a_1d'

            if not db_manager.table_exists(table_name):
                logger.warning(f"Table {table_name} does not exist")
                return None

            query = f'''
                SELECT close, datetime FROM {table_name}
                WHERE stock_code = ?
                ORDER BY datetime DESC
                LIMIT 1
            '''
            result = conn.execute(query, [symbol]).fetchone()

            if result and result[0]:
                date_val = result[1]
                if date_val:
                    if hasattr(date_val, 'strftime'):
                        price_date = date_val.strftime('%Y-%m-%d')
                    elif isinstance(date_val, str):
                        price_date = date_val[:10] if len(date_val) >= 10 else date_val
                    else:
                        price_date = str(date_val)
                else:
                    price_date = None
                return {
                    'price': float(result[0]),
                    'date': price_date
                }

            return None
    except Exception as e:
        logger.error(f"Error getting current price for {symbol}: {e}")
        return None


def get_stock_name(symbol: str) -> str:
    """从 stock_names 表获取股票名称"""
    try:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT name FROM stock_names WHERE code = ?', (symbol,))
            row = cursor.fetchone()
            return row['name'] if row else symbol
        finally:
            conn.close()
    except Exception:
        return symbol


def load_portfolio_settings() -> Dict[str, Any]:
    """加载投资组合设置"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT total_capital, initial_capital, max_total_risk_percent, max_single_risk_percent, updated_at
            FROM risk_portfolio WHERE id = 1
        ''')
        row = cursor.fetchone()

        if row:
            total_capital = row['total_capital'] or 650000
            initial_capital = row['initial_capital'] or total_capital
            return {
                'total_capital': total_capital,
                'initial_capital': initial_capital,
                'max_total_risk_percent': row['max_total_risk_percent'] or 6.0,
                'max_single_risk_percent': row['max_single_risk_percent'] or 2.0,
                'updated_at': row['updated_at']
            }

        # 创建默认设置
        cursor.execute('''
            INSERT INTO risk_portfolio (id, total_capital, initial_capital)
            VALUES (1, 650000, 650000)
        ''')
        conn.commit()

        return {
            'total_capital': 650000,
            'initial_capital': 650000,
            'max_total_risk_percent': 6.0,
            'max_single_risk_percent': 2.0
        }
    finally:
        conn.close()


def save_portfolio_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    """保存投资组合设置"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        total_capital = data.get('total_capital', 650000)

        cursor.execute('SELECT id, initial_capital FROM risk_portfolio WHERE id = 1')
        row = cursor.fetchone()

        if row:
            # 如果 initial_capital 还是默认值（等于旧的 total_capital），则同步更新
            old_initial = row['initial_capital'] if row['initial_capital'] else 650000
            update_initial = data.get('update_initial_capital', False)

            if update_initial or old_initial == 650000:
                # 同步更新 initial_capital
                cursor.execute('''
                    UPDATE risk_portfolio
                    SET total_capital = ?,
                        initial_capital = ?,
                        max_total_risk_percent = ?,
                        max_single_risk_percent = ?,
                        updated_at = ?
                    WHERE id = 1
                ''', (
                    total_capital,
                    total_capital,
                    data.get('max_total_risk_percent', 6.0),
                    data.get('max_single_risk_percent', 2.0),
                    now
                ))
            else:
                cursor.execute('''
                    UPDATE risk_portfolio
                    SET total_capital = ?,
                        max_total_risk_percent = ?,
                        max_single_risk_percent = ?,
                        updated_at = ?
                    WHERE id = 1
                ''', (
                    total_capital,
                    data.get('max_total_risk_percent', 6.0),
                    data.get('max_single_risk_percent', 2.0),
                    now
                ))
        else:
            cursor.execute('''
                INSERT INTO risk_portfolio (id, total_capital, initial_capital, max_total_risk_percent, max_single_risk_percent, updated_at)
                VALUES (1, ?, ?, ?, ?, ?)
            ''', (
                total_capital,
                total_capital,
                data.get('max_total_risk_percent', 6.0),
                data.get('max_single_risk_percent', 2.0),
                now
            ))

        conn.commit()

        return {
            'total_capital': total_capital,
            'max_total_risk_percent': data.get('max_total_risk_percent', 6.0),
            'max_single_risk_percent': data.get('max_single_risk_percent', 2.0),
            'updated_at': now
        }
    finally:
        conn.close()


def load_positions() -> List[Dict[str, Any]]:
    """加载所有持仓（含实时价格和股票名称）"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT symbol, shares, cost_price, stop_loss_base, stop_loss_percent, created_at, updated_at
            FROM risk_positions
            ORDER BY created_at DESC
        ''')
        rows = cursor.fetchall()

        positions = []
        for row in rows:
            symbol = row['symbol']

            price_info = get_current_price_with_date(symbol)
            if price_info:
                current_price = price_info['price']
                price_date = price_info['date']
            else:
                current_price = float(row['cost_price'])
                price_date = None

            name = get_stock_name(symbol)

            positions.append({
                'symbol': symbol,
                'name': name,
                'shares': int(row['shares']),
                'cost_price': float(row['cost_price']),
                'current_price': current_price,
                'price_date': price_date,
                'stop_loss_base': float(row['stop_loss_base']),
                'stop_loss_percent': float(row['stop_loss_percent']),
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            })

        return positions
    finally:
        conn.close()


def add_position_to_db(data: Dict[str, Any]) -> Dict[str, Any]:
    """添加持仓到数据库"""
    init_risk_tables()

    symbol = data['symbol']

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute('SELECT id FROM risk_positions WHERE symbol = ?', (symbol,))
        if cursor.fetchone():
            return {'success': False, 'error': f'持仓已存在: {symbol}'}

        cursor.execute('''
            INSERT INTO risk_positions (symbol, shares, cost_price, stop_loss_base, stop_loss_percent, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            symbol,
            data['shares'],
            data['cost_price'],
            data.get('stop_loss_base', data['cost_price']),
            data.get('stop_loss_percent', 8.0),
            now,
            now
        ))

        conn.commit()

        price_info = get_current_price_with_date(symbol)
        current_price = price_info['price'] if price_info else data['cost_price']
        price_date = price_info['date'] if price_info else None
        name = get_stock_name(symbol)

        return {
            'success': True,
            'position': {
                'symbol': symbol,
                'name': name,
                'shares': data['shares'],
                'cost_price': data['cost_price'],
                'current_price': current_price,
                'price_date': price_date,
                'stop_loss_base': data.get('stop_loss_base', data['cost_price']),
                'stop_loss_percent': data.get('stop_loss_percent', 8.0),
                'created_at': now,
                'updated_at': now
            }
        }
    finally:
        conn.close()


def update_position_in_db(symbol: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """更新持仓"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor.execute('SELECT id FROM risk_positions WHERE symbol = ?', (symbol,))
        if not cursor.fetchone():
            return {'success': False, 'error': f'持仓不存在: {symbol}'}

        updates = []
        params = []

        if 'shares' in data:
            updates.append('shares = ?')
            params.append(data['shares'])
        if 'cost_price' in data:
            updates.append('cost_price = ?')
            params.append(data['cost_price'])
        if 'stop_loss_base' in data:
            updates.append('stop_loss_base = ?')
            params.append(data['stop_loss_base'])
        if 'stop_loss_percent' in data:
            updates.append('stop_loss_percent = ?')
            params.append(data['stop_loss_percent'])

        if updates:
            updates.append('updated_at = ?')
            params.append(now)
            params.append(symbol)

            cursor.execute(f'''
                UPDATE risk_positions
                SET {', '.join(updates)}
                WHERE symbol = ?
            ''', params)
            conn.commit()

        return {'success': True, 'updated_at': now}
    finally:
        conn.close()


def delete_position_from_db(symbol: str) -> Dict[str, Any]:
    """删除持仓"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('DELETE FROM risk_positions WHERE symbol = ?', (symbol,))
        conn.commit()

        if cursor.rowcount > 0:
            return {'success': True}
        else:
            return {'success': False, 'error': f'持仓不存在: {symbol}'}
    finally:
        conn.close()


def load_full_portfolio() -> Dict[str, Any]:
    """加载完整投资组合"""
    settings = load_portfolio_settings()
    positions = load_positions()

    return {
        'total_capital': settings['total_capital'],
        'initial_capital': settings.get('initial_capital', settings['total_capital']),
        'max_total_risk_percent': settings['max_total_risk_percent'],
        'max_single_risk_percent': settings['max_single_risk_percent'],
        'positions': positions,
        'updated_at': settings.get('updated_at')
    }


# ============================================================================
# 资金管理函数
# ============================================================================

def get_capital_state() -> Dict[str, Any]:
    """获取当前资金状态"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('''
            SELECT total_capital, initial_capital, cumulative_pnl, cash
            FROM risk_portfolio WHERE id = 1
        ''')
        row = cursor.fetchone()

        if not row:
            return {
                'initial_capital': 650000,
                'cumulative_pnl': 0,
                'current_capital': 650000,
                'positions_value': 0,
                'cash': 650000,
                'floating_pnl': 0,
            }

        total_capital = row['total_capital'] or 650000
        initial_capital = row['initial_capital'] or total_capital
        cumulative_pnl = row['cumulative_pnl'] or 0
        cash = row['cash'] if row['cash'] is not None else initial_capital

        cursor.execute('SELECT symbol, shares, cost_price FROM risk_positions')
        positions = cursor.fetchall()

        positions_value = 0
        floating_pnl = 0

        for pos in positions:
            current_price = get_current_price(pos['symbol'])
            if current_price:
                mv = pos['shares'] * current_price
                positions_value += mv

        current_capital = positions_value + cash

        # 浮动盈亏 = 当前资金 - 初始资金
        floating_pnl = current_capital - initial_capital

        return {
            'initial_capital': round(initial_capital, 2),
            'cumulative_pnl': round(cumulative_pnl, 2),
            'current_capital': round(current_capital, 2),
            'positions_value': round(positions_value, 2),
            'cash': round(cash, 2),
            'floating_pnl': round(floating_pnl, 2),
        }
    finally:
        conn.close()


def adjust_initial_capital(new_initial_capital: float, reason: str = '') -> Dict[str, Any]:
    """调整初始资金"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('SELECT initial_capital, cash FROM risk_portfolio WHERE id = 1')
        row = cursor.fetchone()

        if not row:
            cursor.execute('''
                INSERT INTO risk_portfolio (id, total_capital, initial_capital, cumulative_pnl, cash)
                VALUES (1, ?, ?, 0, ?)
            ''', (new_initial_capital, new_initial_capital, new_initial_capital))
            conn.commit()
            return {
                'success': True,
                'old_initial_capital': 0,
                'new_initial_capital': new_initial_capital,
                'change': new_initial_capital,
            }

        old_initial_capital = row['initial_capital'] or 0
        old_cash = row['cash'] if row['cash'] is not None else old_initial_capital
        change = new_initial_capital - old_initial_capital
        new_cash = old_cash + change

        cursor.execute('''
            UPDATE risk_portfolio
            SET initial_capital = ?,
                total_capital = ?,
                cash = ?,
                updated_at = datetime('now')
            WHERE id = 1
        ''', (new_initial_capital, new_initial_capital, new_cash))

        conn.commit()

        return {
            'success': True,
            'old_initial_capital': old_initial_capital,
            'new_initial_capital': new_initial_capital,
            'change': change,
            'old_cash': old_cash,
            'new_cash': new_cash,
            'reason': reason,
        }
    finally:
        conn.close()


def update_cash(new_cash: float) -> Dict[str, Any]:
    """更新剩余现金"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('SELECT cash FROM risk_portfolio WHERE id = 1')
        row = cursor.fetchone()
        old_cash = row['cash'] if row and row['cash'] is not None else 0

        cursor.execute('''
            UPDATE risk_portfolio
            SET cash = ?,
                updated_at = datetime('now')
            WHERE id = 1
        ''', (new_cash,))

        conn.commit()

        logger.info(f"Updated cash: {old_cash} -> {new_cash}")

        return get_capital_state()
    except Exception as e:
        logger.error(f"Error updating cash: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()


def add_realized_pnl(realized_profit: float, sell_value: float, symbol: str = '') -> Dict[str, Any]:
    """添加已实现盈亏"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('SELECT cumulative_pnl, cash FROM risk_portfolio WHERE id = 1')
        row = cursor.fetchone()

        old_cumulative_pnl = row['cumulative_pnl'] if row and row['cumulative_pnl'] else 0
        old_cash = row['cash'] if row and row['cash'] is not None else 0

        new_cumulative_pnl = old_cumulative_pnl + realized_profit
        new_cash = old_cash + sell_value

        cursor.execute('''
            UPDATE risk_portfolio
            SET cumulative_pnl = ?,
                cash = ?,
                updated_at = datetime('now')
            WHERE id = 1
        ''', (new_cumulative_pnl, new_cash))

        conn.commit()

        logger.info(f"Added realized PnL: {realized_profit}, sell_value: {sell_value} from {symbol}")

        return {
            'success': True,
            'realized_profit': realized_profit,
            'sell_value': sell_value,
            'old_cumulative_pnl': old_cumulative_pnl,
            'new_cumulative_pnl': new_cumulative_pnl,
            'old_cash': old_cash,
            'new_cash': new_cash,
            'symbol': symbol
        }
    except Exception as e:
        logger.error(f"Error adding realized PnL: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        conn.close()


# ============================================================================
# 月度快照函数
# ============================================================================

def get_current_year_month() -> str:
    """获取当前年月"""
    return datetime.now().strftime('%Y-%m')


def create_monthly_snapshot(year_month: str = None) -> Dict[str, Any]:
    """创建月度快照"""
    init_risk_tables()

    if year_month is None:
        year_month = get_current_year_month()

    capital_state = get_capital_state()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM risk_monthly_snapshots WHERE year_month = ?', (year_month,))
        existing = cursor.fetchone()

        if existing:
            # 更新现有快照，计算本月盈亏
            month_start_capital = existing['month_start_capital']
            month_pnl = capital_state['current_capital'] - month_start_capital
            month_pnl_percent = (month_pnl / month_start_capital * 100) if month_start_capital > 0 else 0

            cursor.execute('''
                UPDATE risk_monthly_snapshots
                SET month_end_capital = ?,
                    month_end_positions_value = ?,
                    month_end_cash = ?,
                    month_pnl = ?,
                    month_pnl_percent = ?,
                    updated_at = datetime('now')
                WHERE year_month = ?
            ''', (
                capital_state['current_capital'],
                capital_state['positions_value'],
                capital_state['cash'],
                month_pnl,
                month_pnl_percent,
                year_month
            ))
            conn.commit()

            cursor.execute('SELECT * FROM risk_monthly_snapshots WHERE year_month = ?', (year_month,))
            return dict(cursor.fetchone())
        else:
            # 创建新快照，本月盈亏为0（因为是月初）
            cursor.execute('''
                INSERT INTO risk_monthly_snapshots (
                    year_month,
                    month_start_capital,
                    month_start_positions_value,
                    month_end_capital,
                    month_end_positions_value,
                    month_end_cash,
                    month_pnl,
                    month_pnl_percent
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 0)
            ''', (
                year_month,
                capital_state['current_capital'],  # 月初资金 = 当前资金
                capital_state['positions_value'],
                capital_state['current_capital'],
                capital_state['positions_value'],
                capital_state['cash']
            ))
            conn.commit()

            cursor.execute('SELECT * FROM risk_monthly_snapshots WHERE year_month = ?', (year_month,))
            return dict(cursor.fetchone())
    finally:
        conn.close()


def get_monthly_snapshots(
    start_month: str = None,
    end_month: str = None,
    limit: int = 12
) -> List[Dict[str, Any]]:
    """获取月度快照列表"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        query = 'SELECT * FROM risk_monthly_snapshots'
        params = []
        conditions = []

        if start_month:
            conditions.append('year_month >= ?')
            params.append(start_month)
        if end_month:
            conditions.append('year_month <= ?')
            params.append(end_month)

        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)

        query += ' ORDER BY year_month DESC LIMIT ?'
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        return [dict(row) for row in rows]
    finally:
        conn.close()


def update_monthly_capital_change(
    year_month: str,
    amount: float,
    reason: str = ''
) -> Dict[str, Any]:
    """更新某月的资金追加/取出记录"""
    init_risk_tables()

    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM risk_monthly_snapshots WHERE year_month = ?', (year_month,))
        if not cursor.fetchone():
            return {'success': False, 'error': f'快照不存在: {year_month}'}

        cursor.execute('''
            UPDATE risk_monthly_snapshots
            SET capital_change = ?,
                capital_change_reason = ?,
                updated_at = datetime('now')
            WHERE year_month = ?
        ''', (amount, reason, year_month))

        conn.commit()

        cursor.execute('SELECT * FROM risk_monthly_snapshots WHERE year_month = ?', (year_month,))
        return dict(cursor.fetchone())
    finally:
        conn.close()
