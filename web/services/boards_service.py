"""
Board data business logic services
"""
from flask import jsonify
from config.settings import BOARD_DB_PATH, TUSHARE_TOKEN, TUSHARE_DB_PATH
import pandas as pd


def get_boards_list():
    """获取所有板块列表"""
    from src.data_sources.board import BoardDB

    db = BoardDB(db_path=str(BOARD_DB_PATH))

    # 获取所有板块
    query = """
    SELECT board_code, board_name, board_type, source, updated_at
    FROM board_names
    ORDER BY board_name
    """
    boards_df = pd.read_sql_query(query, db.engine)

    # 为每个板块添加成分股数量
    for idx, row in boards_df.iterrows():
        count_query = "SELECT COUNT(*) as cnt FROM board_cons WHERE board_code = :board_code"
        with db.engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text(count_query), {"board_code": row['board_code']}).fetchone()
            boards_df.at[idx, 'stock_count'] = result[0] if result else 0

    return boards_df.to_dict('records'), 200


def get_board_constituents(board_code):
    """获取指定板块的成分股（包含最新估值指标）- 使用快照表"""
    from src.data_sources.board import BoardDB
    from src.data_sources.tushare import TushareDB

    board_db = BoardDB(db_path=str(BOARD_DB_PATH))

    query = """
    SELECT bc.stock_code, bc.stock_name, bn.board_name, bn.board_code
    FROM board_cons bc
    JOIN board_names bn ON bc.board_code = bn.board_code
    WHERE bc.board_code = :board_code
    ORDER BY bc.stock_code
    """

    constituents_df = pd.read_sql_query(
        query, board_db.engine,
        params={"board_code": board_code}
    )

    if constituents_df.empty:
        return [], 200

    # 使用快照表获取估值数据（高效方式）
    try:
        tushare_db = TushareDB(token=TUSHARE_TOKEN, db_path=str(TUSHARE_DB_PATH))

        # 从快照表中获取估值数据
        # 尝试匹配多种代码格式
        stock_codes_list = constituents_df['stock_code'].tolist()
        placeholders = ','.join([f':code{i}' for i in range(len(stock_codes_list))])

        snapshot_query = f"""
        SELECT
            stock_code,
            datetime,
            close,
            pe,
            pe_ttm,
            pb,
            ps,
            ps_ttm,
            total_mv_yi,
            circ_mv_yi
        FROM stock_valuation_snapshot
        WHERE stock_code IN ({placeholders})
        """

        params = {f'code{i}': code for i, code in enumerate(stock_codes_list)}

        with tushare_db.engine.connect() as conn:
            valuation_df = pd.read_sql_query(snapshot_query, conn, params=params)

        # 为每只股票匹配估值数据
        constituents_list = []
        for _, row in constituents_df.iterrows():
            stock_code = row['stock_code']

            # 在估值数据中查找匹配
            match = valuation_df[valuation_df['stock_code'] == stock_code]
            valuation_row = match.iloc[0] if not match.empty else None

            stock_data = {
                'stock_code': row['stock_code'],
                'stock_name': row['stock_name'],
                'board_name': row['board_name'],
                'board_code': row['board_code']
            }

            if valuation_row is not None:
                stock_data['datetime'] = str(valuation_row.get('datetime')) if pd.notna(valuation_row.get('datetime')) else None
                stock_data['close'] = float(valuation_row.get('close')) if pd.notna(valuation_row.get('close')) else None
                stock_data['pe'] = float(valuation_row.get('pe')) if pd.notna(valuation_row.get('pe')) else None
                stock_data['pe_ttm'] = float(valuation_row.get('pe_ttm')) if pd.notna(valuation_row.get('pe_ttm')) else None
                stock_data['pb'] = float(valuation_row.get('pb')) if pd.notna(valuation_row.get('pb')) else None
                stock_data['ps'] = float(valuation_row.get('ps')) if pd.notna(valuation_row.get('ps')) else None
                stock_data['ps_ttm'] = float(valuation_row.get('ps_ttm')) if pd.notna(valuation_row.get('ps_ttm')) else None
                stock_data['total_mv_yi'] = float(valuation_row.get('total_mv_yi')) if pd.notna(valuation_row.get('total_mv_yi')) else None
                stock_data['circ_mv_yi'] = float(valuation_row.get('circ_mv_yi')) if pd.notna(valuation_row.get('circ_mv_yi')) else None
            else:
                stock_data['datetime'] = None
                stock_data['close'] = None
                stock_data['pe'] = None
                stock_data['pe_ttm'] = None
                stock_data['pb'] = None
                stock_data['ps'] = None
                stock_data['ps_ttm'] = None
                stock_data['total_mv_yi'] = None
                stock_data['circ_mv_yi'] = None

            constituents_list.append(stock_data)

        return constituents_list, 200

    except Exception as e:
        import traceback
        import logging
        logging.getLogger(__name__).warning(f"Failed to get valuation data from snapshot, returning basic info: {e}")
        traceback.print_exc()
        # 降级：返回基本成分股信息
        return constituents_df.to_dict('records'), 200


def get_stock_boards(stock_code):
    """获取指定股票所属的所有板块"""
    from src.data_sources.board import BoardDB

    db = BoardDB(db_path=str(BOARD_DB_PATH))

    query = """
    SELECT bn.board_code, bn.board_name, bc.stock_code, bc.stock_name
    FROM board_cons bc
    JOIN board_names bn ON bc.board_code = bn.board_code
    WHERE bc.stock_code = :stock_code
    ORDER BY bn.board_name
    """

    boards_df = pd.read_sql_query(
        query, db.engine,
        params={"stock_code": stock_code}
    )

    return boards_df.to_dict('records'), 200
