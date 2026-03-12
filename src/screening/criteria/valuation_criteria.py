"""
估值涨跌幅筛选条件

根据估值引擎计算的公允价值与当前价格的差值百分比进行筛选
"""
import logging
from typing import Optional, Dict, List
import pandas as pd
from src.screening.base_criteria import BaseCriteria

logger = logging.getLogger(__name__)


class ValuationUpsideCriteria(BaseCriteria):
    """
    估值涨跌幅筛选条件

    估值涨跌幅 = (公允价值 - 当前价格) / 当前价格 × 100%
    - 正值表示低估（如 +20% 表示低估20%，有上涨空间）
    - 负值表示高估（如 -15% 表示高估15%，有下跌风险）
    """

    def __init__(
        self,
        min_upside: Optional[float] = None,
        max_upside: Optional[float] = None,
        min_confidence: float = 0.3,
        methods: Optional[List[str]] = None,
        db_path: str = None
    ):
        """
        Args:
            min_upside: 最小涨跌幅（百分比），如 20 表示至少低估 20%
            max_upside: 最大涨跌幅（百分比），如 50 表示最多低估 50%
            min_confidence: 最小置信度阈值（0-1），默认 0.3
            methods: 估值方法列表，支持 combined/peg/dcf（与估值页面一致）
            db_path: 数据库路径
        """
        self.min_upside = min_upside
        self.max_upside = max_upside
        self.min_confidence = min_confidence
        self.methods = methods or ['combined']
        self.db_path = db_path

    @property
    def cost(self) -> int:
        """估值计算成本很高，设置为高成本确保最后执行"""
        return 50

    def filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        应用估值涨跌幅筛选条件

        Args:
            df: 待筛选的 DataFrame，必须包含 'symbol' 列

        Returns:
            符合条件的 DataFrame，新增以下列：
            - valuation_fair_value: 公允价值
            - valuation_upside: 涨跌幅百分比
            - valuation_confidence: 估值置信度
            - valuation_rating: 估值评级
        """
        logger.debug(
            f"[ValuationUpsideCriteria] Starting filter, min_upside={self.min_upside}, "
            f"max_upside={self.max_upside}, min_confidence={self.min_confidence}, "
            f"methods={self.methods}, input size={len(df)}"
        )

        if df.empty:
            return df

        if 'symbol' not in df.columns:
            logger.warning("[ValuationUpsideCriteria] 'symbol' column not found")
            return df

        from src.valuation.engine.valuation_engine import ValuationEngine
        from src.valuation.models.relative_valuation import RelativeValuationModel

        engine = ValuationEngine(self.db_path or "data/tushare_data.db")

        # 根据方法注册模型（与估值页面保持一致）
        model_names = []
        for method in self.methods:
            if method == 'combined':
                for m in ['pe', 'pb', 'ps']:
                    try:
                        model = RelativeValuationModel(
                            method=m,
                            config={'db_path': self.db_path or "data/tushare_data.db"}
                        )
                        engine.register_model(model)
                    except Exception as e:
                        logger.warning(f"[ValuationUpsideCriteria] Failed to register model {m}: {e}")
                combined_model = RelativeValuationModel(
                    method='combined',
                    config={'db_path': self.db_path or "data/tushare_data.db"}
                )
                engine.register_model(combined_model)
                model_names.extend(['Relative_PE', 'Relative_PB', 'Relative_PS'])
            elif method == 'peg':
                try:
                    model = RelativeValuationModel(
                        method='peg',
                        config={'db_path': self.db_path or "data/tushare_data.db"}
                    )
                    engine.register_model(model)
                    model_names.append('Relative_PEG')
                except Exception as e:
                    logger.warning(f"[ValuationUpsideCriteria] Failed to register PEG model: {e}")
            elif method == 'dcf':
                try:
                    from src.valuation.models.absolute_valuation import DCFValuationModel
                    dcf_model = DCFValuationModel(
                        config={'db_path': self.db_path or "data/tushare_data.db"}
                    )
                    engine.register_model(dcf_model)
                    model_names.append('DCF')
                except Exception as e:
                    logger.warning(f"[ValuationUpsideCriteria] Failed to register DCF model: {e}")

        if not engine.models:
            logger.warning("[ValuationUpsideCriteria] No valuation models available")
            return df

        # 去重
        model_names = list(dict.fromkeys(model_names))

        # 逐股估值
        symbols = df['symbol'].tolist()
        logger.info(f"[ValuationUpsideCriteria] Calculating valuation for {len(symbols)} stocks...")

        valuation_results = []
        for i, symbol in enumerate(symbols):
            try:
                result = engine.value_stock(
                    symbol=symbol,
                    date=None,
                    methods=model_names,
                    combine_method='weighted'
                )
                valuation_results.append(result)
                if (i + 1) % 50 == 0:
                    logger.debug(f"[ValuationUpsideCriteria] Progress: {i + 1}/{len(symbols)}")
            except Exception as e:
                logger.warning(f"[ValuationUpsideCriteria] Failed to value {symbol}: {e}")
                valuation_results.append({'symbol': symbol, 'error': str(e)})

        valuation_map = {
            r['symbol']: {
                'fair_value': r.get('fair_value'),
                'upside_downside': r.get('upside_downside'),
                'confidence': r.get('confidence'),
                'rating': r.get('rating'),
            }
            for r in valuation_results
            if r.get('symbol') and 'error' not in r
        }

        logger.info(f"[ValuationUpsideCriteria] Valuation completed, {len(valuation_map)} valid results")

        result_df = df.copy()
        result_df['valuation_fair_value'] = result_df['symbol'].map(
            lambda s: valuation_map.get(s, {}).get('fair_value')
        )
        result_df['valuation_upside'] = result_df['symbol'].map(
            lambda s: valuation_map.get(s, {}).get('upside_downside')
        )
        result_df['valuation_confidence'] = result_df['symbol'].map(
            lambda s: valuation_map.get(s, {}).get('confidence')
        )
        result_df['valuation_rating'] = result_df['symbol'].map(
            lambda s: valuation_map.get(s, {}).get('rating')
        )

        # 过滤置信度
        if self.min_confidence > 0:
            before = len(result_df)
            result_df = result_df[
                result_df['valuation_confidence'].isna() |
                (result_df['valuation_confidence'] >= self.min_confidence)
            ]
            logger.debug(f"[ValuationUpsideCriteria] min_confidence filter: {before} -> {len(result_df)}")

        # 过滤涨跌幅下限
        if self.min_upside is not None:
            before = len(result_df)
            result_df = result_df[
                result_df['valuation_upside'].isna() |
                (result_df['valuation_upside'] >= self.min_upside)
            ]
            logger.debug(f"[ValuationUpsideCriteria] min_upside filter: {before} -> {len(result_df)}")

        # 过滤涨跌幅上限
        if self.max_upside is not None:
            before = len(result_df)
            result_df = result_df[
                result_df['valuation_upside'].isna() |
                (result_df['valuation_upside'] <= self.max_upside)
            ]
            logger.debug(f"[ValuationUpsideCriteria] max_upside filter: {before} -> {len(result_df)}")

        # 移除无有效估值的股票
        before = len(result_df)
        result_df = result_df[result_df['valuation_upside'].notna()]
        logger.debug(f"[ValuationUpsideCriteria] Remove no-valuation: {before} -> {len(result_df)}")

        logger.info(f"[ValuationUpsideCriteria] Filter done, input={len(df)}, output={len(result_df)}")
        return result_df

    def to_config(self) -> Dict:
        return {
            'type': 'ValuationUpside',
            'min_upside': self.min_upside,
            'max_upside': self.max_upside,
            'min_confidence': self.min_confidence,
            'methods': self.methods,
        }

    @classmethod
    def from_config(cls, config: Dict, db_path: str = None):
        return cls(
            min_upside=config.get('min_upside'),
            max_upside=config.get('max_upside'),
            min_confidence=config.get('min_confidence', 0.3),
            methods=config.get('methods', ['combined']),
            db_path=db_path or config.get('db_path'),
        )
