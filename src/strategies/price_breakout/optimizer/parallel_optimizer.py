"""
Parallel optimizer using multiprocessing pool for true multi-core performance.

Implements hierarchical optimization (coarse search → fine search) with
multi-core parallelization using Python's multiprocessing module.
"""

import multiprocessing as mp
import os
import sys
import logging
import time
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
from tqdm import tqdm

# Add project root to path for imports in worker processes
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Environment variable to control backtest verbosity (quiet mode for optimization)
os.environ['BACKTEST_QUIET_MODE'] = '1'


class ProgressTracker:
    """Track and log progress during optimization phases."""

    def __init__(self, total: int, logger: logging.Logger, phase_name: str,
                 log_interval: float = 30.0):
        """
        Initialize progress tracker.

        Args:
            total: Total number of items to process
            logger: Logger instance for writing progress
            phase_name: Name of the phase (e.g., "Coarse Search")
            log_interval: Seconds between progress logs (default: 30)
        """
        self.total = total
        self.logger = logger
        self.phase_name = phase_name
        self.log_interval = log_interval
        self.completed = 0
        self.last_log_time = time.time()
        self.start_time = time.time()

    def update(self, n: int = 1):
        """
        Update progress by n items.

        Args:
            n: Number of items completed (default: 1)
        """
        self.completed += n
        current_time = time.time()

        # Log progress at regular intervals
        if current_time - self.last_log_time >= self.log_interval:
            self._log_progress()
            self.last_log_time = current_time

    def finish(self):
        """Log final progress summary."""
        self._log_progress()

    def _log_progress(self):
        """Log current progress to file."""
        elapsed = time.time() - self.start_time
        pct = (self.completed / self.total * 100) if self.total > 0 else 0

        # Calculate items per second
        rate = self.completed / elapsed if elapsed > 0 else 0

        # Estimate remaining time
        if rate > 0:
            remaining = (self.total - self.completed) / rate
            remaining_str = f"{remaining:.0f}s"
        else:
            remaining_str = "unknown"

        self.logger.info(
            f"{self.phase_name} Progress: {self.completed}/{self.total} "
            f"({pct:.1f}%) - {rate:.1f} items/s - ETA: {remaining_str}"
        )


def setup_optimizer_logger(stock_code: str) -> logging.Logger:
    """Set up logger for optimization process."""
    log_dir = Path("logs/optimization")
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"{stock_code.replace('.', '_')}_{timestamp}.log"

    logger = logging.getLogger(f"optimizer.{stock_code}")
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Force flush for file handler (write logs immediately)
    file_handler.flush = lambda: None  # Will be set below

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info(f"Log file: {log_file}")

    return logger


class ParallelRegimeOptimizer:
    """
    Parallel parameter optimizer using multiprocessing pool.

    Uses hierarchical search:
    1. Coarse search: Fast exploration with large step sizes
    2. Fine search: Local optimization around best coarse parameters
    """

    def __init__(self, stock_code: str, start_date: str, end_date: str,
                 n_workers: Optional[int] = None,
                 base_params: Optional[Dict[str, Any]] = None,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize optimizer.

        Args:
            stock_code: Stock symbol to optimize
            start_date: Backtest start date (YYYY-MM-DD)
            end_date: Backtest end date (YYYY-MM-DD)
            n_workers: Number of parallel workers (default: cpu_count - 1)
            base_params: Additional base parameters for backtest
            logger: Logger instance (will create if None)
        """
        self.stock_code = stock_code
        self.start_date = start_date
        self.end_date = end_date
        self.base_params = base_params or {}

        # Set number of workers
        if n_workers is None:
            n_workers = max(1, mp.cpu_count() - 1)
        self.n_workers = n_workers

        # Set up logger
        self.logger = logger or setup_optimizer_logger(stock_code)

        self.logger.info(f"Optimizer initialized: Stock={stock_code}, Workers={n_workers}, CPU cores={mp.cpu_count()}")

        self.best_coarse_result = None
        self.best_final_result = None

    def optimize(self) -> Dict[str, Any]:
        """
        Run full hierarchical optimization (coarse + fine).

        Returns:
            Best parameters from fine search phase
        """
        self.logger.info("=" * 60)
        self.logger.info(f"Starting optimization for {self.stock_code}")
        self.logger.info(f"Period: {self.start_date} to {self.end_date}")
        self.logger.info("=" * 60)

        # Also print to console
        print(f"\n{'='*60}")
        print(f"Starting optimization for {self.stock_code}")
        print(f"Period: {self.start_date} to {self.end_date}")
        print(f"Log: logs/optimization/{self.stock_code.replace('.', '_')}_*.log")
        print(f"{'='*60}")

        # Phase 1: Coarse search
        best_coarse = self.optimize_coarse()
        self.best_coarse_result = best_coarse

        if best_coarse is None:
            self.logger.error("Coarse search failed to find valid parameters")
            print(f"[ERROR] Coarse search failed")
            return None

        self.logger.info(f"Coarse Search Best score: {best_coarse['score']:.4f}")
        self.logger.info(f"Coarse Search Best params: {self._format_params(best_coarse['params'])}")

        # Phase 2: Fine search
        best_final = self.optimize_fine(best_coarse['params'])
        self.best_final_result = best_final

        if best_final is None:
            self.logger.warning("Fine search failed, returning coarse results")
            print(f"[WARNING] Fine search failed, using coarse results")
            return best_coarse

        self.logger.info(f"Fine Search Best score: {best_final['score']:.4f}")
        self.logger.info(f"Fine Search Best params: {self._format_params(best_final['params'])}")

        # Log final summary
        self._log_final_summary(best_final)

        print(f"\n[Optimization Complete] Final score: {best_final['score']:.4f}")

        return best_final

    def optimize_coarse(self) -> Optional[Dict[str, Any]]:
        """
        Phase 1: Coarse grid search.

        Fast exploration with large step sizes to find promising parameter regions.

        Returns:
            Best parameter set from coarse search
        """
        from src.strategies.price_breakout.optimizer.search_spaces import generate_coarse_search_space

        param_grid = generate_coarse_search_space()
        total = len(param_grid)

        est_hours = total * 4 / 3600 / self.n_workers
        self.logger.info(f"Coarse Search: {total:,} combinations, estimated {est_hours:.1f} hours")

        print(f"\n[Coarse Search] {total:,} parameter combinations")
        print(f"[Coarse Search] Estimated time: {est_hours:.1f} hours\n")

        # Prepare tasks
        tasks = [
            (self.stock_code, self.start_date, self.end_date, params, self.base_params)
            for params in param_grid
        ]

        # Create progress tracker for logging
        progress = ProgressTracker(total, self.logger, "Coarse Search")

        # Custom iterator that tracks progress
        def progress_iterator():
            with mp.Pool(self.n_workers) as pool:
                for result in pool.imap(_run_backtest_worker, tasks):
                    progress.update(1)
                    yield result

        # Run parallel backtests with progress bar and logging
        results = list(tqdm(
            progress_iterator(),
            total=total,
            desc="Coarse Search",
            unit="backtests"
        ))

        # Log final progress
        progress.finish()

        # Filter valid results and sort by score
        valid_results = [r for r in results if r and r.get('valid', False)]
        invalid_results = [r for r in results if r and not r.get('valid', False)]

        self.logger.info(f"Coarse Search: {len(valid_results)}/{total} valid results")
        print(f"\n[Coarse Search] {len(valid_results)}/{total} valid results")

        # Log failure reasons if there are invalid results
        if invalid_results and len(invalid_results) > 0:
            # Collect failure reasons
            failure_stats = {}
            for r in invalid_results[:20]:  # Sample first 20 to avoid too much output
                reasons = r.get('failure_reasons', ['Unknown'])
                for reason in reasons:
                    failure_stats[reason] = failure_stats.get(reason, 0) + 1

            if failure_stats:
                self.logger.info("[Failure Reasons] Sample (first 20 invalid):")
                print("[Failure Reasons] Sample (first 20 invalid):")
                for reason, count in sorted(failure_stats.items(), key=lambda x: -x[1])[:5]:
                    self.logger.info(f"  {reason}: {count} times")
                    print(f"  {reason}: {count} times")

                # Show one example of failed result with details
                if invalid_results:
                    example = invalid_results[0]
                    metrics = example.get('metrics', {})
                    if metrics:
                        basic = metrics.get('basic_info', {})
                        health = metrics.get('health_metrics', {})
                        trades = metrics.get('trade_stats', {})
                        self.logger.info("[Example Failed Result]")
                        print("[Example Failed Result]")
                        self.logger.info(f"  Return: {basic.get('total_return', 0):.2%}")
                        print(f"  Return: {basic.get('total_return', 0):.2%}")
                        self.logger.info(f"  Sharpe: {health.get('sharpe_ratio', 0):.2f}")
                        print(f"  Sharpe: {health.get('sharpe_ratio', 0):.2f}")
                        self.logger.info(f"  MaxDD: {health.get('max_drawdown', 0):.2%}")
                        print(f"  MaxDD: {health.get('max_drawdown', 0):.2%}")
                        self.logger.info(f"  Trades: {trades.get('total_trades', 0)}")
                        print(f"  Trades: {trades.get('total_trades', 0)}")
                        reasons = example.get('failure_reasons', [])
                        self.logger.info(f"  Reasons: {', '.join(reasons)}")
                        print(f"  Reasons: {', '.join(reasons)}")

        if not valid_results:
            self.logger.warning("Coarse Search: No valid results found")
            return None

        valid_results.sort(key=lambda x: x['score'], reverse=True)

        # Log top 3
        self.logger.info("Coarse Search Top 3:")
        for i, r in enumerate(valid_results[:3], 1):
            summary = _format_result_summary(r.get('metrics'))
            self.logger.info(f"  {i}. Score: {r['score']:.4f} - {summary}")

        # Also print to console
        print("[Coarse Search] Top 3 results:")
        for i, r in enumerate(valid_results[:3], 1):
            print(f"  {i}. Score: {r['score']:.4f} - {_format_result_summary(r.get('metrics'))}")

        return valid_results[0]

    def optimize_fine(self, best_coarse_params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Phase 2: Fine local search.

        Performs local optimization around best coarse parameters.

        Args:
            best_coarse_params: Best parameters from coarse search

        Returns:
            Best parameter set from fine search
        """
        from src.strategies.price_breakout.optimizer.search_spaces import generate_fine_search_space

        param_grid = generate_fine_search_space(best_coarse_params)
        total = len(param_grid)

        est_hours = total * 4 / 3600 / self.n_workers
        self.logger.info(f"Fine Search: {total:,} combinations, estimated {est_hours:.1f} hours")

        print(f"[Fine Search] Estimated time: {est_hours:.1f} hours\n")

        # Prepare tasks
        tasks = [
            (self.stock_code, self.start_date, self.end_date, params, self.base_params)
            for params in param_grid
        ]

        # Create progress tracker for logging
        progress = ProgressTracker(total, self.logger, "Fine Search")

        # Custom iterator that tracks progress
        def progress_iterator():
            with mp.Pool(self.n_workers) as pool:
                for result in pool.imap(_run_backtest_worker, tasks):
                    progress.update(1)
                    yield result

        # Run parallel backtests with progress bar and logging
        results = list(tqdm(
            progress_iterator(),
            total=total,
            desc="Fine Search",
            unit="backtests"
        ))

        # Log final progress
        progress.finish()

        # Filter valid results and sort by score
        valid_results = [r for r in results if r and r.get('valid', False)]

        self.logger.info(f"Fine Search: {len(valid_results)}/{total} valid results")
        print(f"\n[Fine Search] {len(valid_results)}/{total} valid results")

        if not valid_results:
            self.logger.warning("Fine Search: No valid results found")
            return None

        valid_results.sort(key=lambda x: x['score'], reverse=True)

        # Log top 3
        self.logger.info("Fine Search Top 3:")
        for i, r in enumerate(valid_results[:3], 1):
            summary = _format_result_summary(r.get('metrics'))
            self.logger.info(f"  {i}. Score: {r['score']:.4f} - {summary}")

        # Also print to console
        print("[Fine Search] Top 3 results:")
        for i, r in enumerate(valid_results[:3], 1):
            print(f"  {i}. Score: {r['score']:.4f} - {_format_result_summary(r.get('metrics'))}")

        return valid_results[0]

    def _log_final_summary(self, result: Dict[str, Any]):
        """Log final optimization summary."""
        self.logger.info("=" * 60)
        self.logger.info("OPTIMIZATION COMPLETE")
        self.logger.info("=" * 60)
        self.logger.info(f"Stock: {self.stock_code}")
        self.logger.info(f"Final Score: {result['score']:.4f}")

        # Log parameters
        mc = result['params'].get('market_config', {})
        rp = result['params'].get('regime_params', {})

        self.logger.info("Market Config:")
        self.logger.info(f"  bull_threshold: {mc.get('bull_threshold', 70)}")
        self.logger.info(f"  bear_threshold: {mc.get('bear_threshold', 40)}")

        for regime in ['bull', 'bear', 'neutral']:
            if regime in rp:
                self.logger.info(f"{regime.capitalize()} Market:")
                r = rp[regime]
                self.logger.info(f"  buy_multiplier: {r.get('buy_threshold_multiplier', 1.0):.3f}")
                self.logger.info(f"  sell_multiplier: {r.get('sell_threshold_multiplier', 1.0):.3f}")
                self.logger.info(f"  stop_multiplier: {r.get('stop_loss_multiplier', 0.8):.3f}")

        # Log metrics
        metrics = result.get('metrics', {})
        if metrics:
            self.logger.info("Performance Metrics:")
            basic = metrics.get('basic_info', {})
            health = metrics.get('health_metrics', {})
            trades = metrics.get('trade_stats', {})

            self.logger.info(f"  Total Return: {basic.get('total_return', 0):.2%}")
            self.logger.info(f"  Sharpe Ratio: {health.get('sharpe_ratio', 0):.2f}")
            self.logger.info(f"  Max Drawdown: {health.get('max_drawdown', 0):.2%}")
            self.logger.info(f"  Win Rate: {trades.get('win_rate', 0):.2%}")
            self.logger.info(f"  Total Trades: {trades.get('total_trades', 0)}")

        self.logger.info("=" * 60)

    def _format_params(self, params: Dict[str, Any]) -> str:
        """Format parameters for display."""
        mc = params.get('market_config', {})
        rp = params.get('regime_params', {})

        bull = rp.get('bull', {})
        bear = rp.get('bear', {})
        neutral = rp.get('neutral', {})

        buy_bull = bull.get('buy_threshold_multiplier', 0.5)
        sell_bull = bull.get('sell_threshold_multiplier', 1.6)
        buy_bear = bear.get('buy_threshold_multiplier', 1.7)
        sell_bear = bear.get('sell_threshold_multiplier', 0.7)
        buy_neutral = neutral.get('buy_threshold_multiplier', 1.0)
        sell_neutral = neutral.get('sell_threshold_multiplier', 1.0)

        return (
            f"bull_th={mc.get('bull_threshold', 70)}, "
            f"bear_th={mc.get('bear_threshold', 40)}, "
            f"bull=[buy:{buy_bull:.2f} sell:{sell_bull:.2f}], "
            f"bear=[buy:{buy_bear:.2f} sell:{sell_bear:.2f}], "
            f"neutral=[buy:{buy_neutral:.2f} sell:{sell_neutral:.2f}]"
        )


# Module-level functions for pickle compatibility

def _run_backtest_worker(task_data: tuple) -> Optional[Dict[str, Any]]:
    """
    Worker function to execute a single backtest.

    Must be a module-level function for pickle serialization in multiprocessing.

    Args:
        task_data: Tuple of (stock_code, start_date, end_date, params, base_params)

    Returns:
        Result dictionary with score and validity flag
    """
    stock_code, start_date, end_date, params, base_params = task_data

    # Set quiet mode for this worker process
    import os
    os.environ['BACKTEST_QUIET_MODE'] = '1'
    os.environ['STRATEGY_QUIET_MODE'] = '1'

    # Import in worker process
    try:
        from web.services.backtest_service import run_single_backtest
        from src.market.market_regime import update_regime_params, reset_regime_params
        from src.strategies.price_breakout.optimizer.objectives import check_constraints, calculate_score

        # Reset to default state first
        reset_regime_params()

        # Apply optimization parameters
        update_regime_params(params)

        # Extract strategy_params if present
        strategy_params = params.get('strategy_params', {}).copy()

        # Remove parameters not accepted by validation
        # enable_adaptive_thresholds is not in the validation whitelist
        # but will be set by the backtest service automatically for price_breakout
        strategy_params.pop('enable_adaptive_thresholds', None)
        strategy_params.pop('enable_blacklist', None)

        # Build backtest parameters
        bt_params = {
            'stock': stock_code,
            'start_date': start_date,
            'end_date': end_date,
            'strategy': 'price_breakout',  # Note: registry uses 'price_breakout' key
            'strategy_params': strategy_params,  # Add strategy_params
            **base_params
        }

        # Run backtest
        result = run_single_backtest(bt_params)

        if result is None:
            return {
                'params': params,
                'score': 0.0,
                'valid': False,
                'error': 'Backtest returned None'
            }

        # Check constraints
        passed, failure_reasons = check_constraints(result, verbose=True)
        if not passed:
            return {
                'params': params,
                'score': 0.0,
                'valid': False,
                'metrics': result,
                'failure_reasons': failure_reasons
            }

        # Calculate score
        score = calculate_score(result)

        return {
            'params': params,
            'score': score,
            'valid': True,
            'metrics': result
        }

    except Exception as e:
        import traceback
        return {
            'params': params,
            'score': 0.0,
            'valid': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }


def _format_result_summary(result: Optional[Dict[str, Any]]) -> str:
    """Format backtest result for logging."""
    if result is None:
        return "No result"

    try:
        basic = result.get('basic_info', {})
        health = result.get('health_metrics', {})
        trades = result.get('trade_stats', {})

        return (
            f"Return: {basic.get('total_return', 0):.2%}, "
            f"Sharpe: {health.get('sharpe_ratio', 0):.2f}, "
            f"MaxDD: {health.get('max_drawdown', 0):.2%}, "
            f"WinRate: {trades.get('win_rate', 0):.2%}"
        )
    except (KeyError, TypeError):
        return "Invalid result"
