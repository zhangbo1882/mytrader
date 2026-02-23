"""
Standalone script for running REGIME_PARAMS optimization.

Default settings:
  - Mode: 'anchored' (anchored walk-forward with embargo, most robust)
  - Search space: 'hybrid' (fixed regime multipliers + 3 base params, recommended)

Usage:
    # Optimize single stock (uses anchored WFO + hybrid search - most robust)
    python -m src.optimization.standalone 00941.HK

    # Optimize single stock with custom dates
    python -m src.optimization.standalone 00941.HK 2021-01-01 2024-12-31

    # Use simple search space (only 3 base params, adaptive OFF, even simpler)
    python -m src.optimization.standalone 00941.HK --simple

    # Use multi-period validation (older approach)
    python -m src.optimization.standalone 00941.HK --mode multi_val

    # Use walk-forward mode (single validation period)
    python -m src.optimization.standalone 00941.HK --mode walk_forward

    # Use standard mode (may overfit, not recommended)
    python -m src.optimization.standalone 00941.HK --mode standard

    # Optimize single stock with custom workers
    python -m src.optimization.standalone 00941.HK --workers 8

    # Run robustness check on already-saved parameters
    python -m src.optimization.standalone 00941.HK --robustness-only

    # Batch optimize from favorites list
    python -m src.optimization.standalone --batch --max-stocks 10

    # Resume batch optimization
    python -m src.optimization.standalone --batch --resume
"""

import argparse
import sys
import os
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from src.strategies.price_breakout.optimizer.parallel_optimizer import ParallelRegimeOptimizer
from src.strategies.price_breakout.optimizer.batch_optimizer import BatchOptimizer
from src.strategies.price_breakout.optimizer.regime_params_db import RegimeParamsDB
from src.strategies.price_breakout.optimizer.walk_forward import (
    anchored_walk_forward_optimization,
    walk_forward_optimization,
    rolling_window_optimization,
    multi_period_walk_forward_optimization
)
from src.strategies.price_breakout.optimizer.cross_validation import time_series_cv
from src.strategies.price_breakout.optimizer.robustness import check_parameter_robustness


def optimize_single_stock(stock_code: str, start_date: str, end_date: str,
                         n_workers: int = 6, save: bool = True, mode: str = 'anchored',
                         val_start: str = None, val_end: str = None,
                         use_simple_space: bool = False,
                         use_hybrid_space: bool = True,  # Default to hybrid (recommended)
                         run_robustness: bool = True):
    """
    Optimize parameters for a single stock.

    Args:
        stock_code: Stock symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        n_workers: Number of parallel workers
        save: Whether to save results to database
        mode: Optimization mode (default: 'anchored')
            - 'anchored': Anchored WFO with embargo (recommended, most robust)
            - 'multi_val': Multi-period validation (4 quarters of last 20%)
            - 'walk_forward': Train on training period, validate on validation period
            - 'standard': Original two-phase optimization (may overfit, not recommended)
            - 'rolling': Rolling window optimization
            - 'cv': Time-series cross-validation
        val_start: Validation start date (for walk_forward mode, auto-split if not provided)
        val_end: Validation end date (for walk_forward mode, auto-split if not provided)
        use_simple_space: Use simplified search space (only 3 base params, adaptive OFF).
        use_hybrid_space: Use hybrid search space (fixed regime multipliers + 3 base params,
            adaptive ON). Recommended for regime-adaptive strategy.
        run_robustness: Run robustness check after optimization (default: True)
    """
    print(f"\n{'='*70}")
    print(f"SINGLE STOCK OPTIMIZATION - MODE: {mode.upper()}")
    if use_hybrid_space:
        print(f"SEARCH SPACE: HYBRID (fixed regime multipliers + 3 base params, adaptive ON)")
    elif use_simple_space:
        print(f"SEARCH SPACE: SIMPLE (3 base params only, adaptive OFF)")
    print(f"{'='*70}")
    print(f"Stock: {stock_code}")
    print(f"Period: {start_date} to {end_date}")
    if val_start and val_end:
        print(f"Validation: {val_start} to {val_end}")
    print(f"Workers: {n_workers}")
    print(f"{'='*70}\n")

    # Patch optimizer search space if requested
    if use_hybrid_space:
        _patch_optimizer_hybrid_space()
    elif use_simple_space:
        _patch_optimizer_simple_space()

    result = None

    if mode == 'standard':
        # Original optimization (may overfit)
        optimizer = ParallelRegimeOptimizer(stock_code, start_date, end_date, n_workers)
        result = optimizer.optimize()

    elif mode == 'anchored':
        # Anchored walk-forward with embargo (recommended, most robust)
        result = anchored_walk_forward_optimization(
            stock_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            n_workers=n_workers,
        )

    elif mode == 'walk_forward':
        # Walk-forward optimization with validation
        if not val_start or not val_end:
            # Auto-split: 80% train, 20% validation
            from datetime import datetime, timedelta
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            total_days = (end_dt - start_dt).days
            split_point = start_dt + timedelta(days=int(total_days * 0.8))
            val_start = split_point.strftime('%Y-%m-%d')
            val_end = end_date
            end_date = (split_point - timedelta(days=1)).strftime('%Y-%m-%d')

            print(f"[Auto-split] Training: {start_date} to {end_date}")
            print(f"[Auto-split] Validation: {val_start} to {val_end}")

        result = walk_forward_optimization(
            stock_code=stock_code,
            train_start=start_date,
            train_end=end_date,
            val_start=val_start,
            val_end=val_end,
            n_workers=n_workers
        )

    elif mode == 'rolling':
        # Rolling window optimization
        from datetime import datetime, timedelta
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        total_days = (end_dt - start_dt).days

        # Use 3 windows: each with 250 days train, 60 days validation
        results = rolling_window_optimization(
            stock_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            train_days=250,
            val_days=60,
            n_workers=n_workers
        )

        if results:
            # Use best window result based on validation score
            result = max(results, key=lambda x: x['result']['val_score'])['result']
            print(f"\n[Rolling Window] Selected best of {len(results)} windows")

    elif mode == 'cv':
        # Time-series cross-validation
        result = time_series_cv(
            stock_code=stock_code,
            start_date=start_date,
            end_date=end_date,
            n_folds=3,
            n_workers=n_workers
        )

    elif mode == 'multi_val':
        # Multi-period walk-forward validation
        # Auto-generate validation periods: divide end period into 4 quarters
        from datetime import datetime, timedelta
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        total_days = (end_dt - start_dt).days

        # Use 80% for training, last 20% for validation periods
        split_point = start_dt + timedelta(days=int(total_days * 0.8))
        train_end = split_point.strftime('%Y-%m-%d')

        # Split validation period into 4 quarters
        val_start_dt = split_point + timedelta(days=1)
        val_total_days = (end_dt - val_start_dt).days
        quarter_days = val_total_days // 4

        validation_periods = []
        for i in range(4):
            q_start_dt = val_start_dt + timedelta(days=i * quarter_days)
            q_end_dt = val_start_dt + timedelta(days=(i + 1) * quarter_days - 1)
            if q_end_dt > end_dt:
                q_end_dt = end_dt
            validation_periods.append((
                f'Q{i+1}',
                q_start_dt.strftime('%Y-%m-%d'),
                q_end_dt.strftime('%Y-%m-%d')
            ))

        print(f"[Multi-Period Validation] Training: {start_date} ~ {train_end}")
        print(f"[Multi-Period Validation] Validation periods:")
        for name, vs, ve in validation_periods:
            print(f"  {name}: {vs} ~ {ve}")

        result = multi_period_walk_forward_optimization(
            stock_code=stock_code,
            train_start=start_date,
            train_end=train_end,
            validation_periods=validation_periods,
            n_workers=n_workers,
            min_avg_val_score=0.35,  # Lower threshold for multi-period
            min_pass_ratio=0.5  # At least 50% of periods must pass
        )

    if result:
        # Run robustness check before saving
        robustness_result = None
        if run_robustness:
            print(f"\n[Robustness Check] Testing parameter stability...")
            robustness_result = check_parameter_robustness(
                best_params=result['params'],
                stock_code=stock_code,
                start_date=start_date,
                end_date=end_date,
            )

        # Save to database
        if save:
            db = RegimeParamsDB()
            db.save_best_params(
                stock_code,
                result['params'],
                result['metrics'],
                result['score'],
                phase='final',
                start_date=start_date,
                end_date=end_date
            )
            print(f"\n[Saved] Results saved to database for {stock_code}")

        # Print final summary
        print(f"\n{'='*70}")
        print(f"FINAL RESULT - {stock_code}")
        print(f"{'='*70}")
        print(f"Optimization Score: {result['score']:.4f}")

        metrics = result.get('metrics', {})
        basic = metrics.get('basic_info', {})
        health = metrics.get('health_metrics', {})
        trades = metrics.get('trade_stats', {})

        print(f"\nPerformance Metrics:")
        print(f"  Total Return: {basic.get('total_return', 0):.2%}")
        print(f"  Sharpe Ratio: {health.get('sharpe_ratio', 0):.2f}")
        print(f"  Max Drawdown: {health.get('max_drawdown', 0):.2%}")
        print(f"  Win Rate: {trades.get('win_rate', 0):.2%}")
        print(f"  Total Trades: {trades.get('total_trades', 0)}")
        print(f"  Profit Factor: {trades.get('profit_factor', 0):.2f}")

        # Buy & Hold Return (基准对比)
        benchmark = metrics.get('benchmark_comparison', {})
        if benchmark and 'benchmark_metrics' in benchmark:
            bm_metrics = benchmark['benchmark_metrics']
            bm_return = bm_metrics.get('total_return', 0)
            bm_type = benchmark.get('benchmark_type', '买入持有')
            bm_name = benchmark.get('benchmark_name', stock_code)

            print(f"\n{bm_type} ({bm_name}):")
            print(f"  Return: {bm_return:.2%}")
            print(f"  Strategy vs {bm_type}: {(basic.get('total_return', 0) - bm_return):.2%} (excess return)")

        # Market State Distribution
        market_state_dist = metrics.get('market_state_distribution', {})
        if market_state_dist:
            print(f"\nMarket State Distribution:")
            bull_count = market_state_dist.get('bull', {}).get('count', 0)
            bull_pct = market_state_dist.get('bull', {}).get('percentage', 0)
            bear_count = market_state_dist.get('bear', {}).get('count', 0)
            bear_pct = market_state_dist.get('bear', {}).get('percentage', 0)
            neutral_count = market_state_dist.get('neutral', {}).get('count', 0)
            neutral_pct = market_state_dist.get('neutral', {}).get('percentage', 0)
            total_days = market_state_dist.get('total', 0)

            print(f"  Bull Market:    {bull_count:3d} days ({bull_pct:5.1f}%)")
            print(f"  Bear Market:    {bear_count:3d} days ({bear_pct:5.1f}%)")
            print(f"  Neutral Market: {neutral_count:3d} days ({neutral_pct:5.1f}%)")
            print(f"  Total:          {total_days:3d} days")

        print(f"\nOptimized Parameters:")
        mc = result['params'].get('market_config', {})
        rp = result['params'].get('regime_params', {})
        sp = result['params'].get('strategy_params', {})

        print(f"  Market Config:")
        print(f"    Bull Threshold: {mc.get('bull_threshold', 70)}")
        print(f"    Bear Threshold: {mc.get('bear_threshold', 40)}")

        for regime in ['bull', 'bear', 'neutral']:
            if regime in rp:
                print(f"  {regime.capitalize()} Market:")
                print(f"    Buy Multiplier: {rp[regime].get('buy_threshold_multiplier', 1.0):.3f}")
                print(f"    Sell Multiplier: {rp[regime].get('sell_threshold_multiplier', 1.0):.3f}")
                print(f"    Stop Loss Multiplier: {rp[regime].get('stop_loss_multiplier', 0.8):.3f}")

        if sp:
            print(f"  Base Strategy Thresholds:")
            print(f"    Buy Threshold: {sp.get('base_buy_threshold', 1.0):.2f}%")
            print(f"    Sell Threshold: {sp.get('base_sell_threshold', 5.0):.2f}%")
            print(f"    Stop Loss Threshold: {sp.get('base_stop_loss_threshold', 10.0):.2f}%")

        # Print robustness summary if available
        if robustness_result:
            rs = robustness_result['robustness_score']
            print(f"\nRobustness Check:")
            print(f"  Score:          {rs:.1%}  ({'ROBUST' if robustness_result['is_robust'] else 'FRAGILE'})")
            print(f"  Passed:         {robustness_result['passed']}/{robustness_result['n_perturbations']} neighbors")
            print(f"  Recommendation: {robustness_result['recommendation']}")

        print(f"{'='*70}\n")

        return 0
    else:
        print(f"\n[ERROR] Optimization failed for {stock_code}")
        return 1


def _patch_optimizer_hybrid_space():
    """
    Monkey-patch the optimizer to use the hybrid search space.
    Fixes regime multipliers based on market logic, only optimizes 3 base params.
    """
    import src.optimization.parallel_optimizer as po_module
    import src.optimization.search_spaces as ss_module

    def _hybrid_optimize_coarse(self):
        param_grid = ss_module.generate_hybrid_search_space()
        total = len(param_grid)
        import multiprocessing as mp
        from tqdm import tqdm
        from src.strategies.price_breakout.optimizer.parallel_optimizer import _run_backtest_worker, _format_result_summary, ProgressTracker

        est_seconds = total * 4 / self.n_workers
        self.logger.info(f"Hybrid Search: {total} combinations, estimated {est_seconds:.0f}s")
        print(f"\n[Hybrid Search] {total} parameter combinations")

        tasks = [
            (self.stock_code, self.start_date, self.end_date, params, self.base_params)
            for params in param_grid
        ]

        progress = ProgressTracker(total, self.logger, "Hybrid Search")

        def progress_iterator():
            with mp.Pool(self.n_workers) as pool:
                for result in pool.imap(_run_backtest_worker, tasks):
                    progress.update(1)
                    yield result

        results = list(tqdm(progress_iterator(), total=total, desc="Hybrid Search", unit="backtests"))
        progress.finish()

        valid_results = [r for r in results if r and r.get('valid', False)]
        invalid_results = [r for r in results if r and not r.get('valid', False)]

        self.logger.info(f"Hybrid Search: {len(valid_results)}/{total} valid results")
        print(f"\n[Hybrid Search] {len(valid_results)}/{total} valid results")

        # Log failure reasons if all results are invalid
        if len(valid_results) == 0 and len(invalid_results) > 0:
            # Collect failure reasons
            failure_stats = {}
            for r in invalid_results[:min(20, len(invalid_results))]:  # Sample first 20
                reasons = r.get('failure_reasons', [])
                if not reasons:
                    # If no failure_reasons, try to infer from metrics
                    metrics = r.get('metrics', {})
                    if metrics:
                        health = metrics.get('health_metrics', {})
                        trades = metrics.get('trade_stats', {})
                        sharpe = health.get('sharpe_ratio', -999)
                        max_dd = health.get('max_drawdown', -999)
                        num_trades = trades.get('total_trades', 0)

                        if sharpe <= 0.3:
                            reasons.append(f"Sharpe {sharpe:.2f} ≤ 0.3")
                        if max_dd < -0.20:
                            reasons.append(f"MaxDD {max_dd*100:.1f}% < -20%")
                        if num_trades < 1:
                            reasons.append(f"Trades {num_trades} < 1")

                        if not reasons:
                            reasons = ['Unknown (check error log)']
                    else:
                        reasons = ['No metrics available']

                for reason in reasons:
                    failure_stats[reason] = failure_stats.get(reason, 0) + 1

            if failure_stats:
                self.logger.info("[Failure Reasons] Top 5 (sample of first 20 invalid):")
                print("[Failure Reasons] Top 5 (sample of first 20 invalid):")
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

                        total_return = basic.get('total_return', 0)
                        sharpe = health.get('sharpe_ratio', 0)
                        max_dd = health.get('max_drawdown', 0)
                        num_trades = trades.get('total_trades', 0)

                        self.logger.info("[Example Failed Result Details]")
                        print("[Example Failed Result Details]")
                        self.logger.info(f"  Return: {total_return*100:.2f}%")
                        print(f"  Return: {total_return*100:.2f}%")
                        self.logger.info(f"  Sharpe: {sharpe:.2f} (need > 0.3)")
                        print(f"  Sharpe: {sharpe:.2f} (need > 0.3)")
                        self.logger.info(f"  MaxDD: {max_dd*100:.2f}% (need ≥ -20%)")
                        print(f"  MaxDD: {max_dd*100:.2f}% (need ≥ -20%)")
                        self.logger.info(f"  Trades: {num_trades} (need ≥ 1)")
                        print(f"  Trades: {num_trades} (need ≥ 1)")

                        reasons = example.get('failure_reasons', [])
                        if reasons:
                            self.logger.info(f"  Reasons: {', '.join(reasons)}")
                            print(f"  Reasons: {', '.join(reasons)}")

        if not valid_results:
            return None

        valid_results.sort(key=lambda x: x['score'], reverse=True)
        print("[Hybrid Search] Top 3 results:")
        for i, r in enumerate(valid_results[:3], 1):
            print(f"  {i}. Score: {r['score']:.4f} - {_format_result_summary(r.get('metrics'))}")

        return valid_results[0]

    def _hybrid_optimize(self):
        best = self.optimize_coarse()
        self.best_coarse_result = best
        self.best_final_result = best
        if best:
            self._log_final_summary(best)
            print(f"\n[Hybrid Optimization Complete] Final score: {best['score']:.4f}")
        return best

    po_module.ParallelRegimeOptimizer.optimize_coarse = _hybrid_optimize_coarse
    po_module.ParallelRegimeOptimizer.optimize = _hybrid_optimize


def _patch_optimizer_simple_space():
    """
    Monkey-patch the coarse optimizer to use the simple search space.
    Restores the original after optimization completes.
    """
    import src.optimization.parallel_optimizer as po_module
    import src.optimization.search_spaces as ss_module

    original_optimize_coarse = po_module.ParallelRegimeOptimizer.optimize_coarse

    def _simple_optimize_coarse(self):
        """Use simple search space instead of coarse search space."""
        param_grid = ss_module.generate_simple_search_space()
        total = len(param_grid)
        import multiprocessing as mp
        from tqdm import tqdm
        from src.strategies.price_breakout.optimizer.parallel_optimizer import _run_backtest_worker, _format_result_summary, ProgressTracker

        est_seconds = total * 4 / self.n_workers
        self.logger.info(f"Simple Search: {total} combinations, estimated {est_seconds:.0f}s")
        print(f"\n[Simple Search] {total} parameter combinations (anti-overfitting mode)")

        tasks = [
            (self.stock_code, self.start_date, self.end_date, params, self.base_params)
            for params in param_grid
        ]

        progress = ProgressTracker(total, self.logger, "Simple Search")

        def progress_iterator():
            with mp.Pool(self.n_workers) as pool:
                for result in pool.imap(_run_backtest_worker, tasks):
                    progress.update(1)
                    yield result

        results = list(tqdm(progress_iterator(), total=total, desc="Simple Search", unit="backtests"))
        progress.finish()

        valid_results = [r for r in results if r and r.get('valid', False)]
        self.logger.info(f"Simple Search: {len(valid_results)}/{total} valid results")
        print(f"\n[Simple Search] {len(valid_results)}/{total} valid results")

        if not valid_results:
            return None

        valid_results.sort(key=lambda x: x['score'], reverse=True)
        print("[Simple Search] Top 3 results:")
        for i, r in enumerate(valid_results[:3], 1):
            print(f"  {i}. Score: {r['score']:.4f} - {_format_result_summary(r.get('metrics'))}")

        return valid_results[0]

    # Replace coarse search with simple search (skip fine search by making it a no-op)
    po_module.ParallelRegimeOptimizer.optimize_coarse = _simple_optimize_coarse

    # Make fine search return coarse results unchanged (no additional over-fitting)
    original_optimize = po_module.ParallelRegimeOptimizer.optimize

    def _simple_optimize(self):
        best_coarse = self.optimize_coarse()
        self.best_coarse_result = best_coarse
        self.best_final_result = best_coarse
        if best_coarse:
            self._log_final_summary(best_coarse)
            print(f"\n[Simple Optimization Complete] Final score: {best_coarse['score']:.4f}")
        return best_coarse

    po_module.ParallelRegimeOptimizer.optimize = _simple_optimize


def run_batch_optimization(start_date: str, end_date: str,
                          max_stocks: int = None, n_workers: int = 6,
                          resume: bool = False):
    """
    Run batch optimization for multiple stocks.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        max_stocks: Maximum number of stocks to optimize
        n_workers: Number of parallel workers per stock
        resume: Resume previous batch optimization
    """
    from src.db.favorites_db import FavoritesDB

    # Load stock list from favorites
    fav_db = FavoritesDB()
    favorites = fav_db.list_favorites()

    # Filter for HK stocks
    stock_list = [f['symbol'] for f in favorites if f['symbol'].endswith('.HK')]

    if not stock_list:
        print("[ERROR] No HK stocks found in favorites")
        return 1

    print(f"\nFound {len(stock_list)} HK stocks in favorites")

    # Run batch optimization
    batch_optimizer = BatchOptimizer(
        stock_list, start_date, end_date, n_workers
    )

    if resume:
        results = batch_optimizer.resume_batch()
    else:
        results = batch_optimizer.run_batch(max_stocks=max_stocks)

    # Print final summary
    print(f"\n{'='*70}")
    print(f"BATCH OPTIMIZATION COMPLETE")
    print(f"{'='*70}")

    successful = [r for r in results if r['status'] == 'success']
    if successful:
        scores = [r['score'] for r in successful]
        print(f"Optimized: {len(successful)} stocks")
        print(f"Average Score: {sum(scores)/len(scores):.4f}")
        print(f"Best Score: {max(scores):.4f}")
    else:
        print("No successful optimizations")

    print(f"{'='*70}\n")

    return 0


def show_database_stats():
    """Show database statistics."""
    db = RegimeParamsDB()

    print(f"\n{'='*70}")
    print(f"OPTIMIZATION DATABASE STATISTICS")
    print(f"{'='*70}")

    stats = db.get_statistics()
    print(f"Total stocks: {stats['total_stocks']}")
    print(f"Average score: {stats['avg_score']:.4f}")
    print(f"Best score: {stats['max_score']:.4f}")
    print(f"Worst score: {stats['min_score']:.4f}")

    print(f"\nTop 10 Stocks:")
    top10 = db.get_top_performers(10)
    for i, stock in enumerate(top10, 1):
        print(f"  {i}. {stock['stock_code']}: {stock['score']:.4f}")

    print(f"{'='*70}\n")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='REGIME_PARAMS optimization with multi-core parallelization'
    )

    parser.add_argument(
        'stock',
        nargs='?',
        help='Stock code to optimize (e.g., 00941.HK)'
    )

    parser.add_argument(
        'start_date',
        nargs='?',
        default='2024-01-01',
        help='Start date (YYYY-MM-DD, default: 2024-01-01)'
    )

    parser.add_argument(
        'end_date',
        nargs='?',
        default=None,
        help='End date (YYYY-MM-DD, default: today)'
    )

    parser.add_argument(
        '--workers', '-w',
        type=int,
        default=6,
        help='Number of parallel workers (default: 6)'
    )

    parser.add_argument(
        '--batch', '-b',
        action='store_true',
        help='Run batch optimization on all favorites'
    )

    parser.add_argument(
        '--max-stocks', '-m',
        type=int,
        default=None,
        help='Maximum number of stocks to optimize in batch mode'
    )

    parser.add_argument(
        '--resume', '-r',
        action='store_true',
        help='Resume previous batch optimization'
    )

    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save results to database'
    )

    parser.add_argument(
        '--stats', '-s',
        action='store_true',
        help='Show database statistics'
    )

    parser.add_argument(
        '--mode', '-M',
        type=str,
        default='anchored',
        choices=['standard', 'walk_forward', 'multi_val', 'rolling', 'cv', 'anchored'],
        help='Optimization mode: anchored (default, most robust), multi_val, walk_forward, standard (may overfit), rolling, cv'
    )

    parser.add_argument(
        '--val-start',
        type=str,
        default=None,
        help='Validation start date for walk_forward mode (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--val-end',
        type=str,
        default=None,
        help='Validation end date for walk_forward mode (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--simple',
        action='store_true',
        help='Use simplified search space (only 3 base params, adaptive thresholds OFF). Overrides default hybrid.'
    )

    parser.add_argument(
        '--hybrid',
        action='store_true',
        dest='use_hybrid_space',
        default=True,  # Already the default, just for explicit setting if needed
        help='Use hybrid search space (fixed regime multipliers + 3 base params, adaptive thresholds ON). This is the default.'
    )

    parser.add_argument(
        '--no-hybrid',
        action='store_false',
        dest='use_hybrid_space',
        help='Disable hybrid search space (use full coarse+fine optimization). Not recommended.'
    )

    parser.add_argument(
        '--no-robustness',
        action='store_true',
        help='Skip robustness check after optimization'
    )

    args = parser.parse_args()

    # Handle stats command
    if args.stats:
        show_database_stats()
        return 0

    # Set default end date
    end_date = args.end_date
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    # Handle batch mode
    if args.batch:
        return run_batch_optimization(
            args.start_date, end_date,
            max_stocks=args.max_stocks,
            n_workers=args.workers,
            resume=args.resume
        )

    # Single stock mode (requires stock code)
    if not args.stock:
        parser.error("Stock code is required for single stock optimization")

    return optimize_single_stock(
        args.stock,
        args.start_date,
        end_date,
        n_workers=args.workers,
        save=not args.no_save,
        mode=args.mode,
        val_start=args.val_start,
        val_end=args.val_end,
        use_simple_space=args.simple,
        use_hybrid_space=args.use_hybrid_space,
        run_robustness=not args.no_robustness,
    )


if __name__ == '__main__':
    sys.exit(main())
