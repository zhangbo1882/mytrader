"""
Batch optimizer for optimizing multiple stocks sequentially.
"""

import time
import numpy as np
from typing import Dict, Any, List, Optional
from pathlib import Path

from src.strategies.price_breakout.optimizer.parallel_optimizer import ParallelRegimeOptimizer
from src.strategies.price_breakout.optimizer.regime_params_db import RegimeParamsDB


class BatchOptimizer:
    """
    Batch optimizer for multiple stocks.

    Optimizes each stock sequentially and stores results in database.
    """

    def __init__(self, stock_list: List[str], start_date: str, end_date: str,
                 n_workers: int = 6, base_params: Optional[Dict[str, Any]] = None):
        """
        Initialize batch optimizer.

        Args:
            stock_list: List of stock symbols to optimize
            start_date: Backtest start date
            end_date: Backtest end date
            n_workers: Number of parallel workers per stock
            base_params: Additional base parameters for backtest
        """
        self.stock_list = stock_list
        self.start_date = start_date
        self.end_date = end_date
        self.n_workers = n_workers
        self.base_params = base_params or {}
        self.db = RegimeParamsDB()

    def run_batch(self, max_stocks: Optional[int] = None,
                  save_interval: int = 1) -> List[Dict[str, Any]]:
        """
        Run batch optimization.

        Args:
            max_stocks: Maximum number of stocks to optimize (None = all)
            save_interval: Save results after every N stocks

        Returns:
            List of optimization results
        """
        stocks_to_optimize = self.stock_list[:max_stocks] if max_stocks else self.stock_list

        total = len(stocks_to_optimize)
        est_hours = total * 8.5 / 24  # 8.5 hours per stock

        print(f"\n{'='*70}")
        print(f"BATCH OPTIMIZATION")
        print(f"{'='*70}")
        print(f"Stocks to optimize: {total}")
        print(f"Period: {self.start_date} to {self.end_date}")
        print(f"Workers per stock: {self.n_workers}")
        print(f"Estimated time: {est_hours:.1f} days")
        print(f"{'='*70}\n")

        results = []
        start_time = time.time()

        for i, stock in enumerate(stocks_to_optimize, 1):
            print(f"\n{'='*70}")
            print(f"[{i}/{total}] Optimizing: {stock}")
            print(f"{'='*70}")

            stock_start = time.time()

            try:
                # Run optimization
                optimizer = ParallelRegimeOptimizer(
                    stock, self.start_date, self.end_date,
                    self.n_workers, self.base_params
                )

                best_result = optimizer.optimize()

                if best_result:
                    # Save to database
                    self.db.save_best_params(
                        stock,
                        best_result['params'],
                        best_result['metrics'],
                        best_result['score'],
                        phase='final',
                        start_date=self.start_date,
                        end_date=self.end_date
                    )

                    elapsed = time.time() - stock_start
                    results.append({
                        'stock': stock,
                        'score': best_result['score'],
                        'params': best_result['params'],
                        'metrics': best_result['metrics'],
                        'elapsed_seconds': elapsed,
                        'status': 'success'
                    })

                    print(f"\n[{stock}] SUCCESS - Score: {best_result['score']:.4f}, "
                          f"Time: {elapsed/3600:.2f}h")

                else:
                    results.append({
                        'stock': stock,
                        'status': 'failed',
                        'error': 'No valid parameters found'
                    })
                    print(f"\n[{stock}] FAILED - No valid parameters found")

            except Exception as e:
                elapsed = time.time() - stock_start
                results.append({
                    'stock': stock,
                    'status': 'error',
                    'error': str(e),
                    'elapsed_seconds': elapsed
                })
                print(f"\n[{stock}] ERROR - {e}")

            # Periodic save
            if i % save_interval == 0:
                self._save_checkpoint(results, i)

        total_elapsed = time.time() - start_time

        # Generate report
        self._generate_report(results, total_elapsed)

        return results

    def resume_batch(self) -> List[Dict[str, Any]]:
        """
        Resume batch optimization, skipping already optimized stocks.

        Returns:
            List of optimization results for newly optimized stocks
        """
        # Get already optimized stocks
        existing = set()
        for row in self.db.list_all_stocks():
            existing.add(row['stock_code'])

        # Filter out already optimized stocks
        remaining = [s for s in self.stock_list if s not in existing]

        print(f"\n[Resume] Found {len(existing)} already optimized stocks")
        print(f"[Resume] {len(remaining)} stocks remaining\n")

        if not remaining:
            print("[Resume] All stocks already optimized!")
            return []

        # Update stock list and run
        self.stock_list = remaining
        return self.run_batch()

    def _save_checkpoint(self, results: List[Dict[str, Any]], completed: int):
        """Save checkpoint information."""
        checkpoint_path = Path('data/optimization_checkpoint.json')
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        import json

        checkpoint = {
            'completed': completed,
            'total': len(self.stock_list),
            'results': results,
            'timestamp': time.time()
        }

        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint, f, indent=2, default=str)

        print(f"\n[Checkpoint] Saved progress: {completed}/{len(self.stock_list)} completed")

    def _generate_report(self, results: List[Dict[str, Any]], total_seconds: float):
        """Generate optimization report."""
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] in ['failed', 'error']]

        print(f"\n{'='*70}")
        print(f"BATCH OPTIMIZATION COMPLETE")
        print(f"{'='*70}")
        print(f"Total time: {total_seconds/3600:.2f} hours ({total_seconds/86400:.2f} days)")
        print(f"\nResults:")
        print(f"  Successful: {len(successful)}/{len(results)}")
        print(f"  Failed: {len(failed)}/{len(results)}")

        if successful:
            scores = [r['score'] for r in successful]
            print(f"\nScore Statistics:")
            print(f"  Mean: {np.mean(scores):.4f}")
            print(f"  Std: {np.std(scores):.4f}")
            print(f"  Max: {np.max(scores):.4f}")
            print(f"  Min: {np.min(scores):.4f}")
            print(f"  Median: {np.median(scores):.4f}")

            # Top 5
            top5 = sorted(successful, key=lambda x: x['score'], reverse=True)[:5]
            print(f"\nTop 5 Stocks:")
            for i, r in enumerate(top5, 1):
                print(f"  {i}. {r['stock']}: {r['score']:.4f}")

            # Bottom 5
            if len(successful) > 5:
                bottom5 = sorted(successful, key=lambda x: x['score'])[:5]
                print(f"\nBottom 5 Stocks:")
                for i, r in enumerate(bottom5, 1):
                    print(f"  {i}. {r['stock']}: {r['score']:.4f}")

        if failed:
            print(f"\nFailed Stocks:")
            for r in failed[:5]:  # Show first 5 failures
                print(f"  {r['stock']}: {r.get('error', 'Unknown error')}")
            if len(failed) > 5:
                print(f"  ... and {len(failed) - 5} more")

        # Database statistics
        db_stats = self.db.get_statistics()
        print(f"\nDatabase Statistics:")
        print(f"  Total stocks: {db_stats['total_stocks']}")
        print(f"  Average score: {db_stats['avg_score']:.4f}")
        print(f"  Best score: {db_stats['max_score']:.4f}")

        print(f"{'='*70}\n")

    def get_optimization_summary(self) -> Dict[str, Any]:
        """Get summary of optimization results."""
        return self.db.get_statistics()
