#!/usr/bin/env python
"""
Train Quarterly Financial Prediction Model

This script trains a machine learning model to predict next-quarter stock returns
using quarterly financial data.

Usage:
    # Train single stock model
    python scripts/train_quarterly_model.py \
        --symbols 600382 \
        --start-quarter 2020Q1 \
        --end-quarter 2024Q4 \
        --feature-mode with_valuation \
        --train-mode single

    # Train multi-stock model (same industry)
    python scripts/train_quarterly_model.py \
        --symbols 600382 000001 600519 \
        --start-quarter 2020Q1 \
        --end-quarter 2024Q4 \
        --feature-mode with_valuation \
        --train-mode multi

    # Train with hyperparameter optimization
    python scripts/train_quarterly_model.py \
        --symbols 600382 \
        --start-quarter 2020Q1 \
        --end-quarter 2024Q4 \
        --optimize-hyperparams
"""
import argparse
import sys
import os
import logging

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ml.trainers.quarterly_trainer import QuarterlyTrainer
from config.settings import TUSHARE_DB_PATH

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Train quarterly financial prediction model',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # Required arguments
    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        required=True,
        help='Stock codes (e.g., 600382 000001 600519)'
    )

    parser.add_argument(
        '--start-quarter',
        type=str,
        default='2020Q1',
        help='Start quarter (default: 2020Q1)'
    )

    parser.add_argument(
        '--end-quarter',
        type=str,
        default='2024Q4',
        help='End quarter (default: 2024Q4)'
    )

    # Feature mode
    parser.add_argument(
        '--feature-mode',
        type=str,
        choices=['financial_only', 'with_reports', 'with_valuation'],
        default='with_valuation',
        help='Feature mode (default: with_valuation)'
    )

    # Training mode
    parser.add_argument(
        '--train-mode',
        type=str,
        choices=['single', 'multi'],
        default='multi',
        help='Training mode: single for one stock, multi for multiple stocks (default: multi)'
    )

    # Training parameters
    parser.add_argument(
        '--train-ratio',
        type=float,
        default=0.7,
        help='Training set ratio (default: 0.7)'
    )

    parser.add_argument(
        '--val-ratio',
        type=float,
        default=0.15,
        help='Validation set ratio (default: 0.15)'
    )

    # Hyperparameter optimization
    parser.add_argument(
        '--optimize-hyperparams',
        action='store_true',
        help='Enable hyperparameter optimization with Optuna'
    )

    parser.add_argument(
        '--n-trials',
        type=int,
        default=50,
        help='Number of optimization trials (default: 50)'
    )

    # Model ID
    parser.add_argument(
        '--model-id',
        type=str,
        default=None,
        help='Custom model ID (optional)'
    )

    # Window parameters
    parser.add_argument(
        '--window-days',
        type=int,
        default=60,
        help='Trading days window for return calculation (default: 60)'
    )

    parser.add_argument(
        '--skip-days',
        type=int,
        default=5,
        help='Trading days to skip after announcement (default: 5)'
    )

    return parser.parse_args()


def main():
    """Main training function"""
    args = parse_args()

    # Validate arguments
    if args.train_mode == 'single' and len(args.symbols) > 1:
        logger.error("Single mode can only train on one stock")
        sys.exit(1)

    logger.info("="*70)
    logger.info("Quarterly Financial Prediction Model Training")
    logger.info("="*70)
    logger.info(f"Symbols: {args.symbols}")
    logger.info(f"Training Mode: {args.train_mode}")
    logger.info(f"Quarter Range: {args.start_quarter} - {args.end_quarter}")
    logger.info(f"Feature Mode: {args.feature_mode}")
    logger.info(f"Hyperparameter Optimization: {args.optimize_hyperparams}")
    logger.info("="*70)

    try:
        # Initialize trainer
        trainer = QuarterlyTrainer(db_path=str(TUSHARE_DB_PATH))

        # Train model
        logger.info("Starting model training...")
        result = trainer.train(
            symbols=args.symbols,
            start_quarter=args.start_quarter,
            end_quarter=args.end_quarter,
            feature_mode=args.feature_mode,
            train_mode=args.train_mode,
            train_ratio=args.train_ratio,
            val_ratio=args.val_ratio,
            optimize_hyperparams=args.optimize_hyperparams,
            model_id=args.model_id,
            window_days=args.window_days,
            skip_days=args.skip_days
        )

        # Print results
        logger.info("")
        logger.info("="*70)
        logger.info("Training Complete!")
        logger.info("="*70)
        logger.info(f"Model ID: {result['model_id']}")
        logger.info(f"Training Samples: {result['train_samples']}")
        logger.info(f"Validation Samples: {result['val_samples']}")
        logger.info(f"Test Samples: {result['test_samples']}")
        logger.info(f"Features: {result['n_features']}")
        logger.info("")
        logger.info("Test Set Performance:")
        logger.info(f"  MAE:  {result['test_metrics']['mae']:.4f}")
        logger.info(f"  RMSE: {result['test_metrics']['rmse']:.4f}")
        logger.info(f"  R²:   {result['test_metrics']['r2']:.4f}")
        logger.info(f"  Direction Accuracy: {result['test_metrics']['direction_accuracy']:.2%}")
        logger.info(f"  Spearman IC: {result['test_metrics']['spearman_ic']:.4f}")
        logger.info("")

        # Print feature importance (top 10)
        if 'feature_importance' in result and result['feature_importance']:
            logger.info("Top 10 Features:")
            sorted_features = sorted(
                result['feature_importance'].items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
            for i, (feat, imp) in enumerate(sorted_features, 1):
                logger.info(f"  {i:2d}. {feat:30s}: {imp:.4f}")

        logger.info("")
        logger.info(f"Model saved to: {result['model_path']}")
        logger.info("="*70)

    except Exception as e:
        logger.error(f"Training failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
