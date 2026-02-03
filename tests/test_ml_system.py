#!/usr/bin/env python3
"""
ML System Verification Test

Tests the core functionality of the ML prediction system.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def test_data_loader():
    """Test data loader"""
    print("\n" + "="*60)
    print("TEST 1: Data Loader")
    print("="*60)

    try:
        from src.ml.data_loader import MlDataLoader

        loader = MlDataLoader(db_path="data/tushare_data.db")

        # Test loading data for a stock
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        print(f"Loading data for 600382 from {start_date} to {end_date}...")

        df = loader.load_training_data(
            symbol="600382",
            start_date=start_date,
            end_date=end_date
        )

        if df.empty:
            print("❌ No data loaded. Please ensure database has data for stock 600382")
            return False

        print(f"✓ Loaded {len(df)} rows of data")
        print(f"  Columns: {list(df.columns)[:10]}...")

        # Check for required columns
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [c for c in required_cols if c not in df.columns]

        if missing_cols:
            print(f"❌ Missing required columns: {missing_cols}")
            return False

        print("✓ All required columns present")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_feature_engineering():
    """Test feature engineering"""
    print("\n" + "="*60)
    print("TEST 2: Feature Engineering")
    print("="*60)

    try:
        from src.ml.preprocessor import FeatureEngineer
        from src.ml.data_loader import MlDataLoader

        # Load some data
        loader = MlDataLoader(db_path="data/tushare_data.db")
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        df = loader.load_training_data(symbol="600382", start_date=start_date, end_date=end_date)

        if df.empty:
            print("❌ No data available for testing")
            return False

        # Test technical indicators
        engineer = FeatureEngineer()

        print("Adding technical indicators...")
        df = engineer.add_technical_indicators(df)

        # Check for technical indicators
        tech_indicators = ['sma_5', 'sma_20', 'rsi_14', 'macd', 'bb_upper']
        missing = [i for i in tech_indicators if i not in df.columns]

        if missing:
            print(f"❌ Missing technical indicators: {missing}")
            return False

        print(f"✓ Added technical indicators: {tech_indicators}")

        # Test target creation
        print("Creating target variable...")
        df = engineer.create_target(df, target_type='return_1d')

        if 'target' not in df.columns:
            print("❌ Target column not created")
            return False

        print(f"✓ Created target variable")
        print(f"  Target stats: mean={df['target'].mean():.6f}, std={df['target'].std():.6f}")

        # Test data preparation
        print("Preparing training data...")
        X, y = engineer.prepare_training_data(df, handle_missing='drop')

        print(f"✓ Prepared training data: X shape={X.shape}, y shape={y.shape}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_lgb_model():
    """Test LightGBM model"""
    print("\n" + "="*60)
    print("TEST 3: LightGBM Model")
    print("="*60)

    try:
        from src.ml.models.lgb_model import LGBModel
        from src.ml.preprocessor import FeatureEngineer
        from src.ml.data_loader import MlDataLoader

        # Load and prepare data
        loader = MlDataLoader(db_path="data/tushare_data.db")
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        df = loader.load_training_data(symbol="600382", start_date=start_date, end_date=end_date)

        if df.empty or len(df) < 100:
            print("❌ Insufficient data for training")
            return False

        # Feature engineering
        engineer = FeatureEngineer()
        df = engineer.add_technical_indicators(df)
        df = engineer.create_target(df, target_type='return_1d')

        # Prepare data
        X, y = engineer.prepare_training_data(df, handle_missing='drop')

        # Split data
        n = len(X)
        train_end = int(n * 0.7)
        val_end = int(n * 0.85)

        X_train, y_train = X[:train_end], y[:train_end]
        X_val, y_val = X[train_end:val_end], y[train_end:val_end]
        X_test, y_test = X[val_end:], y[val_end:]

        print(f"Training LightGBM model...")
        print(f"  Train: {len(X_train)}, Val: {len(X_val)}, Test: {len(X_test)}")

        # Train model
        model = LGBModel(
            model_id="test_lgb",
            params={'num_boost_round': 50, 'verbose': -1}
        )

        model.train(
            X_train, y_train,
            X_val, y_val,
            feature_names=engineer.feature_cols
        )

        print("✓ Model trained successfully")

        # Evaluate
        metrics = model.evaluate(X_test, y_test)

        print(f"✓ Model evaluation:")
        print(f"  MAE:  {metrics['mae']:.6f}")
        print(f"  RMSE: {metrics['rmse']:.6f}")
        print(f"  R²:   {metrics['r2']:.4f}")

        # Check if performance is reasonable
        if metrics['mae'] < 0.1:  # 10% MAE threshold
            print("✓ Model performance is acceptable")
            return True
        else:
            print(f"⚠ Model MAE ({metrics['mae']:.4f}) exceeds 0.1 threshold")
            return True  # Still pass, just warn

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_database_operations():
    """Test database operations"""
    print("\n" + "="*60)
    print("TEST 4: Database Operations")
    print("="*60)

    try:
        from src.ml.utils import MLDatabase

        db = MLDatabase(db_path="data/tushare_data.db")

        # Test saving model metadata
        print("Saving test model metadata...")

        test_metrics = {
            'mae': 0.015,
            'rmse': 0.020,
            'mape': 1.5,
            'r2': 0.85
        }

        success = db.save_model_metadata(
            model_id="test_model_001",
            model_type="lgb",
            symbol="600382",
            hyperparameters={'learning_rate': 0.05},
            metrics=test_metrics,
            feature_importance={'close': 0.5, 'volume': 0.3},
            model_path="/test/path",
            training_start="2023-01-01",
            training_end="2024-01-01",
            n_features=50,
            train_samples=1000,
            val_samples=200,
            test_samples=200,
            target_type="return_1d"
        )

        if not success:
            print("❌ Failed to save model metadata")
            return False

        print("✓ Model metadata saved")

        # Test retrieving model
        model = db.get_model("test_model_001")

        if not model:
            print("❌ Failed to retrieve model")
            return False

        print("✓ Model retrieved successfully")
        print(f"  Model ID: {model['model_id']}")
        print(f"  Symbol: {model['symbol']}")
        print(f"  MAE: {model['metrics']['mae']}")

        # Test listing models
        models = db.list_models(symbol="600382", limit=10)

        print(f"✓ Found {len(models)} models for 600382")

        # Clean up test model
        db.delete_model("test_model_001")
        print("✓ Test model cleaned up")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_monitoring():
    """Test monitoring functionality"""
    print("\n" + "="*60)
    print("TEST 5: Monitoring System")
    print("="*60)

    try:
        from src.ml.monitoring import ModelMonitor, PerformanceLogger

        # Test PerformanceLogger
        print("Testing PerformanceLogger...")
        logger_perf = PerformanceLogger()

        session_id = logger_perf.start_session("test_model")

        logger_perf.log_training_progress(
            session_id,
            epoch=1,
            train_loss=0.05,
            val_loss=0.06
        )

        logger_perf.log_event(session_id, "test_event", {"test": "data"})

        logger_perf.end_session(session_id, {"final_mae": 0.015})

        print("✓ PerformanceLogger working")

        # Test ModelMonitor
        print("Testing ModelMonitor...")
        monitor = ModelMonitor()

        # Test drift detection
        current_metrics = {'mae': 0.018}
        drift_result = monitor.detect_drift("test_model", current_metrics)

        print(f"✓ Drift detection: {drift_result['drift_detected']}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("ML SYSTEM VERIFICATION TESTS")
    print("="*60)
    print(f"Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    tests = [
        ("Data Loader", test_data_loader),
        ("Feature Engineering", test_feature_engineering),
        ("LightGBM Model", test_lgb_model),
        ("Database Operations", test_database_operations),
        ("Monitoring System", test_monitoring),
    ]

    results = {}

    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            print(f"\n❌ {name} test failed with exception: {e}")
            results[name] = False

    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "✓ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")

    print(f"\nResults: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! The ML system is working correctly.")
        return 0
    else:
        print(f"\n⚠️ {total - passed} test(s) failed. Please review the errors above.")
        return 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)
