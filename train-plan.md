# Fraud Model Training Plan

## Current Gaps

- Training may include raw non-numeric columns: `currency`, `merchant`, `timestamp`, `location`.
- Train/test split is random, not stratified.
- Fraud class imbalance is not handled.
- Only `precision` is logged, and it is calculated manually.
- No model artifact is logged or registered in MLflow.
- Hyperparameters are hardcoded in Python.
- No threshold tuning for fraud decisions.
- No validation for empty data, single-class labels, or missing features.

## Priority Plan

### 1. Fix Training Correctness

- Validate the dataset and label column.
- Fail clearly if labels contain only one class.
- Use an explicit feature list instead of `df.drop(...)`.
- Drop or encode raw categorical/time columns.
- Use stratified train/test split.
- Use `sklearn.metrics` for evaluation.

### 2. Make Features Explicit

Add model feature configuration:

```yaml
model:
  label_column: is_fraud
  feature_columns:
    - amount
    - user_id
    - hour_of_day
    - day_of_week
    - is_weekend
    - log_amount
    - is_card_testing
    - is_large_amount
    - is_very_large_amount
    - is_high_risk_merchant
    - is_suspicious_location
```

Training should only use configured columns and fail if any are missing.

### 3. Handle Class Imbalance

- Compute `scale_pos_weight = negative_count / positive_count`.
- Pass it to `XGBClassifier`.
- Log class counts and fraud rate to MLflow.

### 4. Improve Evaluation

Log these metrics:

- `precision`
- `recall`
- `f1`
- `roc_auc`
- `average_precision`
- confusion matrix
- classification report

Use predicted probabilities and tune the classification threshold, probably for best F1 or minimum recall.

### 5. Log and Register the Model

- Log the trained XGBoost model to MLflow.
- Include model signature and input example.
- Register the model as `fraud_detection`.
- Return the real `model_uri` from the Airflow task.

### 6. Move Parameters to Config

```yaml
model:
  test_size: 0.2
  random_state: 42
  threshold_metric: f1
  xgboost:
    n_estimators: 300
    max_depth: 4
    learning_rate: 0.05
    subsample: 0.8
    colsample_bytree: 0.8
    eval_metric: logloss
```

### 7. Add Focused Tests

- Feature selection.
- Missing feature validation.
- Single-class dataset failure.
- Class imbalance weight calculation.
- Threshold selection.

## Later Enhancements

- Encode merchant and location instead of dropping them.
- Use a time-based validation split.
- Add richer user, merchant, and location behavior features.
- Add model promotion rules.
- Add real-time inference for `fraud_predictions`.

## Recommended Order

1. Correctness
2. Metrics
3. MLflow model logging
4. Config cleanup
5. Tests
6. Better features
