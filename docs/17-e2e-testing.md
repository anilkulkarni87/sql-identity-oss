# End-to-End Testing

Validation guide for the full Identity Resolution pipeline.

## Prerequisites
```bash
pip install numpy pyarrow faker
cd tools/scale_test
```

## 1. Generate Test Data
Create a synthetic dataset with known ground truth.
```bash
python generate_global_retail_idr.py --rows 100000 --output data/100k_test
```

## 2. Load and Initialize
Example for **DuckDB**:
```bash
# Load data
python load_duckdb.py --db test.db --input data/100k_test

# Initialize IDR schemas
idr init --platform duckdb --db test.db --reset

# Apply metadata config
cat setup_retail_metadata.sql | duckdb test.db
```

## 3. Run Pipeline
```bash
idr run --platform duckdb --db test.db --mode FULL
```

## 4. Validate Results
Compare IDR output against the known ground truth in the generated data.
```bash
python run_validation.py --platform duckdb --db test.db
```

### Key Metrics to Watch
*   **Precision**: % of entities correctly clustered together.
*   **Recall**: % of true matches found.
*   **Over-clustering**: Are distinct people merged? (False Positives)
*   **Under-clustering**: Are same people split? (False Negatives)

## 5. Clean Up
```bash
rm test.db
rm -rf data/100k_test
```
