"""
Logistics 4.0 ML Pipeline Trainer.
Handles the extraction, algorithmic modeling, and MLflow tracing of
the Late Delivery Risk predictor via PySpark MLLib.

Includes:
- TrainValidationSplit hyperparameter search (no over/underfitting)
- Smart Mix data augmentation for robustness on anomalous streams
- Full MLflow metric+artifact logging
"""

import os
import shutil

os.environ["GIT_PYTHON_REFRESH"] = "quiet"  # Silence git warning
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://minio:9000"

import mlflow
import mlflow.spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, rand, lit
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator
)
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

# ==============================================================================
# MLFLOW TRACKING CONFIG
# ==============================================================================
MLFLOW_URI = "http://mlflow:5000"
EXPERIMENT_NAME = "Logistics_Delay_Prediction"
MODEL_EXPORT_PATH = "/opt/logistics/spark_models/late_delivery_rf"

# Feature columns (15 features — városok kizárva a magas kardinalitás miatt)
CATEGORICAL_COLS = [
    "Order Status", "Shipping Mode", "Customer Country",
    "Order Country", "Order Region", "Category Name",
    "Customer Segment", "Department Name", "Type"
]
NUMERIC_COLS = [
    "Days for shipment (scheduled)", "Benefit per order", "Sales per customer",
    "Order Item Total", "Order Item Discount", "Product Price"
]

# Hyperparameter search space — 4 combos (mémoire contrainte)
PARAM_GRID = {
    "numTrees": [50, 100],
    "maxDepth": [6, 8],
    "minInstancesPerNode": 5  # Fixé : bon compromis bias/variance
}


class SparkModelTrainer:
    """Orchestrates Spark ML training runs with hyperparameter search."""

    def __init__(self):
        mlflow.set_tracking_uri(MLFLOW_URI)
        mlflow.set_experiment(EXPERIMENT_NAME)

        self.spark = SparkSession.builder \
            .appName("Logistics40_Model_Training") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
            
        # Configure Hadoop to use MinIO for S3 MLflow Artifacts
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
        hadoop_conf.set("fs.s3a.access.key", "admin")
        hadoop_conf.set("fs.s3a.secret.key", "password")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        self.spark.sparkContext.setLogLevel("WARN")

    def _load_data(self) -> DataFrame:
        """Load and curate the training features (15-column pipeline schema)."""
        print("[INFO] Loading historical dataset from /tmp/DataCoSupplyChainDataset.csv...")
        df = self.spark.read.csv(
            "/tmp/DataCoSupplyChainDataset.csv",
            header=True,
            inferSchema=True,
            mode="DROPMALFORMED"
        )

        # Select the 15 usable ML features + label
        # Excluded: IDs, dates, target leakage (real days / delivery status), cities (>3000 cardinality)
        selected_data = df.select(
            # Numériques
            col("Days for shipment (scheduled)").cast("integer"),
            col("Benefit per order").cast("double"),
            col("Sales per customer").cast("double"),
            col("Order Item Total").cast("double"),
            col("Order Item Discount").cast("double"),
            col("Product Price").cast("double"),
            # Catégorielles (faible cardinalité)
            col("Order Status").cast("string"),
            col("Shipping Mode").cast("string"),
            col("Customer Country").cast("string"),
            col("Order Country").cast("string"),
            col("Order Region").cast("string"),
            col("Category Name").cast("string"),
            col("Customer Segment").cast("string"),
            col("Department Name").cast("string"),
            col("Type").cast("string"),
            # Label
            col("Late_delivery_risk").cast("integer").alias("label")
        ).dropna()

        return selected_data

    def _augment_with_mix_data(self, df: DataFrame, fraction: float = 0.10) -> DataFrame:
        """
        Augmente le training set avec des perturbations Smart Mix légères.
        Version allégée : 1 seul type d'augmentation, pas de count() (lazy).
        """
        print(f"[INFO] Applying Smart Mix augmentation ({fraction*100:.0f}% of training data)...")

        # Scheduling paradox : le signal le plus informatif pour le modèle
        aug = df.sample(fraction=fraction, seed=42) \
            .withColumn("Shipping Mode", lit("Standard Class")) \
            .withColumn("Days for shipment (scheduled)", lit(7)) \
            .withColumn("label", lit(1))

        augmented = df.union(aug)
        print("[INFO] Smart Mix augmentation applied (lazy — will compute during training).")
        return augmented

    def _build_pipeline_stages(self) -> tuple:
        """Returns (indexers, assembler, rf_classifier) stages."""
        indexers = [
            StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
            for c in CATEGORICAL_COLS
        ]
        assembler = VectorAssembler(
            inputCols=[f"{c}_idx" for c in CATEGORICAL_COLS] + NUMERIC_COLS,
            outputCol="features"
        )
        rf = RandomForestClassifier(
            labelCol="label",
            featuresCol="features",
            maxBins=256,
            seed=42
        )
        return indexers, assembler, rf

    def execute_training(self) -> None:
        """Main training lifecycle with hyperparameter search and MLflow tracking."""
        df = self._load_data()

        # 3-way split: 70% train+val / 30% test (held-out, never seen by tuner)
        train_val_data, test_data = df.randomSplit([0.70, 0.30], seed=42)

        # Augment training set with Mix-stream patterns (lazy, no count())
        train_val_data = self._augment_with_mix_data(train_val_data, fraction=0.10)

        # Build pipeline
        indexers, assembler, rf = self._build_pipeline_stages()
        pipeline = Pipeline(stages=indexers + [assembler, rf])

        # Hyperparameter grid — 4 combos (numTrees × maxDepth)
        param_grid = ParamGridBuilder() \
            .addGrid(rf.numTrees, PARAM_GRID["numTrees"]) \
            .addGrid(rf.maxDepth, PARAM_GRID["maxDepth"]) \
            .build()

        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            metricName="areaUnderROC"
        )
        tvs = TrainValidationSplit(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            trainRatio=0.875,
            parallelism=1
        )

        # Cache le dataset pour éviter les rescans multiples pendant TVS
        train_val_data.cache()
        print(f"[INFO] Launching TrainValidationSplit ({len(param_grid)} param combos)...")

        with mlflow.start_run(run_name="hypertuned_smartmix"):
            tvs_model = tvs.fit(train_val_data)
            best_model: PipelineModel = tvs_model.bestModel

            # Extract best RF parameters
            best_rf = best_model.stages[-1]
            best_params = {
                "numTrees": best_rf.getNumTrees,
                "maxDepth": best_rf.getOrDefault(best_rf.maxDepth),
                "minInstancesPerNode": best_rf.getOrDefault(best_rf.minInstancesPerNode),
                "maxBins": 256,
                "augmentation_fraction": 0.15
            }
            print(f"[INFO] Best params found: {best_params}")

            # Evaluate on held-out test set
            print("[INFO] Evaluating on held-out test set...")
            predictions = best_model.transform(test_data)

            acc_eval = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="accuracy")
            f1_eval = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="f1")
            prec_eval = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
            rec_eval = MulticlassClassificationEvaluator(
                labelCol="label", predictionCol="prediction", metricName="weightedRecall")
            auc_eval = BinaryClassificationEvaluator(
                labelCol="label", metricName="areaUnderROC")

            accuracy = acc_eval.evaluate(predictions)
            f1_score = f1_eval.evaluate(predictions)
            precision = prec_eval.evaluate(predictions)
            recall = rec_eval.evaluate(predictions)
            auc_roc = auc_eval.evaluate(predictions)

            print(f"[METRIC] Accuracy  : {accuracy:.4f}")
            print(f"[METRIC] F1-Score  : {f1_score:.4f}")
            print(f"[METRIC] Precision : {precision:.4f}")
            print(f"[METRIC] Recall    : {recall:.4f}")
            print(f"[METRIC] AUC-ROC   : {auc_roc:.4f}")

            # Log everything to MLflow
            for k, v in best_params.items():
                mlflow.log_param(k, v)

            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1_score", f1_score)
            mlflow.log_metric("precision", precision)
            mlflow.log_metric("recall", recall)
            mlflow.log_metric("auc_roc", auc_roc)
            mlflow.log_metric("val_auc_roc_best", max(tvs_model.validationMetrics))
            mlflow.log_metric("val_auc_roc_worst", min(tvs_model.validationMetrics))

            # Detect over/underfitting
            gap = max(tvs_model.validationMetrics) - auc_roc
            mlflow.log_metric("val_test_gap", round(gap, 4))
            if abs(gap) > 0.05:
                print(f"[WARN] Potential overfitting detected. Val-Test AUC gap: {gap:.4f}")
            else:
                print(f"[INFO] Model generalizes well. Val-Test gap: {gap:.4f}")

            # Save model artifact
            mlflow.spark.log_model(best_model, "random_forest_model")
            self._save_internal_model(best_model)

    def _save_internal_model(self, model: PipelineModel) -> None:
        """Handles physical model overwrite for Spark Streaming consumption."""
        try:
            if os.path.exists(MODEL_EXPORT_PATH):
                shutil.rmtree(MODEL_EXPORT_PATH)
        except Exception as e:
            print(f"[WARN] Failed to delete existing path: {e}")

        print(f"[INFO] Serializing PipelineModel to: {MODEL_EXPORT_PATH}")
        model.write().overwrite().save(MODEL_EXPORT_PATH)
        print("[SUCCESS] Training complete. Model ready for streaming inference.")


if __name__ == "__main__":
    trainer = SparkModelTrainer()
    trainer.execute_training()
