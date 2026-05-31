# ============================================================
# megamart_cdc_pipeline.py
# Production-Grade Incremental CDC Pipeline
# MegaMart Data Platform | Bronze Layer
# ============================================================
 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta import DeltaTable
from datetime import datetime, timedelta
from typing import Optional, Tuple
from dataclasses import dataclass
import logging, json, uuid, time
 
logger = logging.getLogger('megamart.cdc')
 
# ── Config ──────────────────────────────────────────────────
@dataclass
class PipelineConfig:
    source_name:       str
    snowflake_conn:    dict
    source_table:      str
    watermark_col:     str   = 'updated_at'
    late_arrival_days: int   = 7        # handle data up to 7 days late
    batch_id:          str   = ''
    target_path:       str   = 's3://megamart-datalake/bronze/transactions'
    dlq_path:          str   = 's3://megamart-datalake/dlq'
    watermark_path:    str   = 's3://megamart-datalake/meta/watermarks'
 
# ── Watermark Manager ───────────────────────────────────────
class WatermarkManager:
    '''Reads and writes watermark (last processed timestamp) per source.'''
 
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path  = path
 
    def get(self, source: str) -> Optional[str]:
        '''Return last watermark or None if first run.'''
        try:
            df = self.spark.read.json(f'{self.path}/{source}/')
            row = df.orderBy(F.col('updated_at').desc()).limit(1).collect()
            return row[0]['watermark_ts'] if row else None
        except Exception:
            logger.info(f'[{source}] No watermark found — first run.')
            return None
 
    def save(self, source: str, watermark_ts: str, batch_id: str):
        '''Persist new watermark after successful run.'''
        record = {
            'source':       source,
            'watermark_ts': watermark_ts,
            'batch_id':     batch_id,
            'updated_at':   datetime.utcnow().isoformat()
        }
        self.spark.createDataFrame([record]).write \
            .mode('append').json(f'{self.path}/{source}/')
        logger.info(f'[{source}] Watermark saved: {watermark_ts}')
 
# ── Extractor ───────────────────────────────────────────────
def extract_incremental(
    spark: SparkSession,
    cfg: PipelineConfig,
    last_watermark: Optional[str]
) -> DataFrame:
    '''
    Load only new/changed rows from Snowflake using watermark.
    Handles late-arriving data by looking back `late_arrival_days` days.
    '''
    # On first run, load last 7 days as bootstrap
    if last_watermark is None:
        start_ts = (datetime.utcnow() - timedelta(days=cfg.late_arrival_days)).isoformat()
        logger.info(f'[{cfg.source_name}] First run — bootstrapping from {start_ts}')
    else:
        # Look back extra days to catch late-arriving records
        lookback = datetime.fromisoformat(last_watermark) - timedelta(days=cfg.late_arrival_days)
        start_ts = lookback.isoformat()
        logger.info(f'[{cfg.source_name}] Incremental load from {start_ts} (watermark={last_watermark})')
 
    df = (
        spark.read
        .format('snowflake')
        .options(**cfg.snowflake_conn)
        .option('dbtable', cfg.source_table)
        .option('sfTimezone', 'Asia/Jakarta')
        .load()
        .filter(F.col(cfg.watermark_col) >= start_ts)  # incremental filter
        .withColumn('_extracted_at', F.current_timestamp())
        .withColumn('_batch_id', F.lit(cfg.batch_id))
    )
 
    count = df.count()
    logger.info(f'[{cfg.source_name}] Extracted {count:,} rows')
    return df
 
# ── DQ Checks ───────────────────────────────────────────────
def apply_dq_checks(df: DataFrame, source: str) -> DataFrame:
    '''Tag each row with DQ flags. CRITICAL rows go to dead-letter.'''
    return df.withColumn(
        '_dq_flags',
        F.array(
            F.when(F.col('txn_id').isNull(),
                   F.lit('CRITICAL:NULL_TXN_ID')),
            F.when(F.col('txn_timestamp').isNull(),
                   F.lit('CRITICAL:NULL_TIMESTAMP')),
            F.when(F.col('total_amount') <= 0,
                   F.lit('CRITICAL:NON_POSITIVE_AMOUNT')),
            F.when(F.col('txn_timestamp') > F.current_timestamp(),
                   F.lit('CRITICAL:FUTURE_TIMESTAMP')),
            F.when(F.col('tax_amount') > F.col('total_amount') * 0.15,
                   F.lit('WARNING:SUSPICIOUS_TAX')),
            F.when(F.col('payment_method') == 'UNKNOWN',
                   F.lit('WARNING:UNMAPPED_PAYMENT')),
        ).cast('array<string>')
    ).withColumn(
        '_has_critical_error',
        F.exists('_dq_flags', lambda x: x.startswith('CRITICAL:'))
    )
 
def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    valid   = df.filter(~F.col('_has_critical_error')).drop('_has_critical_error')
    invalid = df.filter( F.col('_has_critical_error'))
    return valid, invalid
 
# ── MERGE / Upsert ──────────────────────────────────────────
def merge_to_bronze(spark: SparkSession, new_df: DataFrame, target_path: str):
    '''
    MERGE (upsert) into Delta Lake bronze table.
    - New txn_id: INSERT
    - Existing txn_id with newer timestamp: UPDATE
    - Duplicate re-runs: no-op (idempotent)
    '''
    try:
        target = DeltaTable.forPath(spark, target_path)
        (
            target.alias('tgt')
            .merge(
                new_df.alias('src'),
                'tgt.txn_id = src.txn_id AND tgt.source_system = src.source_system'
            )
            # UPDATE: only if incoming record is newer
            .whenMatchedUpdate(
                condition='src.txn_timestamp > tgt.txn_timestamp',
                set={col: f'src.{col}' for col in new_df.columns}
            )
            # INSERT: brand-new transaction ID
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f'MERGE complete into {target_path}')
    except Exception:
        # First run: table does not exist yet
        logger.info('Target table not found — creating new Delta table')
        (
            new_df.write
            .format('delta')
            .partitionBy('source_system', 'txn_date')
            .mode('overwrite')
            .save(target_path)
        )
 
# ── Dead-Letter Queue ────────────────────────────────────────
def write_dead_letter(df: DataFrame, dlq_path: str, source: str, batch_id: str):
    '''Persist invalid rows to DLQ for investigation and reprocessing.'''
    count = df.count()
    if count == 0:
        return
    (
        df.withColumn('_dlq_source',   F.lit(source))
          .withColumn('_dlq_batch_id', F.lit(batch_id))
          .withColumn('_dlq_ts',       F.current_timestamp())
          .write
          .format('delta')
          .partitionBy('_dlq_source')
          .mode('append')
          .save(f'{dlq_path}/transactions/')
    )
    logger.warning(f'[{source}] {count:,} rows sent to dead-letter queue')
 
# ── Metrics ─────────────────────────────────────────────────
def emit_metrics(metrics: dict):
    '''Emit pipeline run metrics (to CloudWatch / Grafana / Slack).'''
    logger.info(f'PIPELINE_METRICS: {json.dumps(metrics)}')
    # TODO: push to CloudWatch / Grafana via boto3 / prometheus_client
 
# ── Orchestrator ────────────────────────────────────────────
def run_pipeline(spark: SparkSession, cfg: PipelineConfig):
    '''Main pipeline orchestrator. Idempotent — safe to retry.'''
    start_time    = time.time()
    cfg.batch_id  = f'{cfg.source_name}_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}_{uuid.uuid4().hex[:8]}'
    wm_manager    = WatermarkManager(spark, cfg.watermark_path)
    last_watermark = wm_manager.get(cfg.source_name)
 
    try:
        # 1. Extract
        raw_df = extract_incremental(spark, cfg, last_watermark)
        if raw_df.count() == 0:
            logger.info(f'[{cfg.source_name}] No new data — skipping.')
            return
 
        # 2. Transform (from schema_harmonizer.py — Task 1b)
        from schema_harmonizer import SOURCE_REGISTRY
        transform_fn = SOURCE_REGISTRY[cfg.source_name]
        transformed  = transform_fn(raw_df, cfg.batch_id)
        transformed  = transformed.withColumn(
            'txn_date',
            F.to_date('txn_timestamp')   # partition column
        )
 
        # 3. DQ checks
        checked        = apply_dq_checks(transformed, cfg.source_name)
        valid_df, bad_df = split_valid_invalid(checked)
 
        # 4. MERGE to Delta Lake (idempotent upsert)
        merge_to_bronze(spark, valid_df, cfg.target_path)
 
        # 5. Dead-letter queue
        write_dead_letter(bad_df, cfg.dlq_path, cfg.source_name, cfg.batch_id)
 
        # 6. Advance watermark (only after successful write)
        new_watermark = transformed.agg(F.max(cfg.watermark_col)).collect()[0][0]
        wm_manager.save(cfg.source_name, str(new_watermark), cfg.batch_id)
 
        # 7. Emit metrics
        emit_metrics({
            'source':       cfg.source_name,
            'batch_id':     cfg.batch_id,
            'rows_extracted': raw_df.count(),
            'rows_valid':     valid_df.count(),
            'rows_dead_letter': bad_df.count(),
            'duration_sec': round(time.time() - start_time, 2),
            'status':       'SUCCESS'
        })
 
    except Exception as e:
        logger.error(f'[{cfg.source_name}] Pipeline FAILED: {e}', exc_info=True)
        emit_metrics({'source': cfg.source_name, 'batch_id': cfg.batch_id,
                      'status': 'FAILED', 'error': str(e)})
        raise   # re-raise so Airflow/Step Functions marks task as failed
 
# ── Entry point ──────────────────────────────────────────────
if __name__ == '__main__':
    spark = SparkSession.builder \
        .appName('megamart-cdc-pipeline') \
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension') \
        .config('spark.sql.catalog.spark_catalog',
                'org.apache.spark.sql.delta.catalog.DeltaCatalog') \
        .getOrCreate()
 
    SOURCE_CONFIGS = {
        'hypermarket': PipelineConfig(
            source_name    = 'hypermarket',
            snowflake_conn = {'sfURL': '...', 'sfDatabase': 'hypermart_prod'},
            source_table   = 'raw.transactions',
            watermark_col  = 'updated_at',
        ),
        # 'express':   PipelineConfig(...),
        # 'online':    PipelineConfig(...),
        # 'wholesale': PipelineConfig(...),
        # 'fresh':     PipelineConfig(...),
    }
 
    for source_name, cfg in SOURCE_CONFIGS.items():
        run_pipeline(spark, cfg)


# test