# ============================================================
# schema_harmonizer.py — Hypermarket Source Transformation
# MegaMart Data Platform | Bronze Layer Harmonization
# ============================================================
 
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType, StringType
from functools import reduce
from typing import Dict, Optional
import logging
 
logger = logging.getLogger(__name__)
 
# --- Payment method mapping (from schema registry) ---
PAYMENT_MAP_HYPERMARKET = {
    'TUNAI': 'CASH', 'CASH': 'CASH',
    'DEBIT': 'CARD',  'CREDIT': 'CARD', 'VISA': 'CARD', 'MASTER': 'CARD',
    'OVO': 'EWALLET', 'GOPAY': 'EWALLET', 'DANA': 'EWALLET', 'LINKAJA': 'EWALLET',
    'QRIS': 'QRIS'
}
 
def build_payment_map_expr(mapping: Dict[str, str], source_col: str) -> F.Column:
    """Build a CASE WHEN expression from a payment mapping dict."""
    expr = F.lit('UNKNOWN')
    for src_val, tgt_val in mapping.items():
        expr = F.when(F.upper(F.col(source_col)) == src_val, tgt_val).otherwise(expr)
    return expr
 
def transform_hypermarket(df: DataFrame, batch_id: str) -> DataFrame:
    """
    Transform raw.transactions (Hypermarket) -> unified bronze schema.
    Applies schema registry v1 mappings for source 'hypermarket_v1'.
    """
    logger.info(f'[hypermarket] Starting transform. Input rows: {df.count()}')
 
    payment_expr = build_payment_map_expr(PAYMENT_MAP_HYPERMARKET, 'payment_type')
 
    transformed = df.select(
        # --- Identity & Routing ---
        F.col('txn_id').cast(StringType())                          .alias('txn_id'),
        F.lit('HYPERMARKET')                                         .alias('source_system'),
        F.col('store_id').cast(StringType())                        .alias('store_id'),
 
        # --- Customer (nullable) ---
        F.col('member_id').cast(StringType())                       .alias('customer_id'),
 
        # --- Timestamp (normalize to UTC) ---
        F.to_timestamp('transaction_dt', 'yyyy-MM-dd HH:mm:ss')    .alias('txn_timestamp'),
 
        # --- Channel Classification ---
        F.lit('IN_STORE')                                            .alias('channel'),
        F.lit('HYPERMARKET')                                         .alias('sub_channel'),
 
        # --- Financials (safe cast, default 0 for nulls) ---
        F.coalesce(
            F.col('gross_amount').cast(DecimalType(15, 2)), F.lit(0.0)
        )                                                            .alias('total_amount'),
        F.coalesce(
            F.col('disc_amount').cast(DecimalType(15, 2)), F.lit(0.0)
        )                                                            .alias('discount_amount'),
        F.coalesce(
            F.col('vat_amount').cast(DecimalType(15, 2)), F.lit(0.0)
        )                                                            .alias('tax_amount'),
 
        # --- Payment Method Normalization ---
        payment_expr                                                 .alias('payment_method'),
 
        # --- Pipeline Metadata ---
        F.current_timestamp()                                        .alias('_ingestion_ts'),
        F.lit(batch_id)                                              .alias('_batch_id'),
        F.lit('hypermarket_v1')                                      .alias('_schema_version'),
 
        # --- Audit: raw snapshot (for DJP compliance) ---
        F.to_json(F.struct([F.col(c) for c in df.columns]))         .alias('_source_raw')
    )
 
    # --- Data Quality Checks ---
    result = apply_dq_checks(transformed, source='hypermarket')
    logger.info(f'[hypermarket] Transform complete. Output rows: {result.count()}')
    return result
 
 
def apply_dq_checks(df: DataFrame, source: str) -> DataFrame:
    """
    Add DQ flags. Rows failing critical checks are routed to dead-letter.
    Non-critical checks add _dq_warning flag only.
    """
    return df.withColumn(
        '_dq_flags',
        F.array(
            F.when(F.col('txn_id').isNull(),          F.lit('CRITICAL:NULL_TXN_ID')),
            F.when(F.col('txn_timestamp').isNull(),   F.lit('CRITICAL:NULL_TIMESTAMP')),
            F.when(F.col('total_amount') <= 0,        F.lit('CRITICAL:NON_POSITIVE_AMOUNT')),
            F.when(
                F.col('tax_amount') > F.col('total_amount') * 0.15,
                F.lit('WARNING:SUSPICIOUS_TAX')
            ),
            F.when(
                F.col('payment_method') == 'UNKNOWN',
                F.lit('WARNING:UNMAPPED_PAYMENT')
            ),
        ).cast('array<string>')
    ).withColumn(
        '_has_critical_error',
        F.exists('_dq_flags', lambda x: x.startswith('CRITICAL:'))
    )
 
 
def split_valid_invalid(df: DataFrame):
    """Separate clean rows from rows with critical DQ errors."""
    valid   = df.filter(~F.col('_has_critical_error'))
    invalid = df.filter( F.col('_has_critical_error'))
    return valid, invalid
 
 
def harmonize_all_sources(
    spark: SparkSession,
    source_configs: Dict,
    batch_id: str,
    watermark_ts: str
) -> DataFrame:
    """
    Orchestrate all source transformations and union into unified schema.
    Each source has its own transform function registered in SOURCE_REGISTRY.
    """
    SOURCE_REGISTRY = {
        'hypermarket': transform_hypermarket,
        # 'express':     transform_express,    # add new sources here
        # 'online':      transform_online,
        # 'wholesale':   transform_wholesale,
        # 'fresh':       transform_fresh,
    }
 
    unified_frames = []
    dead_letter_frames = []
 
    for source_name, transform_fn in SOURCE_REGISTRY.items():
        cfg = source_configs[source_name]
        raw_df = (
            spark.read
            .format('snowflake')
            .options(**cfg['conn'])
            .option('dbtable', cfg['table'])
            .option('sfTimezone', 'Asia/Jakarta')    # enforce timezone
            .load()
            .filter(F.col(cfg['watermark_col']) > watermark_ts)  # incremental
        )
 
        transformed = transform_fn(raw_df, batch_id)
        valid, invalid = split_valid_invalid(transformed)
 
        unified_frames.append(valid)
        dead_letter_frames.append(invalid.withColumn('_source', F.lit(source_name)))
 
        logger.info(
            f'[{source_name}] valid={valid.count()}, '
            f'dead_letter={invalid.count()}'
        )
 
    # Union must use same schema — enforce with unionByName
    unified = reduce(lambda a, b: a.unionByName(b), unified_frames)
    dead_letter = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True),
                         dead_letter_frames)
 
    return unified, dead_letter
