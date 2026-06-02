from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta import DeltaTable
import logging
 
logger = logging.getLogger('megamart.customer360')
 
def build_transaction_metrics(txn: DataFrame, run_date: str) -> DataFrame:
    '''
    Step 1 — Aggregate transaction metrics per customer.
    Uses exact formulas from assessment spec.
    '''
    # Window for spend trend: last 3 months vs previous 3 months
    w_monthly = Window.partitionBy('customer_id', 'txn_month')
 
    txn_with_month = txn.withColumn(
        'txn_month', F.trunc('txn_timestamp', 'MM')
    ).withColumn(
        'net_amount', F.col('total_amount') - F.col('discount_amount')
    )
 
    # Monthly aggregates for spend trend
    monthly = txn_with_month.groupBy('customer_id','txn_month').agg(
        F.sum('net_amount').alias('monthly_spend')
    )
 
    # Last 3 months vs previous 3 months (relative to run_date)
    last3_start  = F.add_months(F.lit(run_date), -3)
    prev3_start  = F.add_months(F.lit(run_date), -6)
 
    last3 = monthly.filter(F.col('txn_month') >= last3_start) \
        .groupBy('customer_id').agg(F.avg('monthly_spend').alias('last_3m_avg'))
    prev3 = monthly.filter(
        (F.col('txn_month') >= prev3_start) &
        (F.col('txn_month') <  last3_start)
    ).groupBy('customer_id').agg(F.avg('monthly_spend').alias('prev_3m_avg'))
 
    # Core metrics per customer
    metrics = txn.groupBy('customer_id').agg(
        F.countDistinct('txn_id')                   .alias('total_transactions'),
        F.sum(F.col('total_amount')-F.col('discount_amount')).alias('total_spend'),
        F.max('txn_timestamp').cast('date')          .alias('last_purchase_date'),
        F.min('txn_timestamp').cast('date')          .alias('first_purchase_date'),
    )
 
    # Preferred channel (max txn count)
    channel_rank = txn.groupBy('customer_id','channel').agg(
        F.count('txn_id').alias('ch_cnt')
    ).withColumn('rn', F.row_number().over(
        Window.partitionBy('customer_id').orderBy(F.col('ch_cnt').desc())
    )).filter(F.col('rn')==1).select('customer_id',
        F.col('channel').alias('preferred_channel')
    )
 
    # Preferred category (max spend) — requires txn joined with products
    # Assumed category_l1 available on silver.fact_transactions
    cat_rank = txn.groupBy('customer_id','category_l1').agg(
        F.sum(F.col('total_amount')-F.col('discount_amount')).alias('cat_spend')
    ).withColumn('rn', F.row_number().over(
        Window.partitionBy('customer_id').orderBy(F.col('cat_spend').desc())
    )).filter(F.col('rn')==1).select('customer_id',
        F.col('category_l1').alias('preferred_category')
    )
 
    # Combine all metrics
    combined = metrics \
        .join(last3,    'customer_id', 'left') \
        .join(prev3,    'customer_id', 'left') \
        .join(channel_rank, 'customer_id', 'left') \
        .join(cat_rank, 'customer_id', 'left')
 
    # Derived metrics (exact formulas from spec)
    return combined.withColumn(
        'avg_basket_size',
        F.col('total_spend') / F.greatest(F.col('total_transactions'), F.lit(1))
    ).withColumn(
        'days_since_last_purchase',
        F.datediff(F.lit(run_date), F.col('last_purchase_date'))
    ).withColumn(
        'avg_monthly_spend',
        F.col('total_spend') / F.greatest(
            F.months_between(F.lit(run_date), F.col('first_purchase_date')), F.lit(1)
        )
    ).withColumn(
        'spend_trend',
        F.when(F.col('last_3m_avg') > F.col('prev_3m_avg') * 1.1, 'INCREASING')
         .when(F.col('last_3m_avg') < F.col('prev_3m_avg') * 0.9, 'DECREASING')
         .otherwise('STABLE')
    )
 
def build_customer_360(spark: SparkSession, run_date: str):
    '''
    Main pipeline — joins silver customers + transactions
    and writes to gold.dim_customer_360 (partitioned by province).
    '''
    logger.info(f'Building Customer 360 for run_date={run_date}')
 
    # Load silver layer
    customers = spark.read.format('delta') \
        .load('s3://megamart-datalake/silver/dim_customers/') \
        .filter(F.col('is_current') == 1)  # only current SCD row
 
    txn = spark.read.format('delta') \
        .load('s3://megamart-datalake/silver/fact_transactions/')
 
    txn_metrics = build_transaction_metrics(txn, run_date)
 
    # LEFT JOIN: keep all customers even those with no transactions
    result = customers.join(txn_metrics, 'customer_id', 'left')
 
    # customers with no transactions → set metrics to 0 or NULL
    result = result.withColumn(
        'days_since_registration',
        F.datediff(F.lit(run_date), F.col('registration_date'))
    ).withColumn(
        'total_transactions',    F.coalesce('total_transactions',    F.lit(0))
    ).withColumn(
        'total_spend',           F.coalesce('total_spend',           F.lit(0.0))
    ).withColumn(
        'avg_basket_size',       F.coalesce('avg_basket_size',       F.lit(0.0))
    ).withColumn(
        'avg_monthly_spend',     F.coalesce('avg_monthly_spend',     F.lit(0.0))
    ).withColumn(
        'spend_trend',           F.coalesce('spend_trend',           F.lit('NO_DATA'))
    ).withColumn(
        '_run_date', F.lit(run_date)
    )
 
    # DQ Checks
    dq_fail = result.filter(
        F.col('customer_id').isNull() |
        (F.col('total_spend') < 0)   |
        (F.col('days_since_registration') < 0)
    ).count()
    if dq_fail > 0:
        logger.warning(f'DQ: {dq_fail} rows failed checks in Customer 360')
 
    # Write to Gold — partitioned by province for efficient querying
    (
        result.write
        .format('delta')
        .partitionBy('province', '_run_date')
        .mode('overwrite')
        .option('replaceWhere', f"_run_date = '{run_date}'")
        .save('s3://megamart-datalake/gold/dim_customer_360/')
    )
    logger.info(f'Customer 360 written. Total customers: {result.count():,}')
