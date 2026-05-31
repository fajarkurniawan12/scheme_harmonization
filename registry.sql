-- ============================================================
-- schema_registry DDL (stored in Snowflake / meta-database)
-- ============================================================
 
CREATE TABLE IF NOT EXISTS meta.schema_registry (
    registry_id       VARCHAR(50)   NOT NULL,   -- UUID
    source_id         VARCHAR(50)   NOT NULL,   -- e.g. 'hypermarket_v1'
    source_name       VARCHAR(50)   NOT NULL,   -- human label
    source_field      VARCHAR(100)  NOT NULL,   -- original column name
    target_field      VARCHAR(100)  NOT NULL,   -- unified column name
    transform_fn      VARCHAR(50)   NOT NULL,   -- e.g. DIRECT, CAST_DECIMAL
    transform_params  VARIANT,                  -- JSON params if needed
    data_type         VARCHAR(50)   NOT NULL,
    is_nullable       BOOLEAN       NOT NULL DEFAULT TRUE,
    is_pii            BOOLEAN       NOT NULL DEFAULT FALSE,  -- mask in non-prod
    version           INTEGER       NOT NULL DEFAULT 1,
    valid_from        DATE          NOT NULL,
    valid_to          DATE,                     -- NULL = currently active
    created_by        VARCHAR(100),
    notes             VARCHAR(500),
    CONSTRAINT pk_registry PRIMARY KEY (source_id, source_field, version)
);
 
-- View: currently active mappings only
CREATE VIEW meta.v_active_schema_registry AS
SELECT * FROM meta.schema_registry
WHERE valid_to IS NULL OR valid_to > CURRENT_DATE;
 
-- View: detect schema drift (new source columns not in registry)
CREATE VIEW meta.v_schema_drift_alerts AS
SELECT
    i.table_schema || '.' || i.table_name  AS source_table,
    i.column_name                           AS unregistered_column,
    i.data_type                             AS source_data_type,
    CURRENT_TIMESTAMP                       AS detected_at
FROM information_schema.columns i
LEFT JOIN meta.v_active_schema_registry r
    ON  r.source_field = i.column_name
WHERE r.source_field IS NULL
  AND i.table_schema IN ('raw');   -- only watch raw/source layer
