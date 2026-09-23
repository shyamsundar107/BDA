-- ============================================================
-- STEP 1: Hive schema
-- Run this first (hive -f 01_hive_schema.hql) to set up/confirm
-- the base table and add the new tables Member 1 needs.
-- ============================================================

-- Your existing raw event table (from the synthetic + real hybrid
-- dataset described on Slide 10). Included here so the rest of the
-- scripts are self-contained — skip this CREATE if it already exists.
CREATE TABLE IF NOT EXISTS raw_traffic_events (
    vehicle_id      STRING,
    event_ts        TIMESTAMP,
    intersection_id STRING,
    lane            STRING,      -- North / South / East / West
    vehicle_type    STRING,
    speed           DOUBLE,
    waiting_time    DOUBLE,       -- seconds
    emergency_flag  BOOLEAN
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET;

-- ------------------------------------------------------------
-- NEW: historical_trend table (blocking — Member 1 request #2)
-- One row per intersection+lane+day. "trend" = is this lane
-- getting worse over time, normalized 0 (improving/stable) to
-- 1 (worsening fast). Computed by 03_historical_trend.py.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS historical_trend (
    intersection_id STRING,
    lane            STRING,
    trend_date      STRING,       -- date this trend value was computed for
    historical_trend DOUBLE       -- normalized 0-1
)
STORED AS PARQUET;

-- ------------------------------------------------------------
-- STRETCH: adjacency (which junctions are next to which)
-- Needed before the coordination signal means anything.
-- direction = the compass direction you travel FROM this
-- intersection TO the neighbor (helps later with routing).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS intersection_adjacency (
    intersection_id          STRING,
    neighbor_intersection_id STRING,
    direction                STRING,   -- N/S/E/W
    distance_m               DOUBLE
)
STORED AS PARQUET;

-- ------------------------------------------------------------
-- STRETCH: ambulance route/heading (direction of travel, not
-- just an on/off flag) — for corridor pre-clear.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ambulance_events (
    ambulance_id     STRING,
    event_ts         TIMESTAMP,
    intersection_id  STRING,        -- current/nearest intersection
    heading_deg      DOUBLE,        -- compass heading, 0-360
    next_intersection_id STRING,    -- predicted next junction on route
    eta_seconds      DOUBLE         -- estimated time to next_intersection_id
)
STORED AS PARQUET;

-- ------------------------------------------------------------
-- STRETCH: feedback loop (decision -> outcome), logged back
-- into historical data for the coordination model to learn from.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS decision_feedback (
    decision_id      STRING,
    intersection_id  STRING,
    lane             STRING,
    cycle_id         STRING,
    decision_ts      TIMESTAMP,
    green_time_sec   DOUBLE,        -- what the optimizer decided
    priority_score   DOUBLE,        -- score at decision time
    outcome_avg_wait_next_cycle DOUBLE,  -- measured after the fact
    outcome_queue_cleared       BOOLEAN
)
STORED AS PARQUET;
