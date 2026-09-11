CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

ALTER DATABASE pipeline_db
SET timezone TO 'Europe/Zurich';

SET timezone TO 'Europe/Zurich';

CREATE TABLE IF NOT EXISTS pv_data (
    time TIMESTAMPTZ NOT NULL,
    sm_id VARCHAR(50),
    production BIGINT,
    consumption BIGINT
);
SELECT create_hypertable('pv_data', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS weather_data (
    time TIMESTAMPTZ NOT NULL,
    station_id VARCHAR(50),
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    irradiance DOUBLE PRECISION
);
SELECT create_hypertable('weather_data', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS pv_estimates (
    time TIMESTAMPTZ NOT NULL,
    sm_id VARCHAR(50),
    station_id VARCHAR(50),
    irradiance DOUBLE PRECISION,
    expected_production DOUBLE PRECISION,
    expected_production_min DOUBLE PRECISION,
    expected_production_max DOUBLE PRECISION,
    actual_production BIGINT,
    deviation_w DOUBLE PRECISION
);
SELECT create_hypertable('pv_estimates', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS windpark_data (
    time TIMESTAMPTZ NOT NULL,
    windpark_id VARCHAR(50),
    rpm DOUBLE PRECISION
);
SELECT create_hypertable('windpark_data', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS windpark_estimates (
    time TIMESTAMPTZ NOT NULL,
    windpark_id VARCHAR(50),
    station_id VARCHAR(50),
    wind_speed DOUBLE PRECISION,
    expected_rpm_min DOUBLE PRECISION,
    expected_rpm_max DOUBLE PRECISION,
    actual_rpm DOUBLE PRECISION,
    deviation_rpm DOUBLE PRECISION
);
SELECT create_hypertable('windpark_estimates', 'time', if_not_exists => TRUE);