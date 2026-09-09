CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

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