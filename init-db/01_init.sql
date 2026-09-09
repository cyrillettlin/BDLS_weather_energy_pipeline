CREATE TABLE IF NOT EXISTS pv_data (
    time TIMESTAMPTZ NOT NULL,
    station_id VARCHAR(50),
    pv_power DOUBLE PRECISION,
    load_power DOUBLE PRECISION,
    grid_power DOUBLE PRECISION
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