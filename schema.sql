-- postgresql schema

BEGIN;

CREATE TABLE  IF NOT EXISTS locations  (
    id     TEXT NOT NULL PRIMARY KEY,
    station_name TEXT NOT NULL,
    geometry geometry(Point, 4326) NOT NULL
);

CREATE TABLE IF NOT EXISTS timeseries  (
    id              BIGSERIAL PRIMARY KEY,

    location_id     TEXT NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    parameter_id    TEXT NOT NULL,
    qualifier_id    TEXT NOT NULL DEFAULT '', -- '' = observed
    forecast_date   TIMESTAMPTZ,  -- NULL = observed
    timestep        INTERVAL,
    units            TEXT
);

CREATE UNIQUE INDEX timeseries_unique
ON timeseries (
    location_id,
    parameter_id,
    qualifier_id,
    forecast_date
);

CREATE TABLE  IF NOT EXISTS timeseries_values (
    id BIGSERIAL PRIMARY KEY,

    series_id   BIGINT NOT NULL REFERENCES timeseries(id) ON DELETE CASCADE,
    time        TIMESTAMPTZ NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    flag        INTEGER,
    comment     TEXT,

    UNIQUE (series_id, time)
);

CREATE INDEX IF NOT EXISTS idx_locations_ts ON timeseries (location_id);
CREATE INDEX IF NOT EXISTS idx_parameter_ts ON timeseries (parameter_id);
CREATE INDEX IF NOT EXISTS idx_forecast_date_ts ON timeseries (forecast_date);
-- CREATE INDEX IF NOT EXISTS idx_time_tsv ON timeseries_values (series_id, time);

COMMIT;

