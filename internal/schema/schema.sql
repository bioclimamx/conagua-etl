-- schema_version: 1
--
-- Bioclima SQLite schema — the offline processing substrate.
-- Applied idempotently (CREATE TABLE IF NOT EXISTS) on every ingest start.
--
-- Identity model:
--   stations.id          INTEGER surrogate, never user-facing.
--   stations.source      one of 6 enumerated sources (CHECK-constrained).
--   stations.external_id the upstream identifier (e.g. CONAGUA "76225").
--   UNIQUE (source, external_id) is the natural key; child tables FK to id.

CREATE TABLE IF NOT EXISTS stations (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    source                     TEXT NOT NULL
                                 CHECK (source IN (
                                   'conagua_conventional',
                                   'conagua_ema',
                                   'inifap',
                                   'nasa_power',
                                   'era5',
                                   'meta')),
    external_id                TEXT NOT NULL,
    name                       TEXT NOT NULL,
    state                      TEXT,
    municipality               TEXT,
    lat                        REAL,
    lon                        REAL,
    altitude_m                 REAL,
    status                     TEXT,
    first_year                 INTEGER,
    last_year                  INTEGER,
    -- WMO-No. 1203 §4.4.2 data completeness, scored per 30-year
    -- reference period over the three core principal variables
    -- (tmax, tmin, precip — WMO §4.2 Table 1). Two companion systems
    -- stored side by side so product-side UX can pick either:
    --   _bin_  — share of the 36 (3 vars × 12 months) cells where
    --            years_with_data ≥ 24 (the WMO 80% rule). Range [0,1].
    --   _cont_ — mean of min(years_with_data, 30) / 30 across the same
    --            36 cells. A density measure, not a threshold test.
    -- NULL when no extras rows exist for the period at all (file was
    -- missing or carried no AÑOS CON DATOS data); 0 when cells exist
    -- but all passed through with NULL counts.
    wmo_completeness_bin_1961_1990  REAL,
    wmo_completeness_bin_1971_2000  REAL,
    wmo_completeness_bin_1981_2010  REAL,
    wmo_completeness_bin_1991_2020  REAL,
    wmo_completeness_cont_1961_1990 REAL,
    wmo_completeness_cont_1971_2000 REAL,
    wmo_completeness_cont_1981_2010 REAL,
    wmo_completeness_cont_1991_2020 REAL,
    UNIQUE (source, external_id)
);

-- monthly_normals carries CONAGUA's observed monthly climatology.
-- Source is encoded by *table*: a row here is CONAGUA-observed. RH
-- is intentionally absent — the CONAGUA conventional archive
-- publishes zero observed humidity, and POWER's reanalysis RH lives
-- in monthly_supplement keyed by cell. Consumers that want RH for a
-- station resolve through station_power_cell → monthly_supplement.
CREATE TABLE IF NOT EXISTS monthly_normals (
    station_id INTEGER NOT NULL REFERENCES stations(id),
    period     TEXT NOT NULL
                CHECK (period IN ('1961-1990', '1971-2000', '1981-2010', '1991-2020')),
    month      INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    tmax       REAL,
    tmin       REAL,
    tmean      REAL,
    precip     REAL,
    evap       REAL,
    PRIMARY KEY (station_id, period, month)
);

-- monthly_normals_extras carries the per-(station, period, month) data
-- that CONAGUA's normals files publish beyond the NORMAL row: extremes
-- (highest/lowest monthly and daily values + the year or date they
-- occurred) and the per-field AÑOS CON DATOS count that feeds WMO
-- completeness scoring.
--
-- Source-agnostic *in concept*: any rich climate source could populate
-- these fields. CONAGUA is the only one that hands them to us
-- pre-computed today; future NASA POWER / ERA5 / INIFAP ingests that
-- compute their own extremes can populate the same columns.
--
-- Keyed by (station_id, period, month) and not REFERENCES-linked to
-- monthly_normals so that a station can have an "extras" row with no
-- corresponding NORMAL row (or vice versa) without hitting an FK
-- violation. The CHECK constraints match monthly_normals for period
-- and month so the two tables stay in step.
CREATE TABLE IF NOT EXISTS monthly_normals_extras (
    station_id INTEGER NOT NULL REFERENCES stations(id),
    period     TEXT NOT NULL
                CHECK (period IN ('1961-1990', '1971-2000', '1981-2010', '1991-2020')),
    month      INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),

    -- Temperature extremes
    tmax_monthly_extreme        REAL,
    tmax_monthly_extreme_year   INTEGER,
    tmax_daily_extreme          REAL,
    tmax_daily_extreme_date     TEXT,   -- ISO 'YYYY-MM-DD'
    tmin_monthly_extreme        REAL,
    tmin_monthly_extreme_year   INTEGER,
    tmin_daily_extreme          REAL,
    tmin_daily_extreme_date     TEXT,

    -- Precipitation extremes
    precip_monthly_extreme      REAL,
    precip_monthly_extreme_year INTEGER,
    precip_daily_extreme        REAL,
    precip_daily_extreme_date   TEXT,

    -- AÑOS CON DATOS per field (years in the reference period with
    -- valid data for that calendar month). Drives WMO completeness.
    tmax_years_with_data        INTEGER,
    tmin_years_with_data        INTEGER,
    tmean_years_with_data       INTEGER,
    precip_years_with_data      INTEGER,
    evap_years_with_data        INTEGER,

    -- Rainfall frequency (NÚMERO DE DÍAS CON LLUVIA section)
    rain_days                   REAL,
    rain_days_years_with_data   INTEGER,

    PRIMARY KEY (station_id, period, month)
);

CREATE TABLE IF NOT EXISTS daily_observations (
    station_id INTEGER NOT NULL REFERENCES stations(id),
    date       TEXT NOT NULL,
    tmax       REAL,
    tmin       REAL,
    precip     REAL,
    evap       REAL,
    PRIMARY KEY (station_id, date)
);

CREATE TABLE IF NOT EXISTS parsing_warnings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id  INTEGER,
    source_file TEXT,
    line        INTEGER,
    severity    TEXT NOT NULL CHECK (severity IN ('warn', 'error')),
    issue       TEXT NOT NULL
);

-- ingest_runs records one row per `ingest` invocation. A row is inserted
-- with status='running' at the start of a run and transitioned to
-- 'complete' (or left as 'running' on abrupt termination — the scheduled
-- `validate`/`publish` work may later reconcile these). Carries enough
-- provenance for downstream consumers to know which bytes a DB was built
-- from: the snapshot date under conagua-raw/, the sink kind, and the
-- etl git sha at build time.
CREATE TABLE IF NOT EXISTS ingest_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at          TEXT NOT NULL,
    finished_at         TEXT,
    snapshot_date       TEXT NOT NULL,
    sink_kind           TEXT NOT NULL,
    etl_git_sha         TEXT,
    status              TEXT NOT NULL
                          CHECK (status IN ('running', 'complete', 'aborted')),
    stations_attempted  INTEGER,
    stations_succeeded  INTEGER,
    stations_failed     INTEGER,
    daily_rows          INTEGER,
    normals_rows        INTEGER,
    extras_rows         INTEGER,
    warnings_total      INTEGER
);

-- power_runs records one row per `power pull` invocation, parallel to
-- ingest_runs. Doubles as the reproducibility manifest for every row in
-- monthly_supplement *and* daily_supplement: a researcher with one
-- power_runs row + one station's cell_id + lat/lon can reconstruct the
-- exact POWER request that produced the supplement values. The
-- `endpoint_url`, `parameters`, `community`, and `period_*_year` /
-- `period_*_date` fields together fully specify a POWER request.
-- `solar_conversion` documents our MJ/m²/day → W/m² factor so consumers
-- seeing W/m² in our data can still follow back to POWER's native
-- units.
--
-- `temporal_mode` distinguishes monthly (climatological window) and
-- daily (calendar date range) pulls. Monthly rows populate
-- `period_start_year` / `period_end_year`; daily rows populate
-- `period_start_date` / `period_end_date` (and also the year columns
-- with the year-component of the dates, so a reader that only knows
-- about the year columns still gets a coarse span).
CREATE TABLE IF NOT EXISTS power_runs (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    status            TEXT NOT NULL
                       CHECK (status IN ('running', 'complete', 'aborted')),

    -- Reproducibility manifest (see comment above).
    endpoint_url      TEXT NOT NULL,
    parameters        TEXT NOT NULL,
    community         TEXT NOT NULL,
    period_start_year INTEGER NOT NULL,
    period_end_year   INTEGER NOT NULL,
    grid_resolution   TEXT NOT NULL,
    solar_conversion  REAL NOT NULL,

    -- Per-variable unit-conversion manifest as JSON. One entry per
    -- requested POWER parameter, each carrying {power_unit, stored_unit,
    -- factor}. solar_conversion above is kept for back-compat with
    -- existing reproducibility consumers; the full manifest here
    -- generalizes it to every requested variable.
    unit_conversions  TEXT,

    -- Temporal mode of the pull: 'monthly' targets the monthly endpoint
    -- and writes monthly_supplement; 'daily' targets the daily endpoint
    -- and writes daily_supplement. Default 'monthly' so an INSERT that
    -- omits the column lands as a monthly run.
    temporal_mode     TEXT NOT NULL DEFAULT 'monthly'
                       CHECK (temporal_mode IN ('monthly', 'daily')),

    -- Daily-mode date bounds. ISO 'YYYY-MM-DD'. Nullable because
    -- monthly-mode runs leave these unset (the year columns suffice).
    -- Populated for daily-mode runs so the manifest fully reproduces
    -- the request URL (POWER daily takes `start=YYYYMMDD&end=YYYYMMDD`).
    period_start_date TEXT,
    period_end_date   TEXT,

    -- Run statistics.
    cells_attempted   INTEGER,
    cells_succeeded   INTEGER,
    cells_failed      INTEGER,
    supplement_rows   INTEGER,
    etl_git_sha       TEXT
);

-- nasa_power_grid_cells registers the POWER grid cells that any
-- station's lat/lon falls into. POWER's MERRA-2-derived monthly grid
-- is 0.5° lat × 0.625° lon (≈ 50 km × 60 km at Mexico's latitudes), so
-- a single cell typically covers several CONAGUA stations — keying
-- supplement data by cell instead of by station deduplicates the
-- ~5,400-station fetch by ~2.3×.
--
-- (lat, lon) is the cell centroid, decimal degrees, west-negative.
-- cell_id is a deterministic textual key derived from the centroid so
-- a re-run picks up the same row.
CREATE TABLE IF NOT EXISTS nasa_power_grid_cells (
    cell_id         TEXT PRIMARY KEY,
    lat             REAL NOT NULL,
    lon             REAL NOT NULL,
    grid_resolution TEXT NOT NULL
                     CHECK (grid_resolution IN ('0.5x0.625'))
);

-- station_power_cell links each station to its enclosing POWER cell.
-- distance_km is the great-circle distance from the station's lat/lon
-- to the cell centroid; the UI surfaces it as a "possible error"
-- indicator next to POWER-derived values, so an architect can tell
-- when a coastal station fell into a 50-km cell that includes ocean.
CREATE TABLE IF NOT EXISTS station_power_cell (
    station_id  INTEGER PRIMARY KEY REFERENCES stations(id),
    cell_id     TEXT NOT NULL REFERENCES nasa_power_grid_cells(cell_id),
    distance_km REAL NOT NULL
);

-- monthly_supplement holds the per-(cell, period, month) climatology
-- from non-CONAGUA sources — currently NASA POWER, eventually ERA5 or
-- EMA-derived values. Keyed by cell so multiple stations sharing a
-- cell read the same row through station_power_cell.
--
-- Provenance is encoded by table membership: every row in this table
-- is reanalysis from POWER. Per-variable *_source columns are
-- deliberately absent — they would universally hold the single value
-- 'reanalysis_power' and carry no information that the table identity
-- doesn't already provide.
--
-- Unit conventions for every variable are pinned in the column name
-- suffix (`_c`, `_pct`, `_ms`, `_wm2`, `_kpa`, `_mmpd`, `_deg`,
-- `_gkg`). Variables that ship from POWER in non-SI units are
-- converted at ingest; per-variable factors are recorded in
-- power_runs.unit_conversions for reversibility (e.g. all radiation
-- arrives in MJ/m²/day and is converted to W/m² by ×1e6/86400 ≈
-- ×11.574). Wind direction is rolled up with the circular mean
-- (atan2 of mean-sin / mean-cos across the years contributing to
-- each calendar month) — arithmetic averaging would wrap incorrectly
-- across the 0/360° boundary.
CREATE TABLE IF NOT EXISTS monthly_supplement (
    cell_id            TEXT NOT NULL REFERENCES nasa_power_grid_cells(cell_id),
    period             TEXT NOT NULL
                        CHECK (period IN ('1961-1990', '1971-2000', '1981-2010', '1991-2020')),
    month              INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),

    -- Temperature (°C)
    t2m_c              REAL,  -- mean air temperature at 2 m
    t2m_max_c          REAL,  -- daily max at 2 m
    t2m_min_c          REAL,  -- daily min at 2 m
    t2m_wet_c          REAL,  -- wet-bulb at 2 m (direct evaporative design)
    t2m_dew_c          REAL,  -- dew/frost point at 2 m
    ts_c               REAL,  -- earth-skin/ground temperature
    ts_max_c           REAL,  -- skin daily max
    ts_min_c           REAL,  -- skin daily min

    -- Humidity
    rh2m_pct           REAL,  -- relative humidity at 2 m, %
    qv2m_gkg           REAL,  -- specific humidity at 2 m, g/kg

    -- Wind
    ws2m_ms            REAL,  -- speed at 2 m, m/s
    ws10m_ms           REAL,  -- speed at 10 m, m/s
    ws50m_ms           REAL,  -- speed at 50 m, m/s (stack effect, upper stories)
    wd2m_deg           REAL,  -- direction at 2 m, degrees (circular mean)
    wd10m_deg          REAL,  -- direction at 10 m, degrees (circular mean)

    -- Solar — shortwave (W/m², 24-h average; converted from MJ/m²/day)
    solar_ghi_wm2      REAL,  -- ALLSKY_SFC_SW_DWN  — global horizontal
    solar_dhi_wm2      REAL,  -- ALLSKY_SFC_SW_DIFF — diffuse horizontal
    solar_dni_wm2      REAL,  -- ALLSKY_SFC_SW_DNI  — direct normal
    solar_clrsky_wm2   REAL,  -- CLRSKY_SFC_SW_DWN  — clear-sky reference
    clearness_index    REAL,  -- ALLSKY_KT          — dimensionless 0-1
    par_wm2            REAL,  -- ALLSKY_SFC_PAR_TOT — photosynthetic-active
    uva_wm2            REAL,  -- ALLSKY_SFC_UVA
    uvb_wm2            REAL,  -- ALLSKY_SFC_UVB

    -- Solar — longwave (W/m², 24-h average; converted from MJ/m²/day)
    lw_dwn_wm2         REAL,  -- ALLSKY_SFC_LW_DWN — downward longwave (sky temp)

    -- Sky / cloud / atmosphere
    cloud_amt_pct      REAL,  -- CLOUD_AMT, %
    ps_kpa             REAL,  -- surface pressure, kPa (POWER native; ASHRAE convention)

    -- Moisture and evapotranspiration
    precip_mmpd        REAL,  -- PRECTOTCORR — bias-corrected precipitation
    evland_mmpd        REAL,  -- EVLAND — land evapotranspiration (mass flux, mm/day)

    -- Soil moisture (dimensionless 0..1 wetness)
    gwet_top           REAL,  -- top-layer soil wetness
    gwet_root          REAL,  -- root-zone soil wetness
    gwet_prof          REAL,  -- profile-integrated soil moisture

    -- Reproducibility back-pointer: every supplement value can be
    -- traced to the run that wrote it via power_runs (which carries
    -- the endpoint, parameters, period, and per-variable unit-conversion
    -- manifest). Nullable for forward compatibility with non-POWER-sourced
    -- supplement rows (e.g., a future ERA5 path) that won't have a
    -- power_runs entry.
    power_run_id       INTEGER REFERENCES power_runs(id),

    PRIMARY KEY (cell_id, period, month)
);

-- daily_supplement holds POWER's daily reanalysis time series per cell,
-- mirroring monthly_supplement's 31 value columns but keyed by
-- (cell_id, date) instead of (cell_id, period, month). Provenance is
-- encoded by table — every row here is daily POWER reanalysis, joined
-- to stations through station_power_cell at read time.
--
-- Date range: POWER daily starts 1981-01-01 and runs forward; the
-- pre-1981 gap stays empty. UI consumers prefer CONAGUA observed daily
-- values where available (tmax, tmin, precip, evap) and fall back to
-- this table for everything else (humidity, wind speed and direction,
-- shortwave + longwave radiation, cloud cover, pressure, soil
-- moisture, etc.).
--
-- Unit conventions match monthly_supplement: radiation streams arrive
-- in MJ/m²/day from POWER and are converted to W/m² at ingest
-- (× 1e6/86400 ≈ 11.574); everything else is stored in POWER's native
-- unit. Wind direction (`wd2m_deg`, `wd10m_deg`) is stored as-is —
-- POWER's daily WD is already a 24-h vector mean upstream, so no
-- circular averaging is applied at our layer.
CREATE TABLE IF NOT EXISTS daily_supplement (
    cell_id            TEXT NOT NULL REFERENCES nasa_power_grid_cells(cell_id),
    date               TEXT NOT NULL,  -- ISO 'YYYY-MM-DD'

    -- Temperature (°C)
    t2m_c              REAL,
    t2m_max_c          REAL,
    t2m_min_c          REAL,
    t2m_wet_c          REAL,
    t2m_dew_c          REAL,
    ts_c               REAL,
    ts_max_c           REAL,
    ts_min_c           REAL,

    -- Humidity
    rh2m_pct           REAL,
    qv2m_gkg           REAL,

    -- Wind
    ws2m_ms            REAL,
    ws10m_ms           REAL,
    ws50m_ms           REAL,
    wd2m_deg           REAL,
    wd10m_deg          REAL,

    -- Solar — shortwave (W/m², 24-h average; converted from MJ/m²/day)
    solar_ghi_wm2      REAL,
    solar_dhi_wm2      REAL,
    solar_dni_wm2      REAL,
    solar_clrsky_wm2   REAL,
    clearness_index    REAL,
    par_wm2            REAL,
    uva_wm2            REAL,
    uvb_wm2            REAL,

    -- Solar — longwave (W/m², 24-h average; converted from MJ/m²/day)
    lw_dwn_wm2         REAL,

    -- Sky / cloud / atmosphere
    cloud_amt_pct      REAL,
    ps_kpa             REAL,

    -- Moisture and evapotranspiration
    precip_mmpd        REAL,
    evland_mmpd        REAL,

    -- Soil moisture (dimensionless 0..1 wetness)
    gwet_top           REAL,
    gwet_root          REAL,
    gwet_prof          REAL,

    -- Reproducibility back-pointer to power_runs. Nullable for the
    -- same forward-compat reason monthly_supplement.power_run_id is.
    power_run_id       INTEGER REFERENCES power_runs(id),

    PRIMARY KEY (cell_id, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_station_date     ON daily_observations(station_id, date);
CREATE INDEX IF NOT EXISTS idx_monthly_station_period ON monthly_normals(station_id, period);
CREATE INDEX IF NOT EXISTS idx_extras_station_period  ON monthly_normals_extras(station_id, period);
CREATE INDEX IF NOT EXISTS idx_warnings_severity      ON parsing_warnings(severity);
CREATE INDEX IF NOT EXISTS idx_stations_source        ON stations(source);
CREATE INDEX IF NOT EXISTS idx_supplement_cell_period ON monthly_supplement(cell_id, period);
CREATE INDEX IF NOT EXISTS idx_station_power_cell_cell ON station_power_cell(cell_id);
CREATE INDEX IF NOT EXISTS idx_daily_supplement_date  ON daily_supplement(date);
