\set ON_ERROR_STOP on
\pset pager off

CREATE SCHEMA IF NOT EXISTS analytics;

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_new_games_performance_by_launch_year;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_revenue_by_game_yearly;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_revenue_by_subgenre_yearly;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_download_total_yearly;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_taxonomy_monthly_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_publisher_monthly_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_market_monthly_overview;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_game_monthly_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_app_yearly_performance;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_mobile_app_monthly_performance;
DROP TABLE IF EXISTS analytics.mobile_app_yearly_performance_cache;
DROP TABLE IF EXISTS analytics.mobile_app_monthly_performance_cache;
DROP VIEW IF EXISTS analytics.vw_mobile_app_performance_base;

CREATE OR REPLACE VIEW analytics.vw_mobile_app_performance_base AS
SELECT
    f.date,
    f.country,
    f.platform,
    f.app_id,
    a.unified_app_id AS game_id,
    a.name AS app_name,
    g.name AS game_name,
    a.publisher_id,
    a.publisher_name,
    a.cleaned_publisher_name,
    a.os,
    a.active,
    g.game_class,
    g.game_genre,
    g.game_subgenre,
    f.downloads,
    f.revenue
FROM (
    SELECT
        date,
        country_android AS country,
        'android'::text AS platform,
        app_id,
        downloads_android AS downloads,
        revenue_android AS revenue
    FROM core.fact_app_performance_daily

    UNION ALL

    SELECT
        date,
        country_ios AS country,
        'iphone'::text AS platform,
        app_id,
        downloads_iphone AS downloads,
        revenue_iphone AS revenue
    FROM core.fact_app_performance_daily

    UNION ALL

    SELECT
        date,
        country_ios AS country,
        'ipad'::text AS platform,
        app_id,
        downloads_ipad AS downloads,
        revenue_ipad AS revenue
    FROM core.fact_app_performance_daily
) AS f
LEFT JOIN core.dim_app_info AS a
  ON a.app_id = f.app_id
LEFT JOIN core.dim_game_info AS g
  ON g.unified_app_id = a.unified_app_id
WHERE f.country IS NOT NULL;

CREATE TABLE analytics.mobile_app_monthly_performance_cache AS
SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_android AS country,
    'android'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_android, 0)) AS total_downloads,
    SUM(COALESCE(revenue_android, 0)) AS total_revenue
FROM core.fact_app_performance_daily
WHERE country_android IS NOT NULL
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_ios AS country,
    'iphone'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_iphone, 0)) AS total_downloads,
    SUM(COALESCE(revenue_iphone, 0)) AS total_revenue
FROM core.fact_app_performance_daily
WHERE country_ios IS NOT NULL
GROUP BY 1, 2, 3, 4

UNION ALL

SELECT
    DATE_TRUNC('month', date)::date AS month,
    country_ios AS country,
    'ipad'::text AS platform,
    app_id,
    SUM(COALESCE(downloads_ipad, 0)) AS total_downloads,
    SUM(COALESCE(revenue_ipad, 0)) AS total_revenue
FROM core.fact_app_performance_daily
WHERE country_ios IS NOT NULL
GROUP BY 1, 2, 3, 4
WITH NO DATA;

CREATE TABLE analytics.mobile_app_yearly_performance_cache AS
SELECT
    EXTRACT(YEAR FROM date)::int AS year,
    app_id,
    SUM(
        COALESCE(downloads_android, 0)
        + COALESCE(downloads_iphone, 0)
        + COALESCE(downloads_ipad, 0)
    )::numeric AS total_downloads,
    SUM(
        COALESCE(revenue_android, 0)
        + COALESCE(revenue_iphone, 0)
        + COALESCE(revenue_ipad, 0)
    )::numeric AS total_revenue
FROM core.fact_app_performance_daily
GROUP BY 1, 2
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_game_monthly_performance AS
SELECT
    h.month,
    h.country,
    h.platform,
    a.unified_app_id AS game_id,
    g.name AS game_name,
    publisher_id,
    publisher_name,
    cleaned_publisher_name,
    game_class,
    game_genre,
    game_subgenre,
    COUNT(*) AS app_count,
    SUM(COALESCE(h.total_downloads, 0)) AS total_downloads,
    SUM(COALESCE(h.total_revenue, 0)) AS total_revenue
FROM analytics.mobile_app_monthly_performance_cache AS h
LEFT JOIN core.dim_app_info AS a
  ON a.app_id = h.app_id
LEFT JOIN core.dim_game_info AS g
  ON g.unified_app_id = a.unified_app_id
GROUP BY
    h.month,
    h.country,
    h.platform,
    a.unified_app_id,
    g.name,
    publisher_id,
    publisher_name,
    cleaned_publisher_name,
    game_class,
    game_genre,
    game_subgenre
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_market_monthly_overview AS
SELECT
    h.month,
    h.country,
    COUNT(DISTINCT h.app_id) AS active_app_count,
    COUNT(DISTINCT a.unified_app_id) AS active_game_count,
    COUNT(DISTINCT publisher_id) FILTER (WHERE publisher_id IS NOT NULL AND btrim(publisher_id) <> '') AS active_publisher_count,
    SUM(CASE WHEN h.platform = 'android' THEN COALESCE(h.total_downloads, 0) ELSE 0 END) AS downloads_android,
    SUM(CASE WHEN h.platform = 'iphone' THEN COALESCE(h.total_downloads, 0) ELSE 0 END) AS downloads_iphone,
    SUM(CASE WHEN h.platform = 'ipad' THEN COALESCE(h.total_downloads, 0) ELSE 0 END) AS downloads_ipad,
    SUM(COALESCE(h.total_downloads, 0)) AS total_downloads,
    SUM(CASE WHEN h.platform = 'android' THEN COALESCE(h.total_revenue, 0) ELSE 0 END) AS revenue_android,
    SUM(CASE WHEN h.platform = 'iphone' THEN COALESCE(h.total_revenue, 0) ELSE 0 END) AS revenue_iphone,
    SUM(CASE WHEN h.platform = 'ipad' THEN COALESCE(h.total_revenue, 0) ELSE 0 END) AS revenue_ipad,
    SUM(COALESCE(h.total_revenue, 0)) AS total_revenue
FROM analytics.mobile_app_monthly_performance_cache AS h
LEFT JOIN core.dim_app_info AS a
  ON a.app_id = h.app_id
GROUP BY
    h.month,
    h.country
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_publisher_monthly_performance AS
SELECT
    h.month,
    h.country,
    h.platform,
    publisher_id,
    publisher_name,
    cleaned_publisher_name,
    COUNT(*) AS app_count,
    COUNT(DISTINCT a.unified_app_id) AS game_count,
    SUM(COALESCE(h.total_downloads, 0)) AS total_downloads,
    SUM(COALESCE(h.total_revenue, 0)) AS total_revenue
FROM analytics.mobile_app_monthly_performance_cache AS h
LEFT JOIN core.dim_app_info AS a
  ON a.app_id = h.app_id
GROUP BY
    h.month,
    h.country,
    h.platform,
    publisher_id,
    publisher_name,
    cleaned_publisher_name
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_taxonomy_monthly_performance AS
SELECT
    h.month,
    h.country,
    h.platform,
    game_class,
    game_genre,
    game_subgenre,
    COUNT(DISTINCT a.unified_app_id) AS game_count,
    COUNT(*) AS app_count,
    SUM(COALESCE(h.total_downloads, 0)) AS total_downloads,
    SUM(COALESCE(h.total_revenue, 0)) AS total_revenue
FROM analytics.mobile_app_monthly_performance_cache AS h
LEFT JOIN core.dim_app_info AS a
  ON a.app_id = h.app_id
LEFT JOIN core.dim_game_info AS g
  ON g.unified_app_id = a.unified_app_id
GROUP BY
    h.month,
    h.country,
    h.platform,
    game_class,
    game_genre,
    game_subgenre
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_download_total_yearly AS
WITH max_2025 AS (
    SELECT MAX(f.date)::date AS max_date_2025
    FROM core.fact_app_performance_daily AS f
    JOIN core.dim_app_info AS a
      ON a.app_id = f.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE f.date >= DATE '2025-01-01'
      AND f.date < DATE '2026-01-01'
      AND (
        g.game_product_model IS NULL
        OR g.game_product_model NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
      )
),
days_2025 AS (
    SELECT
        max_date_2025,
        ((max_date_2025 - DATE '2025-01-01') + 1)::numeric AS days_elapsed_2025,
        365.0 / NULLIF(((max_date_2025 - DATE '2025-01-01') + 1)::numeric, 0) AS annualize_factor
    FROM max_2025
),
raw_year AS (
    SELECT
        ayp.year,
        SUM(ayp.total_downloads) AS downloads
    FROM analytics.mobile_app_yearly_performance_cache AS ayp
    JOIN core.dim_app_info AS a
      ON a.app_id = ayp.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE ayp.year BETWEEN 2014 AND 2025
      AND (
        g.game_product_model IS NULL
        OR g.game_product_model NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
      )
    GROUP BY 1
)
SELECT
    r.year,
    CASE
        WHEN r.year = 2025 THEN ROUND(r.downloads * d.annualize_factor, 0)::bigint
        ELSE r.downloads::bigint
    END AS total_downloads,
    CASE WHEN r.year = 2025 THEN d.max_date_2025 ELSE NULL END AS data_through_2025,
    CASE WHEN r.year = 2025 THEN d.days_elapsed_2025 ELSE NULL END AS days_elapsed_2025
FROM raw_year AS r
CROSS JOIN days_2025 AS d
WHERE r.year BETWEEN 2014 AND 2025
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_revenue_by_subgenre_yearly AS
WITH max_2025 AS (
    SELECT MAX(f.date)::date AS max_date_2025
    FROM core.fact_app_performance_daily AS f
    JOIN core.dim_app_info AS a
      ON a.app_id = f.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE f.date >= DATE '2025-01-01'
      AND f.date < DATE '2026-01-01'
      AND (
        g.game_product_model IS NULL
        OR g.game_product_model NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
      )
),
days_2025 AS (
    SELECT
        max_date_2025,
        ((max_date_2025 - DATE '2025-01-01') + 1)::numeric AS days_elapsed_2025,
        365.0 / NULLIF(((max_date_2025 - DATE '2025-01-01') + 1)::numeric, 0) AS annualize_factor
    FROM max_2025
),
app_year_revenue AS (
    SELECT
        year,
        app_id,
        total_revenue AS revenue_cent
    FROM analytics.mobile_app_yearly_performance_cache
    WHERE year BETWEEN 2014 AND 2025
),
cgs_year AS (
    SELECT
        g.game_class AS game_class,
        g.game_genre AS game_genre,
        g.game_subgenre AS subgenre,
        ayr.year,
        SUM(ayr.revenue_cent) AS revenue_cent
    FROM app_year_revenue AS ayr
    JOIN core.dim_app_info AS a
      ON a.app_id = ayr.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE g.game_subgenre IS NOT NULL
      AND btrim(g.game_subgenre) <> ''
      AND (
        g.game_product_model IS NULL
        OR g.game_product_model NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
      )
    GROUP BY 1, 2, 3, 4
)
SELECT
    y.game_class,
    y.game_genre,
    y.subgenre,
    y.year,
    ROUND(
        (
            CASE
                WHEN y.year = 2025 THEN y.revenue_cent * d.annualize_factor
                ELSE y.revenue_cent
            END
        ) / 100.0,
        2
    ) AS revenue_usd,
    CASE WHEN y.year = 2025 THEN d.max_date_2025 ELSE NULL END AS data_through_2025,
    CASE WHEN y.year = 2025 THEN d.days_elapsed_2025 ELSE NULL END AS days_elapsed_2025
FROM cgs_year AS y
CROSS JOIN days_2025 AS d
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_revenue_by_game_yearly AS
WITH max_2025 AS (
    SELECT MAX(f.date)::date AS max_date_2025
    FROM core.fact_app_performance_daily AS f
    JOIN core.dim_app_info AS a
      ON a.app_id = f.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE f.date >= DATE '2025-01-01'
      AND f.date < DATE '2026-01-01'
      AND (
        g.game_product_model IS NULL
        OR g.game_product_model NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
      )
),
days_2025 AS (
    SELECT
        max_date_2025,
        ((max_date_2025 - DATE '2025-01-01') + 1)::numeric AS days_elapsed_2025,
        365.0 / NULLIF(((max_date_2025 - DATE '2025-01-01') + 1)::numeric, 0) AS annualize_factor
    FROM max_2025
),
app_year_revenue AS (
    SELECT
        year,
        app_id,
        total_revenue AS revenue_cent
    FROM analytics.mobile_app_yearly_performance_cache
    WHERE year BETWEEN 2014 AND 2025
),
game_year AS (
    SELECT
        a.unified_app_id,
        g.name AS game_name,
        g.game_class,
        g.game_genre,
        g.game_subgenre,
        ayr.year,
        SUM(ayr.revenue_cent) AS revenue_cent
    FROM app_year_revenue AS ayr
    JOIN core.dim_app_info AS a
      ON a.app_id = ayr.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE (
        g.game_product_model IS NULL
        OR g.game_product_model NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
    )
    GROUP BY 1, 2, 3, 4, 5, 6
),
game_publishers AS (
    SELECT
        a.unified_app_id,
        string_agg(DISTINCT a.cleaned_publisher_name, ', ' ORDER BY a.cleaned_publisher_name)
            FILTER (WHERE a.cleaned_publisher_name IS NOT NULL AND btrim(a.cleaned_publisher_name) <> '')
            AS cleaned_publisher_name
    FROM core.dim_app_info AS a
    GROUP BY 1
)
SELECT
    gy.unified_app_id,
    gy.game_name,
    COALESCE(gp.cleaned_publisher_name, '') AS cleaned_publisher_name,
    gy.game_class,
    gy.game_genre,
    gy.game_subgenre,
    gy.year,
    ROUND(
        (
            CASE
                WHEN gy.year = 2025 THEN gy.revenue_cent * d.annualize_factor
                ELSE gy.revenue_cent
            END
        ) / 100.0,
        2
    ) AS revenue_usd,
    CASE WHEN gy.year = 2025 THEN d.max_date_2025 ELSE NULL END AS data_through_2025,
    CASE WHEN gy.year = 2025 THEN d.days_elapsed_2025 ELSE NULL END AS days_elapsed_2025
FROM game_year AS gy
LEFT JOIN game_publishers AS gp
  ON gp.unified_app_id = gy.unified_app_id
CROSS JOIN days_2025 AS d
WITH NO DATA;

CREATE MATERIALIZED VIEW analytics.mv_mobile_new_games_performance_by_launch_year AS
WITH max_date_all AS (
    SELECT MAX(date)::date AS max_date
    FROM core.fact_app_performance_daily
),
app_first_perf AS (
    SELECT
        f.app_id,
        MIN(f.date)::date AS app_first_date
    FROM core.fact_app_performance_daily AS f
    WHERE (
        COALESCE(f.downloads_android, 0)
        + COALESCE(f.downloads_iphone, 0)
        + COALESCE(f.downloads_ipad, 0)
        + COALESCE(f.revenue_android, 0)
        + COALESCE(f.revenue_iphone, 0)
        + COALESCE(f.revenue_ipad, 0)
    ) > 0
    GROUP BY 1
),
game_first_perf AS (
    SELECT
        a.unified_app_id,
        MIN(af.app_first_date) AS game_first_date
    FROM app_first_perf AS af
    JOIN core.dim_app_info AS a
      ON a.app_id = af.app_id
    JOIN core.dim_game_info AS g
      ON g.unified_app_id = a.unified_app_id
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
      AND COALESCE(g.game_product_model, '') NOT IN ('Hybridcasual', 'Hypercasual', 'Exclusive Access')
    GROUP BY 1
),
new_games_2021_2025 AS (
    SELECT
        gfp.unified_app_id,
        gfp.game_first_date,
        EXTRACT(YEAR FROM gfp.game_first_date)::int AS year
    FROM game_first_perf AS gfp
    WHERE gfp.game_first_date >= DATE '2021-01-01'
      AND gfp.game_first_date < DATE '2026-01-01'
),
game_publishers AS (
    SELECT
        a.unified_app_id,
        string_agg(DISTINCT a.cleaned_publisher_name, ', ' ORDER BY a.cleaned_publisher_name)
            FILTER (WHERE a.cleaned_publisher_name IS NOT NULL AND btrim(a.cleaned_publisher_name) <> '')
            AS cleaned_publisher_names
    FROM core.dim_app_info AS a
    WHERE a.unified_app_id IS NOT NULL
      AND btrim(a.unified_app_id) <> ''
    GROUP BY 1
),
game_apps AS (
    SELECT DISTINCT
        ng.year,
        ng.unified_app_id,
        ng.game_first_date,
        a.app_id
    FROM new_games_2021_2025 AS ng
    JOIN core.dim_app_info AS a
      ON a.unified_app_id = ng.unified_app_id
),
game_revenue_windows AS (
    SELECT
        ga.year,
        ga.unified_app_id,
        ga.game_first_date,
        m.max_date,
        SUM(
            (
                COALESCE(f.revenue_android, 0)
                + COALESCE(f.revenue_iphone, 0)
                + COALESCE(f.revenue_ipad, 0)
            )::numeric
        ) FILTER (
            WHERE f.date >= ga.game_first_date
              AND f.date < (ga.game_first_date + INTERVAL '7 days')
        ) / 100.0 AS sum_7d_usd,
        SUM(
            (
                COALESCE(f.revenue_android, 0)
                + COALESCE(f.revenue_iphone, 0)
                + COALESCE(f.revenue_ipad, 0)
            )::numeric
        ) FILTER (
            WHERE f.date >= ga.game_first_date
              AND f.date < (ga.game_first_date + INTERVAL '30 days')
        ) / 100.0 AS sum_30d_usd,
        SUM(
            (
                COALESCE(f.revenue_android, 0)
                + COALESCE(f.revenue_iphone, 0)
                + COALESCE(f.revenue_ipad, 0)
            )::numeric
        ) FILTER (
            WHERE f.date >= ga.game_first_date
              AND f.date < (ga.game_first_date + INTERVAL '90 days')
        ) / 100.0 AS sum_90d_usd,
        SUM(
            (
                COALESCE(f.revenue_android, 0)
                + COALESCE(f.revenue_iphone, 0)
                + COALESCE(f.revenue_ipad, 0)
            )::numeric
        ) FILTER (
            WHERE f.date >= ga.game_first_date
              AND f.date < (ga.game_first_date + INTERVAL '365 days')
        ) / 100.0 AS sum_365d_usd,
        SUM(
            (
                COALESCE(f.revenue_android, 0)
                + COALESCE(f.revenue_iphone, 0)
                + COALESCE(f.revenue_ipad, 0)
            )::numeric
        ) / 100.0 AS revenue_since_first_date_usd
    FROM game_apps AS ga
    JOIN core.fact_app_performance_daily AS f
      ON f.app_id = ga.app_id
    CROSS JOIN max_date_all AS m
    WHERE f.date >= ga.game_first_date
      AND f.date < (m.max_date + INTERVAL '1 day')
    GROUP BY
        ga.year,
        ga.unified_app_id,
        ga.game_first_date,
        m.max_date
)
SELECT
    grw.year,
    grw.unified_app_id,
    g.name AS game_name,
    COALESCE(gp.cleaned_publisher_names, '') AS cleaned_publisher_names,
    grw.game_first_date,
    ROUND(
        CASE
            WHEN grw.max_date >= (grw.game_first_date + INTERVAL '6 days') THEN grw.sum_7d_usd
            ELSE NULL
        END,
        2
    ) AS first_7d_revenue_usd,
    ROUND(
        CASE
            WHEN grw.max_date >= (grw.game_first_date + INTERVAL '29 days') THEN grw.sum_30d_usd
            ELSE NULL
        END,
        2
    ) AS first_30d_revenue_usd,
    ROUND(
        CASE
            WHEN grw.max_date >= (grw.game_first_date + INTERVAL '89 days') THEN grw.sum_90d_usd
            ELSE NULL
        END,
        2
    ) AS first_90d_revenue_usd,
    ROUND(
        CASE
            WHEN grw.max_date >= (grw.game_first_date + INTERVAL '364 days') THEN grw.sum_365d_usd
            ELSE NULL
        END,
        2
    ) AS first_365d_revenue_usd,
    ROUND(
        CASE
            WHEN grw.max_date >= (grw.game_first_date + INTERVAL '29 days')
                THEN COALESCE(grw.revenue_since_first_date_usd, 0)
                    * 30.0
                    / NULLIF(((grw.max_date - grw.game_first_date) + 1), 0)
            ELSE NULL
        END,
        2
    ) AS avg_monthly_revenue_usd
FROM game_revenue_windows AS grw
JOIN core.dim_game_info AS g
  ON g.unified_app_id = grw.unified_app_id
LEFT JOIN game_publishers AS gp
  ON gp.unified_app_id = grw.unified_app_id
WITH NO DATA;

CREATE INDEX idx_mv_mobile_game_monthly_perf_month_country_platform
ON analytics.mv_mobile_game_monthly_performance (month, country, platform);

CREATE INDEX idx_mobile_app_monthly_cache_month_country_platform
ON analytics.mobile_app_monthly_performance_cache (month, country, platform);

CREATE INDEX idx_mobile_app_monthly_cache_app_month
ON analytics.mobile_app_monthly_performance_cache (app_id, month);

CREATE INDEX idx_mobile_app_yearly_cache_year_app
ON analytics.mobile_app_yearly_performance_cache (year, app_id);

CREATE INDEX idx_mv_mobile_game_monthly_perf_game_month
ON analytics.mv_mobile_game_monthly_performance (game_id, month);

CREATE INDEX idx_mv_mobile_game_monthly_perf_publisher_month
ON analytics.mv_mobile_game_monthly_performance (publisher_id, month);

CREATE INDEX idx_mv_mobile_market_monthly_overview_month_country
ON analytics.mv_mobile_market_monthly_overview (month, country);

CREATE INDEX idx_mv_mobile_publisher_monthly_perf_month_country_platform
ON analytics.mv_mobile_publisher_monthly_performance (month, country, platform);

CREATE INDEX idx_mv_mobile_publisher_monthly_perf_publisher_month
ON analytics.mv_mobile_publisher_monthly_performance (publisher_id, month);

CREATE INDEX idx_mv_mobile_taxonomy_monthly_perf_month_country_platform
ON analytics.mv_mobile_taxonomy_monthly_performance (month, country, platform);

CREATE INDEX idx_mv_mobile_taxonomy_monthly_perf_taxonomy_month
ON analytics.mv_mobile_taxonomy_monthly_performance (game_class, game_genre, game_subgenre, month);

CREATE INDEX idx_mv_mobile_download_total_yearly_year
ON analytics.mv_mobile_download_total_yearly (year);

CREATE INDEX idx_mv_mobile_revenue_by_subgenre_yearly_year
ON analytics.mv_mobile_revenue_by_subgenre_yearly (year);

CREATE INDEX idx_mv_mobile_revenue_by_subgenre_yearly_taxonomy_year
ON analytics.mv_mobile_revenue_by_subgenre_yearly (game_class, game_genre, subgenre, year);

CREATE INDEX idx_mv_mobile_revenue_by_game_yearly_year_revenue
ON analytics.mv_mobile_revenue_by_game_yearly (year, revenue_usd DESC);

CREATE INDEX idx_mv_mobile_revenue_by_game_yearly_game_year
ON analytics.mv_mobile_revenue_by_game_yearly (unified_app_id, year);

CREATE INDEX idx_mv_mobile_new_games_perf_launch_year
ON analytics.mv_mobile_new_games_performance_by_launch_year (year);

CREATE INDEX idx_mv_mobile_new_games_perf_first_date
ON analytics.mv_mobile_new_games_performance_by_launch_year (game_first_date);
