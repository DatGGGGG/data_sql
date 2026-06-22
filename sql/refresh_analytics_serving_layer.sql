\set ON_ERROR_STOP on
\pset pager off

SET max_parallel_workers_per_gather = 0;
SET work_mem = '128MB';

TRUNCATE TABLE analytics.mobile_app_monthly_performance_cache;
TRUNCATE TABLE analytics.mobile_app_yearly_performance_cache;

DO $$
DECLARE
    current_year int;
    min_year int;
    max_year int;
BEGIN
    SELECT
        EXTRACT(YEAR FROM MIN(date))::int,
        EXTRACT(YEAR FROM MAX(date))::int
    INTO min_year, max_year
    FROM core.fact_app_performance_daily;

    IF min_year IS NULL OR max_year IS NULL THEN
        RETURN;
    END IF;

    FOR current_year IN min_year..max_year LOOP
        RAISE NOTICE 'Rebuilding monthly mobile cache for year %', current_year;

        INSERT INTO analytics.mobile_app_monthly_performance_cache (
            month,
            country,
            platform,
            app_id,
            total_downloads,
            total_revenue
        )
        SELECT
            DATE_TRUNC('month', date)::date AS month,
            country_android AS country,
            'android'::text AS platform,
            app_id,
            SUM(COALESCE(downloads_android, 0)) AS total_downloads,
            SUM(COALESCE(revenue_android, 0)) AS total_revenue
        FROM core.fact_app_performance_daily
        WHERE date >= MAKE_DATE(current_year, 1, 1)
          AND date < MAKE_DATE(current_year + 1, 1, 1)
          AND country_android IS NOT NULL
        GROUP BY 1, 2, 3, 4;

        INSERT INTO analytics.mobile_app_monthly_performance_cache (
            month,
            country,
            platform,
            app_id,
            total_downloads,
            total_revenue
        )
        SELECT
            DATE_TRUNC('month', date)::date AS month,
            country_ios AS country,
            'iphone'::text AS platform,
            app_id,
            SUM(COALESCE(downloads_iphone, 0)) AS total_downloads,
            SUM(COALESCE(revenue_iphone, 0)) AS total_revenue
        FROM core.fact_app_performance_daily
        WHERE date >= MAKE_DATE(current_year, 1, 1)
          AND date < MAKE_DATE(current_year + 1, 1, 1)
          AND country_ios IS NOT NULL
        GROUP BY 1, 2, 3, 4;

        INSERT INTO analytics.mobile_app_monthly_performance_cache (
            month,
            country,
            platform,
            app_id,
            total_downloads,
            total_revenue
        )
        SELECT
            DATE_TRUNC('month', date)::date AS month,
            country_ios AS country,
            'ipad'::text AS platform,
            app_id,
            SUM(COALESCE(downloads_ipad, 0)) AS total_downloads,
            SUM(COALESCE(revenue_ipad, 0)) AS total_revenue
        FROM core.fact_app_performance_daily
        WHERE date >= MAKE_DATE(current_year, 1, 1)
          AND date < MAKE_DATE(current_year + 1, 1, 1)
          AND country_ios IS NOT NULL
        GROUP BY 1, 2, 3, 4;

        RAISE NOTICE 'Rebuilding yearly mobile cache for year %', current_year;

        INSERT INTO analytics.mobile_app_yearly_performance_cache (
            year,
            app_id,
            total_downloads,
            total_revenue
        )
        SELECT
            current_year AS year,
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
        WHERE date >= MAKE_DATE(current_year, 1, 1)
          AND date < MAKE_DATE(current_year + 1, 1, 1)
        GROUP BY app_id;
    END LOOP;
END;
$$;

ANALYZE analytics.mobile_app_monthly_performance_cache;
ANALYZE analytics.mobile_app_yearly_performance_cache;

REFRESH MATERIALIZED VIEW analytics.agg_game_performance_monthly;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_taxonomy_monthly_performance;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_download_total_yearly;
REFRESH MATERIALIZED VIEW analytics.agg_subgenre_performance_monthly;
REFRESH MATERIALIZED VIEW analytics.agg_subgenre_performance_yearly;
REFRESH MATERIALIZED VIEW analytics.agg_game_performance_yearly;
REFRESH MATERIALIZED VIEW analytics.agg_new_game_new_performance;

ANALYZE analytics.agg_game_performance_monthly;
ANALYZE analytics.mv_mobile_taxonomy_monthly_performance;
ANALYZE analytics.mv_mobile_download_total_yearly;
ANALYZE analytics.agg_subgenre_performance_monthly;
ANALYZE analytics.agg_subgenre_performance_yearly;
ANALYZE analytics.agg_game_performance_yearly;
ANALYZE analytics.agg_new_game_new_performance;
