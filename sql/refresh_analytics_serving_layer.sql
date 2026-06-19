\set ON_ERROR_STOP on
\pset pager off

SET max_parallel_workers_per_gather = 0;
SET work_mem = '128MB';

TRUNCATE TABLE analytics.agg_game_performance_daily;
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
        RAISE NOTICE 'Rebuilding daily game aggregate for year %', current_year;

        INSERT INTO analytics.agg_game_performance_daily (
            date,
            country,
            game_name,
            unified_app_id,
            game_class,
            game_genre,
            game_subgenre,
            downloads,
            revenue
        )
        SELECT
            f.date,
            f.country,
            g.name AS game_name,
            a.unified_app_id,
            g.game_class,
            g.game_genre,
            g.game_subgenre,
            SUM(COALESCE(f.downloads, 0)) AS downloads,
            SUM(COALESCE(f.revenue, 0)) AS revenue
        FROM (
            SELECT
                date,
                country_android AS country,
                app_id,
                downloads_android AS downloads,
                revenue_android AS revenue
            FROM core.fact_app_performance_daily
            WHERE date >= MAKE_DATE(current_year, 1, 1)
              AND date < MAKE_DATE(current_year + 1, 1, 1)
              AND country_android IS NOT NULL

            UNION ALL

            SELECT
                date,
                country_ios AS country,
                app_id,
                downloads_iphone AS downloads,
                revenue_iphone AS revenue
            FROM core.fact_app_performance_daily
            WHERE date >= MAKE_DATE(current_year, 1, 1)
              AND date < MAKE_DATE(current_year + 1, 1, 1)
              AND country_ios IS NOT NULL

            UNION ALL

            SELECT
                date,
                country_ios AS country,
                app_id,
                downloads_ipad AS downloads,
                revenue_ipad AS revenue
            FROM core.fact_app_performance_daily
            WHERE date >= MAKE_DATE(current_year, 1, 1)
              AND date < MAKE_DATE(current_year + 1, 1, 1)
              AND country_ios IS NOT NULL
        ) AS f
        JOIN core.dim_app_info AS a
          ON a.app_id = f.app_id
        JOIN core.dim_game_info AS g
          ON g.unified_app_id = a.unified_app_id
        WHERE a.unified_app_id IS NOT NULL
          AND btrim(a.unified_app_id) <> ''
        GROUP BY
            f.date,
            f.country,
            g.name,
            a.unified_app_id,
            g.game_class,
            g.game_genre,
            g.game_subgenre;

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

ANALYZE analytics.agg_game_performance_daily;
ANALYZE analytics.mobile_app_monthly_performance_cache;
ANALYZE analytics.mobile_app_yearly_performance_cache;

REFRESH MATERIALIZED VIEW analytics.mv_mobile_game_monthly_performance;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_market_monthly_overview;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_publisher_monthly_performance;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_taxonomy_monthly_performance;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_download_total_yearly;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_revenue_by_subgenre_yearly;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_revenue_by_game_yearly;
REFRESH MATERIALIZED VIEW analytics.mv_mobile_new_games_performance_by_launch_year;

ANALYZE analytics.mv_mobile_game_monthly_performance;
ANALYZE analytics.mv_mobile_market_monthly_overview;
ANALYZE analytics.mv_mobile_publisher_monthly_performance;
ANALYZE analytics.mv_mobile_taxonomy_monthly_performance;
ANALYZE analytics.mv_mobile_download_total_yearly;
ANALYZE analytics.mv_mobile_revenue_by_subgenre_yearly;
ANALYZE analytics.mv_mobile_revenue_by_game_yearly;
ANALYZE analytics.mv_mobile_new_games_performance_by_launch_year;
