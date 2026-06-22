from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CatalogEntry:
    name: str
    grain: str
    description: str
    examples: tuple[dict[str, str], ...]


CATALOG: dict[str, CatalogEntry] = {
    "analytics.agg_game_performance_daily": CatalogEntry(
        name="analytics.agg_game_performance_daily",
        grain="date + country + unified_app_id",
        description="Daily game performance aggregated across linked app ids and mobile platforms.",
        examples=(
            {
                "question": "Show daily performance for one game in VN.",
                "sql": (
                    "SELECT date, country, game_name, downloads, revenue "
                    "FROM analytics.agg_game_performance_daily "
                    "WHERE unified_app_id = 'YOUR_UNIFIED_APP_ID' AND country = 'VN' "
                    "AND date::date >= DATE '2026-01-01' "
                    "ORDER BY date DESC LIMIT 30"
                ),
            },
        ),
    ),
    "analytics.agg_game_performance_monthly": CatalogEntry(
        name="analytics.agg_game_performance_monthly",
        grain="month + country + unified_app_id",
        description="Monthly game performance aggregated across linked apps, including cleaned publisher names.",
        examples=(
            {
                "question": "Show top games by monthly revenue in VN.",
                "sql": (
                    "SELECT month, game_name, cleaned_publisher_name, downloads, revenue "
                    "FROM analytics.agg_game_performance_monthly "
                    "WHERE country = 'VN' AND month = DATE '2026-05-01' "
                    "ORDER BY revenue DESC LIMIT 20"
                ),
            },
        ),
    ),
    "analytics.agg_game_performance_yearly": CatalogEntry(
        name="analytics.agg_game_performance_yearly",
        grain="year + country + unified_app_id",
        description="Yearly game performance by country with downloads and revenue.",
        examples=(
            {
                "question": "Find the highest revenue games in VN for a given year.",
                "sql": (
                    "SELECT year, country, game_name, downloads, revenue "
                    "FROM analytics.agg_game_performance_yearly "
                    "WHERE country = 'VN' AND year = 2025 "
                    "ORDER BY revenue DESC LIMIT 20"
                ),
            },
        ),
    ),
    "analytics.agg_subgenre_performance_monthly": CatalogEntry(
        name="analytics.agg_subgenre_performance_monthly",
        grain="month + game_class + game_genre + subgenre",
        description="Monthly performance rolled up to subgenre level.",
        examples=(
            {
                "question": "Track monthly Puzzle subgenre downloads.",
                "sql": (
                    "SELECT month, game_class, game_genre, subgenre, downloads, revenue "
                    "FROM analytics.agg_subgenre_performance_monthly "
                    "WHERE game_genre = 'Puzzle' "
                    "ORDER BY month DESC LIMIT 24"
                ),
            },
        ),
    ),
    "analytics.agg_subgenre_performance_yearly": CatalogEntry(
        name="analytics.agg_subgenre_performance_yearly",
        grain="year + game_class + game_genre + subgenre",
        description="Yearly performance rolled up to subgenre level.",
        examples=(
            {
                "question": "Compare yearly subgenre revenue.",
                "sql": (
                    "SELECT year, game_class, game_genre, subgenre, downloads, revenue "
                    "FROM analytics.agg_subgenre_performance_yearly "
                    "WHERE year BETWEEN 2021 AND 2025 "
                    "ORDER BY year, revenue DESC LIMIT 100"
                ),
            },
        ),
    ),
    "analytics.agg_new_game_new_performance": CatalogEntry(
        name="analytics.agg_new_game_new_performance",
        grain="launch year + unified_app_id",
        description="New game launch performance windows with estimated release date and early revenue benchmarks.",
        examples=(
            {
                "question": "Find strongest new launches in 2025.",
                "sql": (
                    "SELECT year, game_name, estimated_release_date, first_30d_revenue_usd "
                    "FROM analytics.agg_new_game_new_performance "
                    "WHERE year = 2025 "
                    "ORDER BY first_30d_revenue_usd DESC NULLS LAST LIMIT 20"
                ),
            },
        ),
    ),
    "analytics.mv_mobile_download_total_yearly": CatalogEntry(
        name="analytics.mv_mobile_download_total_yearly",
        grain="year",
        description="Yearly total downloads with 2025 annualization metadata.",
        examples=(
            {
                "question": "Check annualized total downloads by year.",
                "sql": (
                    "SELECT year, total_downloads, data_through_2025, days_elapsed_2025 "
                    "FROM analytics.mv_mobile_download_total_yearly "
                    "ORDER BY year"
                ),
            },
        ),
    ),
}


def allowed_catalog_objects() -> tuple[str, ...]:
    return tuple(CATALOG.keys())
