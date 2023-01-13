-- Databricks SQL dashboard: NYC taxi operations (Oct-Dec 2022)

-- KPI tiles
SELECT COUNT(*)                                   AS trips,
       ROUND(SUM(total_amount), 0)                AS revenue,
       ROUND(AVG(total_amount), 2)                AS avg_ticket,
       ROUND(AVG(duration_min), 1)                AS avg_duration_min,
       ROUND(AVG(CASE WHEN payment_type = 1 THEN tip_pct END), 1) AS avg_card_tip_pct
FROM nyc_taxi_gold.fact_trips;

-- Daily trips with 7-day moving average
SELECT d.date,
       COUNT(f.trip_id)                                                      AS trips,
       ROUND(AVG(COUNT(f.trip_id)) OVER (ORDER BY d.date ROWS 6 PRECEDING), 0) AS trips_7d_avg,
       d.is_holiday
FROM nyc_taxi_gold.dim_date d
LEFT JOIN nyc_taxi_gold.fact_trips f ON f.pickup_date_key = d.date_key
GROUP BY d.date, d.is_holiday
ORDER BY d.date;

-- Demand heatmap: hour x weekday
SELECT d.day_name, f.pickup_hour, COUNT(*) AS trips
FROM nyc_taxi_gold.fact_trips f
JOIN nyc_taxi_gold.dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.day_name, f.pickup_hour;

-- Revenue by borough per month
SELECT DATE_TRUNC('month', pickup_date) AS month, borough, SUM(trips) AS trips, ROUND(SUM(revenue), 0) AS revenue
FROM nyc_taxi_gold.daily_borough_revenue
GROUP BY 1, 2 ORDER BY 1, revenue DESC;

-- Airport share of revenue
SELECT is_airport_trip, COUNT(*) AS trips, ROUND(SUM(total_amount), 0) AS revenue,
       ROUND(100 * SUM(total_amount) / SUM(SUM(total_amount)) OVER (), 1) AS revenue_share_pct
FROM nyc_taxi_gold.fact_trips
GROUP BY is_airport_trip;

-- Top 10 routes
SELECT route_rank, pickup_zone, dropoff_zone, trips, avg_total, avg_duration_min, p90_duration_min
FROM nyc_taxi_gold.top_routes WHERE route_rank <= 10 ORDER BY route_rank;

-- Rush hour slowdown: average speed by hour (Manhattan pickups)
SELECT f.pickup_hour, ROUND(AVG(f.avg_speed_mph), 1) AS avg_mph
FROM nyc_taxi_gold.fact_trips f
JOIN nyc_taxi_gold.dim_zone z ON f.pickup_zone_key = z.zone_key
WHERE z.borough = 'Manhattan'
GROUP BY f.pickup_hour ORDER BY f.pickup_hour;

-- Tip behaviour by hour (card only)
SELECT pickup_hour, avg_tip_pct, no_tip_pct FROM nyc_taxi_gold.tip_behaviour ORDER BY pickup_hour;

-- Top drivers per base (revenue per hour)
SELECT driver_base, driver_id, trips, revenue, revenue_per_hour
FROM nyc_taxi_gold.driver_performance WHERE rank_in_base <= 5
ORDER BY driver_base, rank_in_base;

-- Live surge windows (from the streaming job)
SELECT s.window_start, z.zone, s.trips, s.avg_trips, s.demand_ratio
FROM nyc_taxi_gold.surge_windows s
JOIN nyc_taxi_gold.dim_zone z ON s.PULocationID = z.zone_key
WHERE s.is_surge
ORDER BY s.window_start DESC LIMIT 25;

-- Forecast vs actual (test period)
SELECT hour_ts, SUM(trips) AS actual, ROUND(SUM(prediction), 1) AS predicted
FROM nyc_taxi_gold.demand_predictions
GROUP BY hour_ts ORDER BY hour_ts;

-- Data quality history
SELECT dataset, total, valid, quarantined, pass_rate, checked_at
FROM nyc_taxi_gold.data_quality ORDER BY checked_at DESC, dataset;
