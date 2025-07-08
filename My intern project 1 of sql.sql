# MySQL Code for Uber Request Data Analysis

# 1. First, let's create the database and table structure


-- Create a new database for Uber data analysis
CREATE DATABASE IF NOT EXISTS uber_analysis;
USE uber_analysis;

-- Create the table to store trip request data
CREATE TABLE IF NOT EXISTS trip_requests (
    request_id INT PRIMARY KEY,
    pickup_point ENUM('Airport', 'City') NOT NULL,
    driver_id INT NOT NULL,
    status VARCHAR(20) NOT NULL,
    request_timestamp DATETIME NOT NULL,
    drop_timestamp DATETIME NOT NULL,
    -- Calculate trip duration in minutes as a generated column
    trip_duration_minutes INT AS (TIMESTAMPDIFF(MINUTE, request_timestamp, drop_timestamp)),
    -- Indexes for performance
    INDEX idx_pickup_point (pickup_point),
    INDEX idx_driver_id (driver_id),
    INDEX idx_request_time (request_timestamp),
    INDEX idx_drop_time (drop_timestamp)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


## 2. Load the data from CSV into the table


-- First, we need to prepare the data for import since the timestamp formats are inconsistent
-- This step would typically be done by preprocessing the CSV file to standardize date formats
-- For this example, we'll assume the data has been cleaned and is in a consistent format

-- Load data from CSV (this would be run from command line or via MySQL Workbench)
-- The actual LOAD DATA command would look like this:
/*
LOAD DATA INFILE '/path/to/Uber_Request_Data.csv'
INTO TABLE trip_requests
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(request_id, pickup_point, driver_id, status, @request_datetime, @drop_datetime)
SET 
    request_timestamp = STR_TO_DATE(@request_datetime, '%d/%m/%Y %H:%i'),
    drop_timestamp = STR_TO_DATE(@drop_datetime, '%d/%m/%Y %H:%i');
*/

-- For the purposes of this example, we'll insert some sample data manually
-- Note: In a real scenario, you would use the LOAD DATA command above
INSERT INTO trip_requests (request_id, pickup_point, driver_id, status, request_timestamp, drop_timestamp)
VALUES
(619, 'Airport', 1, 'Trip Completed', '2016-07-11 11:51:00', '2016-07-11 13:00:00'),
(867, 'Airport', 1, 'Trip Completed', '2016-07-11 17:57:00', '2016-07-11 18:47:00'),
(1807, 'City', 1, 'Trip Completed', '2016-07-12 09:17:00', '2016-07-12 09:58:00'),
-- Additional rows would be inserted here...
(2532, 'Airport', 1, 'Trip Completed', '2016-07-12 21:08:00', '2016-07-12 22:03:00');


#3. Basic Data Analysis Queries


-- Query 1: Total number of trips
SELECT COUNT(*) AS total_trips FROM trip_requests;

-- Query 2: Trips by pickup point
SELECT 
    pickup_point,
    COUNT(*) AS trip_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM trip_requests), 2) AS percentage
FROM trip_requests
GROUP BY pickup_point;

-- Query 3: Average trip duration by pickup point
SELECT 
    pickup_point,
    AVG(trip_duration_minutes) AS avg_duration_minutes,
    SEC_TO_TIME(AVG(TIME_TO_SEC(TIMEDIFF(drop_timestamp, request_timestamp)))) AS avg_duration_time
FROM trip_requests
GROUP BY pickup_point;

-- Query 4: Busiest hours for pickups
SELECT 
    HOUR(request_timestamp) AS hour_of_day,
    COUNT(*) AS trip_count
FROM trip_requests
GROUP BY hour_of_day
ORDER BY trip_count DESC;

-- Query 5: Most active drivers
SELECT 
    driver_id,
    COUNT(*) AS trips_completed,
    SEC_TO_TIME(AVG(TIME_TO_SEC(TIMEDIFF(drop_timestamp, request_timestamp)))) AS avg_trip_duration
FROM trip_requests
GROUP BY driver_id
ORDER BY trips_completed DESC
LIMIT 10;

## 4. Advanced Analysis Queries


-- Query 6: Hourly demand pattern for airport vs city pickups
SELECT 
    HOUR(request_timestamp) AS hour_of_day,
    SUM(CASE WHEN pickup_point = 'Airport' THEN 1 ELSE 0 END) AS airport_pickups,
    SUM(CASE WHEN pickup_point = 'City' THEN 1 ELSE 0 END) AS city_pickups
FROM trip_requests
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- Query 7: Driver utilization (time spent driving vs idle)
-- This calculates the total time each driver spent on trips
SELECT 
    driver_id,
    SEC_TO_TIME(SUM(TIME_TO_SEC(TIMEDIFF(drop_timestamp, request_timestamp)))) AS total_driving_time,
    COUNT(*) AS trips_completed
FROM trip_requests
GROUP BY driver_id
ORDER BY total_driving_time DESC;

-- Query 8: Time between consecutive trips for drivers
-- This helps identify driver availability patterns
SELECT 
    t1.driver_id,
    t1.request_id AS first_trip_id,
    t2.request_id AS next_trip_id,
    TIMESTAMPDIFF(MINUTE, t1.drop_timestamp, t2.request_timestamp) AS minutes_between_trips
FROM trip_requests t1
JOIN trip_requests t2 ON t1.driver_id = t2.driver_id 
    AND t2.request_timestamp > t1.drop_timestamp
    AND NOT EXISTS (
        SELECT 1 FROM trip_requests t3 
        WHERE t3.driver_id = t1.driver_id 
        AND t3.request_timestamp > t1.drop_timestamp 
        AND t3.request_timestamp < t2.request_timestamp
    )
ORDER BY t1.driver_id, t1.request_timestamp;

-- Query 9: Peak demand days
SELECT 
    DATE(request_timestamp) AS trip_date,
    COUNT(*) AS trips_count
FROM trip_requests
GROUP BY trip_date
ORDER BY trips_count DESC;

-- Query 10: Average wait time between request and pickup (if we had that data)
-- This would require additional columns in the dataset


## 5. Data Quality Checks
-- Check for data anomalies
-- Query 11: Trips with impossible durations (negative or too long)
SELECT 
    request_id,
    request_timestamp,
    drop_timestamp,
    trip_duration_minutes
FROM trip_requests
WHERE drop_timestamp < request_timestamp OR trip_duration_minutes > 180;

-- Query 12: Missing or null values check
SELECT 
    SUM(CASE WHEN request_id IS NULL THEN 1 ELSE 0 END) AS null_request_ids,
    SUM(CASE WHEN pickup_point IS NULL THEN 1 ELSE 0 END) AS null_pickup_points,
    SUM(CASE WHEN driver_id IS NULL THEN 1 ELSE 0 END) AS null_driver_ids,
    SUM(CASE WHEN request_timestamp IS NULL THEN 1 ELSE 0 END) AS null_request_times,
    SUM(CASE WHEN drop_timestamp IS NULL THEN 1 ELSE 0 END) AS null_drop_times
FROM trip_requests;

-- Query 13: Check for duplicate request IDs
SELECT 
    request_id,
    COUNT(*) AS count
FROM trip_requests
GROUP BY request_id
HAVING COUNT(*) > 1;


## 6. Create Views for Common Reports


-- View 1: Daily summary statistics
CREATE VIEW daily_summary AS
SELECT 
    DATE(request_timestamp) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(CASE WHEN pickup_point = 'Airport' THEN 1 ELSE 0 END) AS airport_trips,
    SUM(CASE WHEN pickup_point = 'City' THEN 1 ELSE 0 END) AS city_trips,
    AVG(trip_duration_minutes) AS avg_duration_minutes
FROM trip_requests
GROUP BY trip_date;

-- View 2: Driver performance metrics
CREATE VIEW driver_performance AS
SELECT 
    driver_id,
    COUNT(*) AS trips_completed,
    SEC_TO_TIME(SUM(TIME_TO_SEC(TIMEDIFF(drop_timestamp, request_timestamp)))) AS total_driving_time,
    AVG(trip_duration_minutes) AS avg_trip_duration,
    MIN(request_timestamp) AS first_trip,
    MAX(drop_timestamp) AS last_trip
FROM trip_requests
GROUP BY driver_id;


## 7. Stored Procedures for Common Analyses


-- Procedure 1: Get driver's daily activity
DELIMITER //
CREATE PROCEDURE GetDriverActivity(IN driver_id_param INT)
BEGIN
    SELECT 
        DATE(request_timestamp) AS trip_date,
        COUNT(*) AS trips_completed,
        SEC_TO_TIME(SUM(TIME_TO_SEC(TIMEDIFF(drop_timestamp, request_timestamp)))) AS total_driving_time,
        MIN(request_timestamp) AS first_trip_time,
        MAX(drop_timestamp) AS last_trip_time
    FROM trip_requests
    WHERE driver_id = driver_id_param
    GROUP BY trip_date
    ORDER BY trip_date;
END //
DELIMITER ;

-- Procedure 2: Get demand heatmap by hour and pickup point
DELIMITER //
CREATE PROCEDURE GetDemandHeatmap()
BEGIN
    SELECT 
        HOUR(request_timestamp) AS hour_of_day,
        pickup_point,
        COUNT(*) AS trip_count
    FROM trip_requests
    GROUP BY hour_of_day, pickup_point
    ORDER BY hour_of_day, pickup_point;
END //
DELIMITER ;


## 8. Example Usage of the Stored Procedures


-- Get activity for driver 5
CALL GetDriverActivity(5);

-- Get the demand heatmap
CALL GetDemandHeatmap();


# 9. Optimization and Maintenance

-- Create a summary table for faster reporting
CREATE TABLE trip_daily_summary (
    summary_date DATE PRIMARY KEY,
    total_trips INT,
    airport_trips INT,
    city_trips INT,
    avg_duration_minutes DECIMAL(10,2),
    last_updated TIMESTAMP
);

-- Procedure to refresh the summary table
DELIMITER //
CREATE PROCEDURE RefreshDailySummary()
BEGIN
    TRUNCATE TABLE trip_daily_summary;
    
    INSERT INTO trip_daily_summary
    SELECT 
        DATE(request_timestamp) AS summary_date,
        COUNT(*) AS total_trips,
        SUM(CASE WHEN pickup_point = 'Airport' THEN 1 ELSE 0 END) AS airport_trips,
        SUM(CASE WHEN pickup_point = 'City' THEN 1 ELSE 0 END) AS city_trips,
        AVG(trip_duration_minutes) AS avg_duration_minutes,
        CURRENT_TIMESTAMP
    FROM trip_requests
    GROUP BY summary_date;
END //
DELIMITER ;

-- Run the refresh procedure
CALL RefreshDailySummary();




#Explanation of Key Components:

#1 Table Structure: The trip_requests table is designed with appropriate data types and includes a computed column for trip duration.

#2 Indexes: We've added indexes on frequently queried columns to improve performance.

#3 Data Loading: The commented LOAD DATA command shows how you would import the CSV data after standardizing the date formats.

#4 Basic Analysis: Simple queries to understand trip volumes, durations, and driver activity.

#5 Advanced Analysis: More complex queries that examine patterns like hourly demand and driver utilization.

#6 Data Quality: Important checks to ensure data integrity before analysis.

#7 Views and Stored Procedures: These create reusable components for common reports and analyses.

#8 Optimization: The summary table and refresh procedure demonstrate how to optimize for reporting performance.