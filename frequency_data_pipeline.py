import duckdb as dk

con = dk.connect('data/vermont.duckdb')

# read raw data into duckdb
con.execute("""--sql
DROP TABLE IF EXISTS sightings_raw;

CREATE TABLE sightings_raw AS
SELECT * 
FROM read_csv_auto('data/ebd_US-VT_smp_relJul-2025.txt',
    delim='\t', sample_size=-1)
;
""")
print("Done reading raw data into duckdb")
print("Read {} rows".format(con.execute("SELECT COUNT(*) FROM sightings_raw").fetchone()[0]))

# rename columns for easier querying
# keep only complete checklists from hotspot locations
con.execute("""--sql
DROP TABLE IF EXISTS sightings_staging;
CREATE TABLE sightings_staging AS
SELECT
    TRY_CAST(REGEXP_EXTRACT("GLOBAL UNIQUE IDENTIFIER", '(\\d+)$') AS BIGINT) AS global_id,
    TRY_CAST("LAST EDITED DATE" AS DATE) AS last_edited_date,
    "TAXONOMIC ORDER" as taxonomic_order,
    "CATEGORY" as species_category,
    "COMMON NAME" as common_name,
    "SCIENTIFIC NAME" as scientific_name,
    TRY_CAST("OBSERVATION COUNT" AS INT) AS observation_count,
    "COUNTRY" as country,
    "COUNTRY CODE" as country_code,
    "STATE" as state,
    "STATE CODE" as state_code,
    "COUNTY" as county,
    "COUNTY CODE" as county_code,
    "LOCALITY" as locality,
    TRY_CAST(REGEXP_EXTRACT("LOCALITY ID", '(\\d+)$') AS BIGINT) AS locality_id,
    "LOCALITY TYPE" as locality_type,
    "LATITUDE"::FLOAT AS latitude,
    "LONGITUDE"::FLOAT AS longitude,
    TRY_CAST("OBSERVATION DATE" AS DATE) AS observation_date,
    TRY_CAST("TIME OBSERVATIONS STARTED" AS TIME) AS time_observations_started,
    TRY_CAST(REGEXP_EXTRACT("OBSERVER ID", '(\\d+)$') AS BIGINT) AS observer_id,
    TRY_CAST(REGEXP_EXTRACT("SAMPLING EVENT IDENTIFIER", '(\\d+)$') AS BIGINT) AS sampling_id,
    "OBSERVATION TYPE" as observation_type,
    "DURATION MINUTES"::INT AS duration_minutes,
    "EFFORT DISTANCE KM"::FLOAT AS effort_distance_km,
    "NUMBER OBSERVERS"::INT AS number_observers,
    "ALL SPECIES REPORTED"::BOOLEAN as all_species_reported,
    TRY_CAST(REGEXP_EXTRACT("GROUP IDENTIFIER", '(\\d+)$') AS BIGINT) AS group_id  
FROM sightings_raw
WHERE locality_type == 'H' AND
    (species_category == 'species' OR species_category == 'issf' OR species_category == 'form') AND
    all_species_reported IS TRUE
;
""")
print("Done staging")

# drop duplicate observations from group checklists
con.execute("""--sql
DROP TABLE IF EXISTS sightings_clean;
CREATE TABLE sightings_clean AS
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY group_id, common_name
            ORDER BY sampling_id --figure out something else to order by?
        ) as row_num
    FROM sightings_staging
) t
WHERE group_id IS NULL or row_num = 1
;
""")

# group checklists and sighting counts by locality, date, and species
con.execute("""--sql
DROP TABLE IF EXISTS detection_frequencies;
CREATE TABLE detection_frequencies AS
WITH checklists AS (
    SELECT
        locality,
        locality_id,
        DAYOFYEAR(observation_date) AS day_of_year,
        COUNT(DISTINCT sampling_id) AS total_checklists
    FROM sightings_clean
    GROUP BY locality, locality_id, day_of_year
), detections AS (
    SELECT
        locality_id,
        DAYOFYEAR(observation_date) AS day_of_year,
        common_name,
        COUNT(DISTINCT sampling_id) AS total_detections
    FROM sightings_clean
    GROUP BY locality_id, day_of_year, common_name
), species AS (
    SELECT DISTINCT
        locality_id,
        common_name
    FROM sightings_clean
)

SELECT
    c.locality,
    c.locality_id,
    c.day_of_year,
    s.common_name,
    COALESCE(d.total_detections, 0) AS total_detections,
    c.total_checklists
FROM checklists c
JOIN species s
    ON s.locality_id = c.locality_id
LEFT JOIN detections d
    ON d.locality_id = c.locality_id AND d.day_of_year = c.day_of_year AND d.common_name = s.common_name
ORDER BY c.locality_id, c.day_of_year DESC
;
""")

# calculate rolling average frequencies
con.execute("""--sql
DROP TABLE IF EXISTS rolling_avg_freq;
CREATE TABLE rolling_avg_freq AS
WITH wrapped AS (
    SELECT
        *,
        day_of_year AS wrapped_day_of_year
    FROM detection_frequencies

    UNION ALL

    SELECT
        *,
        day_of_year + 366 AS wrapped_day_of_year
    FROM detection_frequencies
    WHERE day_of_year <= 6
)

SELECT
    locality,
    locality_id,
    day_of_year,
    common_name,
    total_detections,
    total_checklists,
    SUM(total_detections) OVER w / SUM(total_checklists) OVER w as rolling_avg_freq
FROM wrapped
WHERE 4 <= wrapped_day_of_year AND wrapped_day_of_year <= 369
WINDOW w AS (
    PARTITION BY locality_id, common_name
    ORDER BY wrapped_day_of_year
    RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING
)
ORDER BY day_of_year
;
""")