import duckdb as dk

con = dk.connect('///data/vermont.duckdb')

# read in raw personal ebird data
con.execute("""--sql
DROP TABLE IF EXISTS personal_raw;
CREATE TABLE personal_raw AS
    SELECT *
    FROM read_csv('data/MyEBirdData.csv', header=true, delim=',', quote='"', null_padding=true)
;
""")

# read in columns of interest and cast to proper types
# drop subspecies classification
# then drop records with /, sp., or x in the species name, which are hybrids or species groups
con.execute("""--sql
DROP TABLE IF EXISTS personal_staging;
CREATE TABLE personal_staging AS
SELECT
    "Taxonomic Order" AS taxonomic_order,
    split_part("Common Name", ' (', 1) AS common_name,
    "Scientific Name" AS scientific_name,
    TRY_CAST("Count" AS INT) AS observation_count,
    "State/Province" AS state,
    "County" AS county,
    "Location" AS locality,
    TRY_CAST(REGEXP_EXTRACT("Location ID", '(\\d+)$') AS BIGINT) AS locality_id,
    "Latitude"::FLOAT AS latitude,
    "Longitude"::FLOAT AS longitude,
    TRY_CAST("Date" AS DATE) AS observation_date,
    TRY_CAST(strptime("Time", '%I:%M %p') AS TIME) AS time_observations_started,
    TRY_CAST(REGEXP_EXTRACT("Submission ID", '(\\d+)$') AS BIGINT) AS sampling_id,
    "Protocol" AS observation_type,
    "Duration (Min)"::INT AS duration_minutes,
    "Distance Traveled (km)"::FLOAT AS effort_distance_km,
    "Number of Observers"::INT AS number_observers,
    "All Obs Reported"::BOOLEAN AS all_species_reported
FROM personal_raw
WHERE NOT (contains(common_name, '/') OR contains(common_name, ' sp.') OR contains(scientific_name, ' x '))
""")

# create user life list
# keep first observation of each species
con.execute("""--sql
DROP TABLE IF EXISTS life_list;
CREATE TABLE life_list AS
SELECT
    taxonomic_order,
    common_name,
    scientific_name,
    observation_date,
    locality,
    rn
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY common_name ORDER BY observation_date) AS rn
    FROM personal_staging
)
WHERE rn = 1
;
ALTER TABLE life_list DROP COLUMN rn;
""")