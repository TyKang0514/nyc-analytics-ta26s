-- Clean and standardize NYC open restaurants data
-- One row per one application

WITH source AS (
   SELECT * FROM {{ source('raw', 'source_nyc_open_restaurant_apps') }}
), -- Easier to refer to the dbt reference to a long name table this way

cleaned AS (
   SELECT
       -- Get all columns from source, except ones we're transforming below
       -- To do cleaning on them or explicitly cast them as types just in case
       * EXCEPT (
           globalid, -- check
           time_of_submission, -- check           
           seating_interest_sidewalk, -- check (changed name)
           restaurant_name, -- check
           legal_business_name, -- check
           bulding_number, -- check
           street, -- check
           borough, -- check
           zip, --check
           latitude, --check
           longitude, --check
           sidewalk_dimensions_length, --check
           sidewalk_dimensions_width, --check
           sidewalk_dimensions_area, --check
           roadway_dimensions_length, --check
           roadway_dimensions_width, --check
           roadway_dimensions_area --check            
       ),

       -- Identifiers
       CAST(globalid AS STRING) AS application_id,

       -- Date/Time
       CAST(time_of_submission AS TIMESTAMP) AS time_of_submission,

       -- Application details
       CAST(seating_interest_sidewalk AS STRING) AS seating_interest,
       CAST(restaurant_name AS STRING) AS restaurant_name,
       CAST(legal_business_name AS STRING) AS legal_business_name,
       
       -- Location 
       CAST(bulding_number AS STRING) AS bulding_number,
       CAST(street AS STRING) AS street,
       CAST(borough AS STRING) AS borough, -- already standardized into 5 values

       -- Location - clean zip code
       CASE
           WHEN UPPER(TRIM(CAST(zip AS STRING))) IN ('N/A', 'NA') THEN NULL
           WHEN UPPER(TRIM(CAST(zip AS STRING))) = 'ANONYMOUS' THEN 'Anonymous'
           WHEN LENGTH(CAST(zip AS STRING)) = 5 THEN CAST(zip AS STRING)
           WHEN LENGTH(CAST(zip AS STRING)) = 9 THEN CAST(zip AS STRING)
           WHEN LENGTH(CAST(zip AS STRING)) = 10
               AND REGEXP_CONTAINS(CAST(zip AS STRING), r'^\d{5}-\d{4}')
           THEN CAST(zip AS STRING)
           ELSE NULL
       END AS zip,

       CAST(latitude AS DECIMAL) AS latitude,
       CAST(longitude AS DECIMAL) AS longitude,

       -- Sidewalk and Roadway Info
       CAST(sidewalk_dimensions_length AS DECIMAL) AS sidewalk_dimensions_length,
       CAST(sidewalk_dimensions_width AS DECIMAL) AS sidewalk_dimensions_width,
       CAST(sidewalk_dimensions_area AS DECIMAL) AS sidewalk_dimensions_area,
       CAST(roadway_dimensions_length AS DECIMAL) AS roadway_dimensions_length,
       CAST(roadway_dimensions_width AS DECIMAL) AS roadway_dimensions_width,
       CAST(roadway_dimensions_area AS DECIMAL) AS roadway_dimensions_area

   FROM source

   -- Filters
   -- WHERE (agency = 'DOT' OR agency_name LIKE '%Transportation%')
   WHERE globalid IS NOT NULL
   AND time_of_submission IS NOT NULL
   -- AND CAST(time_of_submission AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 YEAR)
   AND borough IS NOT NULL

   -- Deduplicate
   QUALIFY ROW_NUMBER() OVER (PARTITION BY globalid ORDER BY time_of_submission DESC) = 1
)

SELECT * FROM cleaned
-- All should be part of this table: stg_nyc_open_restaurants