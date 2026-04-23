
-- Restaurant dimension: One row per physical location
-- Grain: Unique combination of food_service_establishment + business_address

WITH restaurants AS (
    SELECT
        food_service_establishment,
        business_address,
        -- For other attributes, pick representative values when duplicates exist
        MAX(restaurant_name) as restaurant_name,  -- Or MIN, or most common
        MAX(legal_business_name) as legal_business_name,
        MAX(doing_business_as_dba) as doing_business_as_dba,
        -- For coordinates, average if geocoding varies slightly
        AVG(latitude) as latitude,
        AVG(longitude) as longitude
    FROM {{ ref('stg_nyc_open_restaurant_apps') }}
    WHERE food_service_establishment IS NOT NULL  -- Must have permit
        OR business_address IS NOT NULL  -- Or must have address
    GROUP BY
        food_service_establishment,
        business_address
),

restaurant_dimension AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'food_service_establishment',
            'business_address'
        ]) }} AS restaurant_key,

        restaurant_name,
        legal_business_name,
        doing_business_as_dba,
        business_address,
        latitude,
        longitude,
        food_service_establishment

    FROM restaurants
)

SELECT * FROM restaurant_dimension