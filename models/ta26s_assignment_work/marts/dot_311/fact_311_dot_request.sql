-- Grain: one row per request

-- --Pattern for fact table model:
-- --   WITH
-- --     [fact_source] AS (SELECT * FROM staging),
-- --     [dim1] AS (SELECT surrogate_key, join_keys FROM dim1),
-- --     [dim2] AS (SELECT surrogate_key, join_keys FROM dim2),
-- --     ...
-- --     final AS (
-- --       SELECT
-- --         [fact fields from staging data, maybe renamed with AS ___],
-- --         [dim1 surrogate key] as dim1_key_or_whatever_name,
-- --         [dim2 surrogate key] as dim2_key_or_whatever_name, ...
-- --       FROM [fact_source]
-- --       LEFT JOIN [dim1] ON ... (join fields match)
-- --       LEFT JOIN [dim2] ON ... (join fields match)
-- --     )
-- --   SELECT * FROM final
------

-- WRITE THIS 1st - Start: all data from staging for relevant data
  WITH requests AS (
      SELECT * FROM {{ ref('stg_nyc_311_dot') }}
  ),
-- Continue: named dimension CTEs that you'll need, either the sructure or build them out slowly. 

-- WRITE this - THESE 2nd... get all the dimension stuff you need
-- What context describes each row? Then, ONLY the surrogate key + join fields from each dimension.
-- Can reference back to the dimension tables for that.
  dim_date AS (
      SELECT date_key, full_date FROM {{ ref('dim_date') }}
  ),

  dim_location AS (
      SELECT location_key, borough, zip_code FROM {{ ref('dim_location') }}
  ),

  dim_complaint AS (
      SELECT
          complaint_type_key,
          complaint_type,
          descriptor
      FROM {{ ref('dim_complaint_type') }}
  ),
 -- WRITE this - the structure for this 3rd: final AS ( ... ) + see end of file as well
  final AS (
      -- WRITE this 5th -- fill in the select statement, first surrogate key, then each other thing needed in the fact from source
      -- Some of which need to come from the LEFT JOINed tables
      SELECT
          -- Surrogate key, generated from unique id in data.
          -- If there were none such (rare), could generate the surrogate id from a combo of things you are sure are unique in staging data.
          {{ dbt_utils.generate_surrogate_key(['r.request_id']) }} AS request_key,

          -- Natural key, direct from staging data
          r.request_id,

          -- Timestamps, direct from staging data
          r.created_date AS request_created_at,
          r.closed_date AS request_closed_at,

          -- Foreign keys: I usually start writing these as ??? AS created_date_key, etc
          d_created.date_key AS created_date_key,
          d_closed.date_key AS closed_date_key,
          l.location_key,
          c.complaint_type_key,

          -- Request location details
          r.incident_address,
          r.address_type,
          r.street_name,
          r.cross_street_1,
          r.cross_street_2,
          r.latitude,
          r.longitude,

          -- Measures: small calculations included in a fact table
          CASE
              WHEN r.closed_date IS NOT NULL
              THEN DATE_DIFF(CAST(r.closed_date AS DATE), CAST(r.created_date AS DATE), DAY)
              ELSE NULL
          END AS days_to_close,

          -- Flags, support easy fact queries (e.g. 'all requests that are closed...')
          CASE WHEN UPPER(r.status) = 'CLOSED' THEN TRUE ELSE FALSE END AS is_closed,

          -- Additional attributes
          r.status,
          r.method_of_submission AS channel_type,
          --r.open_data_channel_type AS channel_type,
          r.resolution_description
    
      -- **** INSIDE that, WRITE THIS 4th, join by join:
      FROM requests r -- All staging data

      LEFT JOIN dim_date d_created -- Date dimension to get created date
          ON CAST(r.created_date AS DATE) = d_created.full_date -- Cast as date to match yyyy-mm-dd date format

      LEFT JOIN dim_date d_closed
          ON CAST(r.closed_date AS DATE) = d_closed.full_date -- Cast as date to match yyyy-mm-dd date format

      LEFT JOIN dim_location l
          ON r.borough = l.borough
          AND r.incident_zip = l.zip_code
          -- AND r.zip_code = l.zip_code

      LEFT JOIN dim_complaint c
          ON r.complaint_type = c.complaint_type
          AND COALESCE(r.descriptor, '') = COALESCE(c.descriptor, '') -- COALESCE gets the first non-null thing in the list
            -- The COALESCE ensures that when the fact table has a request when eg:
            --  - complaint_type = "Street Condition"
            --  - descriptor = NULL
            -- If r.descriptor is null, you still want to be able to match to the correct foreign key in dim_complaint
            -- FOR EXAMPLE:
                -- complaint_type_key | complaint_type      | descriptor
                -- -------------------|---------------------|-------------
                -- key_123           | Street Condition    | Pothole
                -- key_456           | Street Condition    | NULL (converted to '')
                -- key_789           | Traffic Signal      | Light Out
                -- key_101           | Traffic Signal      | NULL (converted to '')
  )
 -- Also WRITE THIS 3rd
  SELECT * FROM final