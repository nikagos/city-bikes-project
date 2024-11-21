SELECT * EXCEPT (timestamp)
FROM (
        SELECT bs.* 
            , FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP(bs.timestamp), 'America/New_York') AS timestamp_ny
            , n.city AS network_city
            , c.company_id
            , n.company
            , n.network_name
            , n.country AS network_country
        FROM 
            {{ source('dezoomcamp', 'bike_station_data') }} bs
        JOIN 
            {{ source('dezoomcamp', 'networks') }} n
        ON bs.network_id = n.network_id
        LEFT JOIN 
            {{ ref('companies') }} c
        ON n.company = c.company
        AND n.country = c.country
)