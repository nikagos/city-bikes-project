SELECT bs.*
     , n.city AS network_city
     , n.company
     , n.network_name
     , n.country AS network_country
FROM 
    {{ source('dezoomcamp', 'bike_station_data_20241120') }} bs
JOIN 
    {{ source('dezoomcamp', 'networks_20241120') }} n
ON bs.network_id = n.network_id