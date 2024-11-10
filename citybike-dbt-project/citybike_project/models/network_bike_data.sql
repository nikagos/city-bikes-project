SELECT bs.*
	 , n.city AS network_city
     , n.company
	 , n.network_name
	 , n.country AS network_country
FROM 
    {{ source('main', 'bike_station_data') }} bs
JOIN 
	{{ source('main', 'networks') }} n
ON bs.network_id = n.network_id