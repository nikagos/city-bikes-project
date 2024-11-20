SELECT {{ dbt_utils.generate_surrogate_key(['company'
                                          , 'country']) }} AS company_id
     , *
FROM
    (
        SELECT 
            company
            , country AS country
        FROM 
            {{ source('dezoomcamp', 'networks') }}
        WHERE
            company IS NOT NULL
        GROUP BY 1,2
    )
