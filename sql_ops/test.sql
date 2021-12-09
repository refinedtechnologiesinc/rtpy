WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'ctm',
            'bookings_hotel'
        ) }}
),
staging AS (
    SELECT
        SHA1(ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(*), ',')) AS id,
        address AS address,
        booking_source AS booking_source,
        checkin_date AS checkin_date,
        checkout_date AS checkout_date,
        city AS city,
        clients_name AS clients_name,
        client_name AS client_name,
        confirmation_number AS confirmation_number,
        country_name AS country_name,
        hotel AS hotel,
        invoice_date AS invoice_date,
        invoice_number AS invoice_number,
        nights AS nights,
        pid_number AS pid_number,
        pnr AS pnr,
        rate AS rate,
        total_amount AS total_amount,
        traveler_name AS traveler_name,
        travel_arranger AS travel_arranger,
        _FIVETRAN_BATCH AS _fivetran_batch,
        _FIVETRAN_INDEX AS _fivetran_index,
        _FIVETRAN_SYNCED AS _fivetran_synced
    FROM
        source)
    SELECT
        *
    FROM
        staging
