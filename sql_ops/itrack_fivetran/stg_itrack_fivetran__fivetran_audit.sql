with source as (

            select * from {{ source('itrack_fivetran','fivetran_audit') }}

        ),

        staging as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                DONE                                                        as done,
                ID                                                          as id,
                MESSAGE                                                     as message,
                PROGRESS                                                    as progress,
                ROWS_UPDATED_OR_INSERTED                                    as rows_updated_or_inserted,
                SCHEMA                                                      as schema,
                START                                                       as start,
                STATUS                                                      as status,
                TABLE                                                       as table,
                UPDATE_ID                                                   as update_id,
                UPDATE_STARTED                                              as update_started,
                _FIVETRAN_SYNCED                                            as _fivetran_synced,
      
            from source

        )

        select * from staging