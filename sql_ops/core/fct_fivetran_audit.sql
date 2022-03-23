with staging as (

            select * from {{ source('itrack_fivetran','fivetran_audit') }}

        ),

        final as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                done                                                        as done,
                message                                                     as message,
                progress                                                    as progress,
                rows_updated_or_inserted                                    as rows_updated_or_inserted,
                schema                                                      as schema,
                start                                                       as start,
                status                                                      as status,
                table                                                       as table,
                update_id                                                   as update_id,
                update_started                                              as update_started,
                _fivetran_synced                                            as _fivetran_synced,
      
            from staging

        )

        select * from final