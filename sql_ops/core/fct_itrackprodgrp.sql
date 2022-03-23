with staging as (

            select * from {{ source('itrack_fivetran','itrackprodgrp') }}

        ),

        final as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                anlcode                                                     as anlcode,
                catdesc                                                     as catdesc,
                code                                                        as code,
                name                                                        as name,
                recid                                                       as recid,
                recorder                                                    as recorder,
                rv                                                          as rv,
                sid                                                         as sid,
                _fivetran_deleted                                           as _fivetran_deleted,
                _fivetran_synced                                            as _fivetran_synced,
      
            from staging

        )

        select * from final