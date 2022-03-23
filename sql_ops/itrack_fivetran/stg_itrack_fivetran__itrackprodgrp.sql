with source as (

            select * from {{ source('itrack_fivetran','itrackprodgrp') }}

        ),

        staging as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                ANLCODE                                                     as anlcode,
                CATDESC                                                     as catdesc,
                CODE                                                        as code,
                NAME                                                        as name,
                RECID                                                       as recid,
                RECORDER                                                    as recorder,
                RV                                                          as rv,
                SID                                                         as sid,
                _FIVETRAN_DELETED                                           as _fivetran_deleted,
                _FIVETRAN_SYNCED                                            as _fivetran_synced,
      
            from source

        )

        select * from staging