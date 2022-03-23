with staging as (

            select * from {{ source('itrack_fivetran','itrackrates') }}

        ),

        final as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                acct                                                        as acct,
                baserate                                                    as baserate,
                code                                                        as code,
                expiry                                                      as expiry,
                itemno                                                      as itemno,
                minhours                                                    as minhours,
                overridedefaultrates                                        as overridedefaultrates,
                rate_1                                                      as rate_1,
                rate_10                                                     as rate_10,
                rate_11                                                     as rate_11,
                rate_12                                                     as rate_12,
                rate_13                                                     as rate_13,
                rate_14                                                     as rate_14,
                rate_15                                                     as rate_15,
                rate_16                                                     as rate_16,
                rate_17                                                     as rate_17,
                rate_18                                                     as rate_18,
                rate_19                                                     as rate_19,
                rate_2                                                      as rate_2,
                rate_20                                                     as rate_20,
                rate_3                                                      as rate_3,
                rate_4                                                      as rate_4,
                rate_5                                                      as rate_5,
                rate_6                                                      as rate_6,
                rate_7                                                      as rate_7,
                rate_8                                                      as rate_8,
                rate_9                                                      as rate_9,
                recid                                                       as recid,
                recorder                                                    as recorder,
                rv                                                          as rv,
                sid                                                         as sid,
                specialrate                                                 as specialrate,
                userid                                                      as userid,
                _fivetran_deleted                                           as _fivetran_deleted,
                _fivetran_synced                                            as _fivetran_synced,
      
            from staging

        )

        select * from final