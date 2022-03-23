with source as (

            select * from {{ source('itrack_fivetran','itrackrates') }}

        ),

        staging as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                ACCT                                                        as acct,
                BASERATE                                                    as baserate,
                CODE                                                        as code,
                EXPIRY                                                      as expiry,
                ITEMNO                                                      as itemno,
                MINHOURS                                                    as minhours,
                OVERRIDEDEFAULTRATES                                        as overridedefaultrates,
                RATE_1                                                      as rate_1,
                RATE_10                                                     as rate_10,
                RATE_11                                                     as rate_11,
                RATE_12                                                     as rate_12,
                RATE_13                                                     as rate_13,
                RATE_14                                                     as rate_14,
                RATE_15                                                     as rate_15,
                RATE_16                                                     as rate_16,
                RATE_17                                                     as rate_17,
                RATE_18                                                     as rate_18,
                RATE_19                                                     as rate_19,
                RATE_2                                                      as rate_2,
                RATE_20                                                     as rate_20,
                RATE_3                                                      as rate_3,
                RATE_4                                                      as rate_4,
                RATE_5                                                      as rate_5,
                RATE_6                                                      as rate_6,
                RATE_7                                                      as rate_7,
                RATE_8                                                      as rate_8,
                RATE_9                                                      as rate_9,
                RECID                                                       as recid,
                RECORDER                                                    as recorder,
                RV                                                          as rv,
                SID                                                         as sid,
                SPECIALRATE                                                 as specialrate,
                USERID                                                      as userid,
                _FIVETRAN_DELETED                                           as _fivetran_deleted,
                _FIVETRAN_SYNCED                                            as _fivetran_synced,
      
            from source

        )

        select * from staging