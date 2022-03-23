with staging as (

            select * from {{ source('itrack_fivetran','itrackpophdr') }}

        ),

        final as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                acct                                                        as acct,
                acctname                                                    as acctname,
                addr_1                                                      as addr_1,
                addr_2                                                      as addr_2,
                addr_3                                                      as addr_3,
                addr_4                                                      as addr_4,
                auth                                                        as auth,
                auto                                                        as auto,
                basegoods                                                   as basegoods,
                basevat                                                     as basevat,
                colinstruction                                              as colinstruction,
                contno                                                      as contno,
                currency                                                    as currency,
                currid                                                      as currid,
                currtbl                                                     as currtbl,
                deladdr                                                     as deladdr,
                deladdr_1                                                   as deladdr_1,
                deladdr_2                                                   as deladdr_2,
                deladdr_3                                                   as deladdr_3,
                deladdr_4                                                   as deladdr_4,
                delcode                                                     as delcode,
                deldepot                                                    as deldepot,
                delemail                                                    as delemail,
                delname                                                     as delname,
                delpcode                                                    as delpcode,
                depot                                                       as depot,
                discount                                                    as discount,
                duedate                                                     as duedate,
                duetime                                                     as duetime,
                euro                                                        as euro,
                exrate                                                      as exrate,
                extmemo                                                     as extmemo,
                faxno                                                       as faxno,
                goods                                                       as goods,
                intmemo                                                     as intmemo,
                lastdel                                                     as lastdel,
                layout                                                      as layout,
                linkedto                                                    as linkedto,
                liveposttype                                                as liveposttype,
                ordby                                                       as ordby,
                orddate                                                     as orddate,
                ordno                                                       as ordno,
                origordno                                                   as origordno,
                ourref                                                      as ourref,
                postcode                                                    as postcode,
                projectno                                                   as projectno,
                quoteref                                                    as quoteref,
                recid                                                       as recid,
                recorder                                                    as recorder,
                rtndate                                                     as rtndate,
                rtninstruction                                              as rtninstruction,
                rtntime                                                     as rtntime,
                rv                                                          as rv,
                settdays                                                    as settdays,
                settdisc                                                    as settdisc,
                showdaybook                                                 as showdaybook,
                sid                                                         as sid,
                spcode                                                      as spcode,
                status                                                      as status,
                taxareacode                                                 as taxareacode,
                telno                                                       as telno,
                theirref                                                    as theirref,
                type                                                        as type,
                vat                                                         as vat,
                xhacct                                                      as xhacct,
                _fivetran_deleted                                           as _fivetran_deleted,
                _fivetran_synced                                            as _fivetran_synced,
      
            from staging

        )

        select * from final