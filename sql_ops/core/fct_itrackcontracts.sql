with staging as (

            select * from {{ source('itrack_fivetran','itrackcontracts') }}

        ),

        final as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                acct                                                        as acct,
                acctname                                                    as acctname,
                aicode                                                      as aicode,
                amended                                                     as amended,
                amtpaid                                                     as amtpaid,
                amtused                                                     as amtused,
                budgetcostadditional                                        as budgetcostadditional,
                budgetcostdelcharges                                        as budgetcostdelcharges,
                budgetcosthire                                              as budgetcosthire,
                budgetcostother                                             as budgetcostother,
                budgetcostresource                                          as budgetcostresource,
                budgetcostsale                                              as budgetcostsale,
                budgetcostxhire                                             as budgetcostxhire,
                budgetcostxhireresource                                     as budgetcostxhireresource,
                budgetrevdelcharges                                         as budgetrevdelcharges,
                budgetrevhire                                               as budgetrevhire,
                budgetrevother                                              as budgetrevother,
                budgetrevresource                                           as budgetrevresource,
                budgetrevsale                                               as budgetrevsale,
                budgetrevxhire                                              as budgetrevxhire,
                budgetrevxhireresource                                      as budgetrevxhireresource,
                ccrec                                                       as ccrec,
                chasedate                                                   as chasedate,
                chaserecid                                                  as chaserecid,
                chasetime                                                   as chasetime,
                chaseuser                                                   as chaseuser,
                ciflag                                                      as ciflag,
                colinstruction                                              as colinstruction,
                collectiondate                                              as collectiondate,
                collectiontime                                              as collectiontime,
                contact                                                     as contact,
                contactid                                                   as contactid,
                contdate                                                    as contdate,
                contname                                                    as contname,
                contno                                                      as contno,
                contrecid                                                   as contrecid,
                credsupcde                                                  as credsupcde,
                currency                                                    as currency,
                custcollect                                                 as custcollect,
                custom                                                      as custom,
                custom10                                                    as custom10,
                custom11                                                    as custom11,
                custom12                                                    as custom12,
                custom13                                                    as custom13,
                custom14                                                    as custom14,
                custom15                                                    as custom15,
                custom2                                                     as custom2,
                custom3                                                     as custom3,
                custom4                                                     as custom4,
                custom5                                                     as custom5,
                custom6                                                     as custom6,
                custom7                                                     as custom7,
                custom8                                                     as custom8,
                custom9                                                     as custom9,
                custommemo                                                  as custommemo,
                custreturn                                                  as custreturn,
                cycles                                                      as cycles,
                datetype                                                    as datetype,
                dealamt                                                     as dealamt,
                dealfixed                                                   as dealfixed,
                dealflag                                                    as dealflag,
                dealvatinc                                                  as dealvatinc,
                defins                                                      as defins,
                defnlcc                                                     as defnlcc,
                defnlcode                                                   as defnlcode,
                defnldept                                                   as defnldept,
                deinstallfromdate                                           as deinstallfromdate,
                deinstallfromtime                                           as deinstallfromtime,
                deinstalltodate                                             as deinstalltodate,
                deinstalltotime                                             as deinstalltotime,
                deladdr_1                                                   as deladdr_1,
                deladdr_2                                                   as deladdr_2,
                deladdr_3                                                   as deladdr_3,
                deladdr_4                                                   as deladdr_4,
                delcode                                                     as delcode,
                delcont                                                     as delcont,
                deldate                                                     as deldate,
                deldepot                                                    as deldepot,
                delemail                                                    as delemail,
                delfax                                                      as delfax,
                delinstruction                                              as delinstruction,
                delname                                                     as delname,
                delpcode                                                    as delpcode,
                deltel                                                      as deltel,
                deltime                                                     as deltime,
                deposit                                                     as deposit,
                depused                                                     as depused,
                discount                                                    as discount,
                duedays                                                     as duedays,
                egoods                                                      as egoods,
                estretd                                                     as estretd,
                estrett                                                     as estrett,
                estrevno                                                    as estrevno,
                euro                                                        as euro,
                evat                                                        as evat,
                exrate                                                      as exrate,
                faxno                                                       as faxno,
                fcgoods                                                     as fcgoods,
                fcprices                                                    as fcprices,
                fctotal                                                     as fctotal,
                fcvat                                                       as fcvat,
                fromest                                                     as fromest,
                geocode                                                     as geocode,
                gmacct                                                      as gmacct,
                goods                                                       as goods,
                group                                                       as group,
                headerrevision                                              as headerrevision,
                hiredate                                                    as hiredate,
                hiredepot                                                   as hiredepot,
                hiretime                                                    as hiretime,
                inrun                                                       as inrun,
                insreq                                                      as insreq,
                installfromdate                                             as installfromdate,
                installfromtime                                             as installfromtime,
                installtodate                                               as installtodate,
                installtotime                                               as installtotime,
                instaxcode                                                  as instaxcode,
                instaxinc                                                   as instaxinc,
                instaxrate                                                  as instaxrate,
                insurancerequired                                           as insurancerequired,
                insval                                                      as insval,
                invrcode                                                    as invrcode,
                isparent                                                    as isparent,
                itemrevision                                                as itemrevision,
                lastcalc                                                    as lastcalc,
                lastchg                                                     as lastchg,
                lastinv                                                     as lastinv,
                lastohr_1                                                   as lastohr_1,
                lastohr_2                                                   as lastohr_2,
                lastohr_3                                                   as lastohr_3,
                lastohr_4                                                   as lastohr_4,
                lchgby                                                      as lchgby,
                loadfromdate                                                as loadfromdate,
                loadfromtime                                                as loadfromtime,
                loadtodate                                                  as loadtodate,
                loadtotime                                                  as loadtotime,
                memo                                                        as memo,
                nodays                                                      as nodays,
                ordby                                                       as ordby,
                ordbyemail                                                  as ordbyemail,
                ordbytel                                                    as ordbytel,
                parcontno                                                   as parcontno,
                pencilled                                                   as pencilled,
                pflag                                                       as pflag,
                pfperc                                                      as pfperc,
                poacct                                                      as poacct,
                poourref                                                    as poourref,
                postjc                                                      as postjc,
                postsl                                                      as postsl,
                poxhacct                                                    as poxhacct,
                prepfromdate                                                as prepfromdate,
                prepfromtime                                                as prepfromtime,
                preptodate                                                  as preptodate,
                preptotime                                                  as preptotime,
                primaryvat                                                  as primaryvat,
                proforma                                                    as proforma,
                proformacn                                                  as proformacn,
                purord                                                      as purord,
                qstatus                                                     as qstatus,
                ratetype                                                    as ratetype,
                recid                                                       as recid,
                recorder                                                    as recorder,
                refundamt                                                   as refundamt,
                retfix                                                      as retfix,
                roworder                                                    as roworder,
                rv                                                          as rv,
                scflag                                                      as scflag,
                scvalue                                                     as scvalue,
                servdue                                                     as servdue,
                setdays                                                     as setdays,
                setdisc                                                     as setdisc,
                showdaybook                                                 as showdaybook,
                sid                                                         as sid,
                source                                                      as source,
                spcode                                                      as spcode,
                status                                                      as status,
                taxareacode                                                 as taxareacode,
                telno                                                       as telno,
                theirref                                                    as theirref,
                tocont                                                      as tocont,
                total                                                       as total,
                unloadfromdate                                              as unloadfromdate,
                unloadfromtime                                              as unloadfromtime,
                unloadtodate                                                as unloadtodate,
                unloadtotime                                                as unloadtotime,
                usecontadd                                                  as usecontadd,
                vat                                                         as vat,
                weight                                                      as weight,
                wherefrom                                                   as wherefrom,
                _fivetran_deleted                                           as _fivetran_deleted,
                _fivetran_synced                                            as _fivetran_synced,
      
            from staging

        )

        select * from final