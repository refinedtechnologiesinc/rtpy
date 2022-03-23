with staging as (

            select * from {{ source('itrack_fivetran','itrackstock') }}

        ),

        final as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                altperiod_1                                                 as altperiod_1,
                altperiod_10                                                as altperiod_10,
                altperiod_11                                                as altperiod_11,
                altperiod_12                                                as altperiod_12,
                altperiod_13                                                as altperiod_13,
                altperiod_14                                                as altperiod_14,
                altperiod_15                                                as altperiod_15,
                altperiod_16                                                as altperiod_16,
                altperiod_17                                                as altperiod_17,
                altperiod_18                                                as altperiod_18,
                altperiod_19                                                as altperiod_19,
                altperiod_2                                                 as altperiod_2,
                altperiod_20                                                as altperiod_20,
                altperiod_3                                                 as altperiod_3,
                altperiod_4                                                 as altperiod_4,
                altperiod_5                                                 as altperiod_5,
                altperiod_6                                                 as altperiod_6,
                altperiod_7                                                 as altperiod_7,
                altperiod_8                                                 as altperiod_8,
                altperiod_9                                                 as altperiod_9,
                anlcode                                                     as anlcode,
                avcost                                                      as avcost,
                barcode                                                     as barcode,
                bomjob                                                      as bomjob,
                budgetcost                                                  as budgetcost,
                budget_1                                                    as budget_1,
                budget_10                                                   as budget_10,
                budget_11                                                   as budget_11,
                budget_12                                                   as budget_12,
                budget_2                                                    as budget_2,
                budget_3                                                    as budget_3,
                budget_4                                                    as budget_4,
                budget_5                                                    as budget_5,
                budget_6                                                    as budget_6,
                budget_7                                                    as budget_7,
                budget_8                                                    as budget_8,
                budget_9                                                    as budget_9,
                buyprice                                                    as buyprice,
                calcode                                                     as calcode,
                capacity                                                    as capacity,
                captype                                                     as captype,
                cbudget_1                                                   as cbudget_1,
                cbudget_10                                                  as cbudget_10,
                cbudget_11                                                  as cbudget_11,
                cbudget_12                                                  as cbudget_12,
                cbudget_2                                                   as cbudget_2,
                cbudget_3                                                   as cbudget_3,
                cbudget_4                                                   as cbudget_4,
                cbudget_5                                                   as cbudget_5,
                cbudget_6                                                   as cbudget_6,
                cbudget_7                                                   as cbudget_7,
                cbudget_8                                                   as cbudget_8,
                cbudget_9                                                   as cbudget_9,
                ccrec                                                       as ccrec,
                coanlcc                                                     as coanlcc,
                coanlcode                                                   as coanlcode,
                coanldept                                                   as coanldept,
                component                                                   as component,
                cosnlcc                                                     as cosnlcc,
                cosnlcode                                                   as cosnlcode,
                cosnldept                                                   as cosnldept,
                currdepot                                                   as currdepot,
                custdate1                                                   as custdate1,
                custdate10                                                  as custdate10,
                custdate11                                                  as custdate11,
                custdate12                                                  as custdate12,
                custdate13                                                  as custdate13,
                custdate14                                                  as custdate14,
                custdate15                                                  as custdate15,
                custdate16                                                  as custdate16,
                custdate17                                                  as custdate17,
                custdate18                                                  as custdate18,
                custdate19                                                  as custdate19,
                custdate2                                                   as custdate2,
                custdate20                                                  as custdate20,
                custdate21                                                  as custdate21,
                custdate22                                                  as custdate22,
                custdate23                                                  as custdate23,
                custdate24                                                  as custdate24,
                custdate25                                                  as custdate25,
                custdate3                                                   as custdate3,
                custdate4                                                   as custdate4,
                custdate5                                                   as custdate5,
                custdate6                                                   as custdate6,
                custdate7                                                   as custdate7,
                custdate8                                                   as custdate8,
                custdate9                                                   as custdate9,
                custnmbr1                                                   as custnmbr1,
                custnmbr10                                                  as custnmbr10,
                custnmbr11                                                  as custnmbr11,
                custnmbr12                                                  as custnmbr12,
                custnmbr13                                                  as custnmbr13,
                custnmbr14                                                  as custnmbr14,
                custnmbr15                                                  as custnmbr15,
                custnmbr16                                                  as custnmbr16,
                custnmbr17                                                  as custnmbr17,
                custnmbr18                                                  as custnmbr18,
                custnmbr19                                                  as custnmbr19,
                custnmbr2                                                   as custnmbr2,
                custnmbr20                                                  as custnmbr20,
                custnmbr21                                                  as custnmbr21,
                custnmbr22                                                  as custnmbr22,
                custnmbr23                                                  as custnmbr23,
                custnmbr24                                                  as custnmbr24,
                custnmbr25                                                  as custnmbr25,
                custnmbr3                                                   as custnmbr3,
                custnmbr4                                                   as custnmbr4,
                custnmbr5                                                   as custnmbr5,
                custnmbr6                                                   as custnmbr6,
                custnmbr7                                                   as custnmbr7,
                custnmbr8                                                   as custnmbr8,
                custnmbr9                                                   as custnmbr9,
                customer                                                    as customer,
                custtext1                                                   as custtext1,
                custtext10                                                  as custtext10,
                custtext11                                                  as custtext11,
                custtext12                                                  as custtext12,
                custtext13                                                  as custtext13,
                custtext14                                                  as custtext14,
                custtext15                                                  as custtext15,
                custtext16                                                  as custtext16,
                custtext17                                                  as custtext17,
                custtext18                                                  as custtext18,
                custtext19                                                  as custtext19,
                custtext2                                                   as custtext2,
                custtext20                                                  as custtext20,
                custtext21                                                  as custtext21,
                custtext22                                                  as custtext22,
                custtext23                                                  as custtext23,
                custtext24                                                  as custtext24,
                custtext25                                                  as custtext25,
                custtext3                                                   as custtext3,
                custtext4                                                   as custtext4,
                custtext5                                                   as custtext5,
                custtext6                                                   as custtext6,
                custtext7                                                   as custtext7,
                custtext8                                                   as custtext8,
                custtext9                                                   as custtext9,
                datemanufacture                                             as datemanufacture,
                datepurch                                                   as datepurch,
                datesold                                                    as datesold,
                dateused                                                    as dateused,
                daysunav                                                    as daysunav,
                defdep                                                      as defdep,
                defrate                                                     as defrate,
                delcode                                                     as delcode,
                deposit                                                     as deposit,
                deprmeth                                                    as deprmeth,
                depth                                                       as depth,
                desc_1                                                      as desc_1,
                desc_2                                                      as desc_2,
                desc_3                                                      as desc_3,
                ebusiness                                                   as ebusiness,
                extmemo                                                     as extmemo,
                fwdorder                                                    as fwdorder,
                grpcode                                                     as grpcode,
                height                                                      as height,
                icosnlcc                                                    as icosnlcc,
                icosnlcode                                                  as icosnlcode,
                icosnldept                                                  as icosnldept,
                inlcc                                                       as inlcc,
                inlcode                                                     as inlcode,
                inldept                                                     as inldept,
                insvalue                                                    as insvalue,
                invoho                                                      as invoho,
                itemno                                                      as itemno,
                kititem                                                     as kititem,
                lastser_1                                                   as lastser_1,
                lastser_10                                                  as lastser_10,
                lastser_11                                                  as lastser_11,
                lastser_12                                                  as lastser_12,
                lastser_13                                                  as lastser_13,
                lastser_14                                                  as lastser_14,
                lastser_15                                                  as lastser_15,
                lastser_16                                                  as lastser_16,
                lastser_17                                                  as lastser_17,
                lastser_18                                                  as lastser_18,
                lastser_19                                                  as lastser_19,
                lastser_2                                                   as lastser_2,
                lastser_20                                                  as lastser_20,
                lastser_3                                                   as lastser_3,
                lastser_4                                                   as lastser_4,
                lastser_5                                                   as lastser_5,
                lastser_6                                                   as lastser_6,
                lastser_7                                                   as lastser_7,
                lastser_8                                                   as lastser_8,
                lastser_9                                                   as lastser_9,
                lasttestresid                                               as lasttestresid,
                lifecosts                                                   as lifecosts,
                lifedepr                                                    as lifedepr,
                liferev                                                     as liferev,
                lmtdrev                                                     as lmtdrev,
                lytddepr                                                    as lytddepr,
                lytdrev                                                     as lytdrev,
                mastrec                                                     as mastrec,
                maxstk                                                      as maxstk,
                meter                                                       as meter,
                metertotal                                                  as metertotal,
                minstk                                                      as minstk,
                mobileallocateall                                           as mobileallocateall,
                mtdcosts                                                    as mtdcosts,
                mtdrev                                                      as mtdrev,
                nlcc                                                        as nlcc,
                nlcode                                                      as nlcode,
                nldept                                                      as nldept,
                nodisc                                                      as nodisc,
                nonstock                                                    as nonstock,
                nosusp                                                      as nosusp,
                notes                                                       as notes,
                ohservwrk                                                   as ohservwrk,
                ohstatus                                                    as ohstatus,
                onorder                                                     as onorder,
                pack                                                        as pack,
                paload                                                      as paload,
                paloadun                                                    as paloadun,
                patlastser                                                  as patlastser,
                patperiod                                                   as patperiod,
                patpertype                                                  as patpertype,
                pattest                                                     as pattest,
                period_1                                                    as period_1,
                period_10                                                   as period_10,
                period_11                                                   as period_11,
                period_12                                                   as period_12,
                period_13                                                   as period_13,
                period_14                                                   as period_14,
                period_15                                                   as period_15,
                period_16                                                   as period_16,
                period_17                                                   as period_17,
                period_18                                                   as period_18,
                period_19                                                   as period_19,
                period_2                                                    as period_2,
                period_20                                                   as period_20,
                period_3                                                    as period_3,
                period_4                                                    as period_4,
                period_5                                                    as period_5,
                period_6                                                    as period_6,
                period_7                                                    as period_7,
                period_8                                                    as period_8,
                period_9                                                    as period_9,
                perlast_1                                                   as perlast_1,
                perlast_10                                                  as perlast_10,
                perlast_11                                                  as perlast_11,
                perlast_12                                                  as perlast_12,
                perlast_13                                                  as perlast_13,
                perlast_14                                                  as perlast_14,
                perlast_15                                                  as perlast_15,
                perlast_16                                                  as perlast_16,
                perlast_17                                                  as perlast_17,
                perlast_18                                                  as perlast_18,
                perlast_19                                                  as perlast_19,
                perlast_2                                                   as perlast_2,
                perlast_20                                                  as perlast_20,
                perlast_3                                                   as perlast_3,
                perlast_4                                                   as perlast_4,
                perlast_5                                                   as perlast_5,
                perlast_6                                                   as perlast_6,
                perlast_7                                                   as perlast_7,
                perlast_8                                                   as perlast_8,
                perlast_9                                                   as perlast_9,
                pertypealt_1                                                as pertypealt_1,
                pertypealt_10                                               as pertypealt_10,
                pertypealt_11                                               as pertypealt_11,
                pertypealt_12                                               as pertypealt_12,
                pertypealt_13                                               as pertypealt_13,
                pertypealt_14                                               as pertypealt_14,
                pertypealt_15                                               as pertypealt_15,
                pertypealt_16                                               as pertypealt_16,
                pertypealt_17                                               as pertypealt_17,
                pertypealt_18                                               as pertypealt_18,
                pertypealt_19                                               as pertypealt_19,
                pertypealt_2                                                as pertypealt_2,
                pertypealt_20                                               as pertypealt_20,
                pertypealt_3                                                as pertypealt_3,
                pertypealt_4                                                as pertypealt_4,
                pertypealt_5                                                as pertypealt_5,
                pertypealt_6                                                as pertypealt_6,
                pertypealt_7                                                as pertypealt_7,
                pertypealt_8                                                as pertypealt_8,
                pertypealt_9                                                as pertypealt_9,
                pertype_1                                                   as pertype_1,
                pertype_10                                                  as pertype_10,
                pertype_11                                                  as pertype_11,
                pertype_12                                                  as pertype_12,
                pertype_13                                                  as pertype_13,
                pertype_14                                                  as pertype_14,
                pertype_15                                                  as pertype_15,
                pertype_16                                                  as pertype_16,
                pertype_17                                                  as pertype_17,
                pertype_18                                                  as pertype_18,
                pertype_19                                                  as pertype_19,
                pertype_2                                                   as pertype_2,
                pertype_20                                                  as pertype_20,
                pertype_3                                                   as pertype_3,
                pertype_4                                                   as pertype_4,
                pertype_5                                                   as pertype_5,
                pertype_6                                                   as pertype_6,
                pertype_7                                                   as pertype_7,
                pertype_8                                                   as pertype_8,
                pertype_9                                                   as pertype_9,
                pgroup                                                      as pgroup,
                picture                                                     as picture,
                pirecid                                                     as pirecid,
                predelinsp                                                  as predelinsp,
                price                                                       as price,
                pvatcode                                                    as pvatcode,
                qtyalloc                                                    as qtyalloc,
                qtyhire                                                     as qtyhire,
                qtyit                                                       as qtyit,
                qtyrep                                                      as qtyrep,
                qtyser                                                      as qtyser,
                readdate                                                    as readdate,
                recid                                                       as recid,
                recorder                                                    as recorder,
                reorder                                                     as reorder,
                rv                                                          as rv,
                sccode                                                      as sccode,
                sdesc1                                                      as sdesc1,
                sdesc2                                                      as sdesc2,
                sdesc3                                                      as sdesc3,
                serno                                                       as serno,
                serpname1                                                   as serpname1,
                serpname10                                                  as serpname10,
                serpname11                                                  as serpname11,
                serpname12                                                  as serpname12,
                serpname13                                                  as serpname13,
                serpname14                                                  as serpname14,
                serpname15                                                  as serpname15,
                serpname16                                                  as serpname16,
                serpname17                                                  as serpname17,
                serpname18                                                  as serpname18,
                serpname19                                                  as serpname19,
                serpname2                                                   as serpname2,
                serpname20                                                  as serpname20,
                serpname3                                                   as serpname3,
                serpname4                                                   as serpname4,
                serpname5                                                   as serpname5,
                serpname6                                                   as serpname6,
                serpname7                                                   as serpname7,
                serpname8                                                   as serpname8,
                serpname9                                                   as serpname9,
                sid                                                         as sid,
                sold                                                        as sold,
                soldawaitpost                                               as soldawaitpost,
                soldby                                                      as soldby,
                status                                                      as status,
                stdcost                                                     as stdcost,
                sterdept                                                    as sterdept,
                stklevel                                                    as stklevel,
                stknlcc                                                     as stknlcc,
                stknlcode                                                   as stknlcode,
                stknldept                                                   as stknldept,
                subcontractor                                               as subcontractor,
                superseded                                                  as superseded,
                supplier                                                    as supplier,
                swload                                                      as swload,
                swloadun                                                    as swloadun,
                telematicprovider                                           as telematicprovider,
                telematicunitid                                             as telematicunitid,
                template01                                                  as template01,
                template02                                                  as template02,
                template03                                                  as template03,
                template04                                                  as template04,
                template05                                                  as template05,
                template06                                                  as template06,
                template07                                                  as template07,
                template08                                                  as template08,
                template09                                                  as template09,
                template10                                                  as template10,
                template11                                                  as template11,
                template12                                                  as template12,
                template13                                                  as template13,
                template14                                                  as template14,
                template15                                                  as template15,
                template16                                                  as template16,
                template17                                                  as template17,
                template18                                                  as template18,
                template19                                                  as template19,
                template20                                                  as template20,
                template21                                                  as template21,
                template22                                                  as template22,
                testcode01                                                  as testcode01,
                testcode02                                                  as testcode02,
                testcode03                                                  as testcode03,
                testcode04                                                  as testcode04,
                testcode05                                                  as testcode05,
                testcode06                                                  as testcode06,
                testcode07                                                  as testcode07,
                testcode08                                                  as testcode08,
                testcode09                                                  as testcode09,
                testcode10                                                  as testcode10,
                testcode11                                                  as testcode11,
                testcode12                                                  as testcode12,
                testcode13                                                  as testcode13,
                testcode14                                                  as testcode14,
                testcode15                                                  as testcode15,
                testcode16                                                  as testcode16,
                testcode17                                                  as testcode17,
                testcode18                                                  as testcode18,
                testcode19                                                  as testcode19,
                testcode20                                                  as testcode20,
                testcode21                                                  as testcode21,
                testcode22                                                  as testcode22,
                testcode23                                                  as testcode23,
                testcode24                                                  as testcode24,
                tothire                                                     as tothire,
                type                                                        as type,
                ulastser                                                    as ulastser,
                unique                                                      as unique,
                uperiod                                                     as uperiod,
                uperlast                                                    as uperlast,
                upertype                                                    as upertype,
                username                                                    as username,
                usertype                                                    as usertype,
                vatcode                                                     as vatcode,
                weight                                                      as weight,
                width                                                       as width,
                xhcnlcc                                                     as xhcnlcc,
                xhcnlcode                                                   as xhcnlcode,
                xhcnldept                                                   as xhcnldept,
                xhireqty                                                    as xhireqty,
                xhrnlcc                                                     as xhrnlcc,
                xhrnlcode                                                   as xhrnlcode,
                xhrnldept                                                   as xhrnldept,
                ytdcosts                                                    as ytdcosts,
                ytddepr                                                     as ytddepr,
                ytdrev                                                      as ytdrev,
                _fivetran_deleted                                           as _fivetran_deleted,
                _fivetran_synced                                            as _fivetran_synced,
      
            from staging

        )

        select * from final