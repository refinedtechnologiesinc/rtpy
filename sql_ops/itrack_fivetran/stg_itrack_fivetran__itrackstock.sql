with source as (

            select * from {{ source('itrack_fivetran','itrackstock') }}

        ),

        staging as (

            select
                sha1(array_to_string(array_construct_compact(*),','))       as id,
                ALTPERIOD_1                                                 as altperiod_1,
                ALTPERIOD_10                                                as altperiod_10,
                ALTPERIOD_11                                                as altperiod_11,
                ALTPERIOD_12                                                as altperiod_12,
                ALTPERIOD_13                                                as altperiod_13,
                ALTPERIOD_14                                                as altperiod_14,
                ALTPERIOD_15                                                as altperiod_15,
                ALTPERIOD_16                                                as altperiod_16,
                ALTPERIOD_17                                                as altperiod_17,
                ALTPERIOD_18                                                as altperiod_18,
                ALTPERIOD_19                                                as altperiod_19,
                ALTPERIOD_2                                                 as altperiod_2,
                ALTPERIOD_20                                                as altperiod_20,
                ALTPERIOD_3                                                 as altperiod_3,
                ALTPERIOD_4                                                 as altperiod_4,
                ALTPERIOD_5                                                 as altperiod_5,
                ALTPERIOD_6                                                 as altperiod_6,
                ALTPERIOD_7                                                 as altperiod_7,
                ALTPERIOD_8                                                 as altperiod_8,
                ALTPERIOD_9                                                 as altperiod_9,
                ANLCODE                                                     as anlcode,
                AVCOST                                                      as avcost,
                BARCODE                                                     as barcode,
                BOMJOB                                                      as bomjob,
                BUDGETCOST                                                  as budgetcost,
                BUDGET_1                                                    as budget_1,
                BUDGET_10                                                   as budget_10,
                BUDGET_11                                                   as budget_11,
                BUDGET_12                                                   as budget_12,
                BUDGET_2                                                    as budget_2,
                BUDGET_3                                                    as budget_3,
                BUDGET_4                                                    as budget_4,
                BUDGET_5                                                    as budget_5,
                BUDGET_6                                                    as budget_6,
                BUDGET_7                                                    as budget_7,
                BUDGET_8                                                    as budget_8,
                BUDGET_9                                                    as budget_9,
                BUYPRICE                                                    as buyprice,
                CALCODE                                                     as calcode,
                CAPACITY                                                    as capacity,
                CAPTYPE                                                     as captype,
                CBUDGET_1                                                   as cbudget_1,
                CBUDGET_10                                                  as cbudget_10,
                CBUDGET_11                                                  as cbudget_11,
                CBUDGET_12                                                  as cbudget_12,
                CBUDGET_2                                                   as cbudget_2,
                CBUDGET_3                                                   as cbudget_3,
                CBUDGET_4                                                   as cbudget_4,
                CBUDGET_5                                                   as cbudget_5,
                CBUDGET_6                                                   as cbudget_6,
                CBUDGET_7                                                   as cbudget_7,
                CBUDGET_8                                                   as cbudget_8,
                CBUDGET_9                                                   as cbudget_9,
                CCREC                                                       as ccrec,
                COANLCC                                                     as coanlcc,
                COANLCODE                                                   as coanlcode,
                COANLDEPT                                                   as coanldept,
                COMPONENT                                                   as component,
                COSNLCC                                                     as cosnlcc,
                COSNLCODE                                                   as cosnlcode,
                COSNLDEPT                                                   as cosnldept,
                CURRDEPOT                                                   as currdepot,
                CUSTDATE1                                                   as custdate1,
                CUSTDATE10                                                  as custdate10,
                CUSTDATE11                                                  as custdate11,
                CUSTDATE12                                                  as custdate12,
                CUSTDATE13                                                  as custdate13,
                CUSTDATE14                                                  as custdate14,
                CUSTDATE15                                                  as custdate15,
                CUSTDATE16                                                  as custdate16,
                CUSTDATE17                                                  as custdate17,
                CUSTDATE18                                                  as custdate18,
                CUSTDATE19                                                  as custdate19,
                CUSTDATE2                                                   as custdate2,
                CUSTDATE20                                                  as custdate20,
                CUSTDATE21                                                  as custdate21,
                CUSTDATE22                                                  as custdate22,
                CUSTDATE23                                                  as custdate23,
                CUSTDATE24                                                  as custdate24,
                CUSTDATE25                                                  as custdate25,
                CUSTDATE3                                                   as custdate3,
                CUSTDATE4                                                   as custdate4,
                CUSTDATE5                                                   as custdate5,
                CUSTDATE6                                                   as custdate6,
                CUSTDATE7                                                   as custdate7,
                CUSTDATE8                                                   as custdate8,
                CUSTDATE9                                                   as custdate9,
                CUSTNMBR1                                                   as custnmbr1,
                CUSTNMBR10                                                  as custnmbr10,
                CUSTNMBR11                                                  as custnmbr11,
                CUSTNMBR12                                                  as custnmbr12,
                CUSTNMBR13                                                  as custnmbr13,
                CUSTNMBR14                                                  as custnmbr14,
                CUSTNMBR15                                                  as custnmbr15,
                CUSTNMBR16                                                  as custnmbr16,
                CUSTNMBR17                                                  as custnmbr17,
                CUSTNMBR18                                                  as custnmbr18,
                CUSTNMBR19                                                  as custnmbr19,
                CUSTNMBR2                                                   as custnmbr2,
                CUSTNMBR20                                                  as custnmbr20,
                CUSTNMBR21                                                  as custnmbr21,
                CUSTNMBR22                                                  as custnmbr22,
                CUSTNMBR23                                                  as custnmbr23,
                CUSTNMBR24                                                  as custnmbr24,
                CUSTNMBR25                                                  as custnmbr25,
                CUSTNMBR3                                                   as custnmbr3,
                CUSTNMBR4                                                   as custnmbr4,
                CUSTNMBR5                                                   as custnmbr5,
                CUSTNMBR6                                                   as custnmbr6,
                CUSTNMBR7                                                   as custnmbr7,
                CUSTNMBR8                                                   as custnmbr8,
                CUSTNMBR9                                                   as custnmbr9,
                CUSTOMER                                                    as customer,
                CUSTTEXT1                                                   as custtext1,
                CUSTTEXT10                                                  as custtext10,
                CUSTTEXT11                                                  as custtext11,
                CUSTTEXT12                                                  as custtext12,
                CUSTTEXT13                                                  as custtext13,
                CUSTTEXT14                                                  as custtext14,
                CUSTTEXT15                                                  as custtext15,
                CUSTTEXT16                                                  as custtext16,
                CUSTTEXT17                                                  as custtext17,
                CUSTTEXT18                                                  as custtext18,
                CUSTTEXT19                                                  as custtext19,
                CUSTTEXT2                                                   as custtext2,
                CUSTTEXT20                                                  as custtext20,
                CUSTTEXT21                                                  as custtext21,
                CUSTTEXT22                                                  as custtext22,
                CUSTTEXT23                                                  as custtext23,
                CUSTTEXT24                                                  as custtext24,
                CUSTTEXT25                                                  as custtext25,
                CUSTTEXT3                                                   as custtext3,
                CUSTTEXT4                                                   as custtext4,
                CUSTTEXT5                                                   as custtext5,
                CUSTTEXT6                                                   as custtext6,
                CUSTTEXT7                                                   as custtext7,
                CUSTTEXT8                                                   as custtext8,
                CUSTTEXT9                                                   as custtext9,
                DATEMANUFACTURE                                             as datemanufacture,
                DATEPURCH                                                   as datepurch,
                DATESOLD                                                    as datesold,
                DATEUSED                                                    as dateused,
                DAYSUNAV                                                    as daysunav,
                DEFDEP                                                      as defdep,
                DEFRATE                                                     as defrate,
                DELCODE                                                     as delcode,
                DEPOSIT                                                     as deposit,
                DEPRMETH                                                    as deprmeth,
                DEPTH                                                       as depth,
                DESC_1                                                      as desc_1,
                DESC_2                                                      as desc_2,
                DESC_3                                                      as desc_3,
                EBUSINESS                                                   as ebusiness,
                EXTMEMO                                                     as extmemo,
                FWDORDER                                                    as fwdorder,
                GRPCODE                                                     as grpcode,
                HEIGHT                                                      as height,
                ICOSNLCC                                                    as icosnlcc,
                ICOSNLCODE                                                  as icosnlcode,
                ICOSNLDEPT                                                  as icosnldept,
                INLCC                                                       as inlcc,
                INLCODE                                                     as inlcode,
                INLDEPT                                                     as inldept,
                INSVALUE                                                    as insvalue,
                INVOHO                                                      as invoho,
                ITEMNO                                                      as itemno,
                KITITEM                                                     as kititem,
                LASTSER_1                                                   as lastser_1,
                LASTSER_10                                                  as lastser_10,
                LASTSER_11                                                  as lastser_11,
                LASTSER_12                                                  as lastser_12,
                LASTSER_13                                                  as lastser_13,
                LASTSER_14                                                  as lastser_14,
                LASTSER_15                                                  as lastser_15,
                LASTSER_16                                                  as lastser_16,
                LASTSER_17                                                  as lastser_17,
                LASTSER_18                                                  as lastser_18,
                LASTSER_19                                                  as lastser_19,
                LASTSER_2                                                   as lastser_2,
                LASTSER_20                                                  as lastser_20,
                LASTSER_3                                                   as lastser_3,
                LASTSER_4                                                   as lastser_4,
                LASTSER_5                                                   as lastser_5,
                LASTSER_6                                                   as lastser_6,
                LASTSER_7                                                   as lastser_7,
                LASTSER_8                                                   as lastser_8,
                LASTSER_9                                                   as lastser_9,
                LASTTESTRESID                                               as lasttestresid,
                LIFECOSTS                                                   as lifecosts,
                LIFEDEPR                                                    as lifedepr,
                LIFEREV                                                     as liferev,
                LMTDREV                                                     as lmtdrev,
                LYTDDEPR                                                    as lytddepr,
                LYTDREV                                                     as lytdrev,
                MASTREC                                                     as mastrec,
                MAXSTK                                                      as maxstk,
                METER                                                       as meter,
                METERTOTAL                                                  as metertotal,
                MINSTK                                                      as minstk,
                MOBILEALLOCATEALL                                           as mobileallocateall,
                MTDCOSTS                                                    as mtdcosts,
                MTDREV                                                      as mtdrev,
                NLCC                                                        as nlcc,
                NLCODE                                                      as nlcode,
                NLDEPT                                                      as nldept,
                NODISC                                                      as nodisc,
                NONSTOCK                                                    as nonstock,
                NOSUSP                                                      as nosusp,
                NOTES                                                       as notes,
                OHSERVWRK                                                   as ohservwrk,
                OHSTATUS                                                    as ohstatus,
                ONORDER                                                     as onorder,
                PACK                                                        as pack,
                PALOAD                                                      as paload,
                PALOADUN                                                    as paloadun,
                PATLASTSER                                                  as patlastser,
                PATPERIOD                                                   as patperiod,
                PATPERTYPE                                                  as patpertype,
                PATTEST                                                     as pattest,
                PERIOD_1                                                    as period_1,
                PERIOD_10                                                   as period_10,
                PERIOD_11                                                   as period_11,
                PERIOD_12                                                   as period_12,
                PERIOD_13                                                   as period_13,
                PERIOD_14                                                   as period_14,
                PERIOD_15                                                   as period_15,
                PERIOD_16                                                   as period_16,
                PERIOD_17                                                   as period_17,
                PERIOD_18                                                   as period_18,
                PERIOD_19                                                   as period_19,
                PERIOD_2                                                    as period_2,
                PERIOD_20                                                   as period_20,
                PERIOD_3                                                    as period_3,
                PERIOD_4                                                    as period_4,
                PERIOD_5                                                    as period_5,
                PERIOD_6                                                    as period_6,
                PERIOD_7                                                    as period_7,
                PERIOD_8                                                    as period_8,
                PERIOD_9                                                    as period_9,
                PERLAST_1                                                   as perlast_1,
                PERLAST_10                                                  as perlast_10,
                PERLAST_11                                                  as perlast_11,
                PERLAST_12                                                  as perlast_12,
                PERLAST_13                                                  as perlast_13,
                PERLAST_14                                                  as perlast_14,
                PERLAST_15                                                  as perlast_15,
                PERLAST_16                                                  as perlast_16,
                PERLAST_17                                                  as perlast_17,
                PERLAST_18                                                  as perlast_18,
                PERLAST_19                                                  as perlast_19,
                PERLAST_2                                                   as perlast_2,
                PERLAST_20                                                  as perlast_20,
                PERLAST_3                                                   as perlast_3,
                PERLAST_4                                                   as perlast_4,
                PERLAST_5                                                   as perlast_5,
                PERLAST_6                                                   as perlast_6,
                PERLAST_7                                                   as perlast_7,
                PERLAST_8                                                   as perlast_8,
                PERLAST_9                                                   as perlast_9,
                PERTYPEALT_1                                                as pertypealt_1,
                PERTYPEALT_10                                               as pertypealt_10,
                PERTYPEALT_11                                               as pertypealt_11,
                PERTYPEALT_12                                               as pertypealt_12,
                PERTYPEALT_13                                               as pertypealt_13,
                PERTYPEALT_14                                               as pertypealt_14,
                PERTYPEALT_15                                               as pertypealt_15,
                PERTYPEALT_16                                               as pertypealt_16,
                PERTYPEALT_17                                               as pertypealt_17,
                PERTYPEALT_18                                               as pertypealt_18,
                PERTYPEALT_19                                               as pertypealt_19,
                PERTYPEALT_2                                                as pertypealt_2,
                PERTYPEALT_20                                               as pertypealt_20,
                PERTYPEALT_3                                                as pertypealt_3,
                PERTYPEALT_4                                                as pertypealt_4,
                PERTYPEALT_5                                                as pertypealt_5,
                PERTYPEALT_6                                                as pertypealt_6,
                PERTYPEALT_7                                                as pertypealt_7,
                PERTYPEALT_8                                                as pertypealt_8,
                PERTYPEALT_9                                                as pertypealt_9,
                PERTYPE_1                                                   as pertype_1,
                PERTYPE_10                                                  as pertype_10,
                PERTYPE_11                                                  as pertype_11,
                PERTYPE_12                                                  as pertype_12,
                PERTYPE_13                                                  as pertype_13,
                PERTYPE_14                                                  as pertype_14,
                PERTYPE_15                                                  as pertype_15,
                PERTYPE_16                                                  as pertype_16,
                PERTYPE_17                                                  as pertype_17,
                PERTYPE_18                                                  as pertype_18,
                PERTYPE_19                                                  as pertype_19,
                PERTYPE_2                                                   as pertype_2,
                PERTYPE_20                                                  as pertype_20,
                PERTYPE_3                                                   as pertype_3,
                PERTYPE_4                                                   as pertype_4,
                PERTYPE_5                                                   as pertype_5,
                PERTYPE_6                                                   as pertype_6,
                PERTYPE_7                                                   as pertype_7,
                PERTYPE_8                                                   as pertype_8,
                PERTYPE_9                                                   as pertype_9,
                PGROUP                                                      as pgroup,
                PICTURE                                                     as picture,
                PIRECID                                                     as pirecid,
                PREDELINSP                                                  as predelinsp,
                PRICE                                                       as price,
                PVATCODE                                                    as pvatcode,
                QTYALLOC                                                    as qtyalloc,
                QTYHIRE                                                     as qtyhire,
                QTYIT                                                       as qtyit,
                QTYREP                                                      as qtyrep,
                QTYSER                                                      as qtyser,
                READDATE                                                    as readdate,
                RECID                                                       as recid,
                RECORDER                                                    as recorder,
                REORDER                                                     as reorder,
                RV                                                          as rv,
                SCCODE                                                      as sccode,
                SDESC1                                                      as sdesc1,
                SDESC2                                                      as sdesc2,
                SDESC3                                                      as sdesc3,
                SERNO                                                       as serno,
                SERPNAME1                                                   as serpname1,
                SERPNAME10                                                  as serpname10,
                SERPNAME11                                                  as serpname11,
                SERPNAME12                                                  as serpname12,
                SERPNAME13                                                  as serpname13,
                SERPNAME14                                                  as serpname14,
                SERPNAME15                                                  as serpname15,
                SERPNAME16                                                  as serpname16,
                SERPNAME17                                                  as serpname17,
                SERPNAME18                                                  as serpname18,
                SERPNAME19                                                  as serpname19,
                SERPNAME2                                                   as serpname2,
                SERPNAME20                                                  as serpname20,
                SERPNAME3                                                   as serpname3,
                SERPNAME4                                                   as serpname4,
                SERPNAME5                                                   as serpname5,
                SERPNAME6                                                   as serpname6,
                SERPNAME7                                                   as serpname7,
                SERPNAME8                                                   as serpname8,
                SERPNAME9                                                   as serpname9,
                SID                                                         as sid,
                SOLD                                                        as sold,
                SOLDAWAITPOST                                               as soldawaitpost,
                SOLDBY                                                      as soldby,
                STATUS                                                      as status,
                STDCOST                                                     as stdcost,
                STERDEPT                                                    as sterdept,
                STKLEVEL                                                    as stklevel,
                STKNLCC                                                     as stknlcc,
                STKNLCODE                                                   as stknlcode,
                STKNLDEPT                                                   as stknldept,
                SUBCONTRACTOR                                               as subcontractor,
                SUPERSEDED                                                  as superseded,
                SUPPLIER                                                    as supplier,
                SWLOAD                                                      as swload,
                SWLOADUN                                                    as swloadun,
                TELEMATICPROVIDER                                           as telematicprovider,
                TELEMATICUNITID                                             as telematicunitid,
                TEMPLATE01                                                  as template01,
                TEMPLATE02                                                  as template02,
                TEMPLATE03                                                  as template03,
                TEMPLATE04                                                  as template04,
                TEMPLATE05                                                  as template05,
                TEMPLATE06                                                  as template06,
                TEMPLATE07                                                  as template07,
                TEMPLATE08                                                  as template08,
                TEMPLATE09                                                  as template09,
                TEMPLATE10                                                  as template10,
                TEMPLATE11                                                  as template11,
                TEMPLATE12                                                  as template12,
                TEMPLATE13                                                  as template13,
                TEMPLATE14                                                  as template14,
                TEMPLATE15                                                  as template15,
                TEMPLATE16                                                  as template16,
                TEMPLATE17                                                  as template17,
                TEMPLATE18                                                  as template18,
                TEMPLATE19                                                  as template19,
                TEMPLATE20                                                  as template20,
                TEMPLATE21                                                  as template21,
                TEMPLATE22                                                  as template22,
                TESTCODE01                                                  as testcode01,
                TESTCODE02                                                  as testcode02,
                TESTCODE03                                                  as testcode03,
                TESTCODE04                                                  as testcode04,
                TESTCODE05                                                  as testcode05,
                TESTCODE06                                                  as testcode06,
                TESTCODE07                                                  as testcode07,
                TESTCODE08                                                  as testcode08,
                TESTCODE09                                                  as testcode09,
                TESTCODE10                                                  as testcode10,
                TESTCODE11                                                  as testcode11,
                TESTCODE12                                                  as testcode12,
                TESTCODE13                                                  as testcode13,
                TESTCODE14                                                  as testcode14,
                TESTCODE15                                                  as testcode15,
                TESTCODE16                                                  as testcode16,
                TESTCODE17                                                  as testcode17,
                TESTCODE18                                                  as testcode18,
                TESTCODE19                                                  as testcode19,
                TESTCODE20                                                  as testcode20,
                TESTCODE21                                                  as testcode21,
                TESTCODE22                                                  as testcode22,
                TESTCODE23                                                  as testcode23,
                TESTCODE24                                                  as testcode24,
                TOTHIRE                                                     as tothire,
                TYPE                                                        as type,
                ULASTSER                                                    as ulastser,
                UNIQUE                                                      as unique,
                UPERIOD                                                     as uperiod,
                UPERLAST                                                    as uperlast,
                UPERTYPE                                                    as upertype,
                USERNAME                                                    as username,
                USERTYPE                                                    as usertype,
                VATCODE                                                     as vatcode,
                WEIGHT                                                      as weight,
                WIDTH                                                       as width,
                XHCNLCC                                                     as xhcnlcc,
                XHCNLCODE                                                   as xhcnlcode,
                XHCNLDEPT                                                   as xhcnldept,
                XHIREQTY                                                    as xhireqty,
                XHRNLCC                                                     as xhrnlcc,
                XHRNLCODE                                                   as xhrnlcode,
                XHRNLDEPT                                                   as xhrnldept,
                YTDCOSTS                                                    as ytdcosts,
                YTDDEPR                                                     as ytddepr,
                YTDREV                                                      as ytdrev,
                _FIVETRAN_DELETED                                           as _fivetran_deleted,
                _FIVETRAN_SYNCED                                            as _fivetran_synced,
      
            from source

        )

        select * from staging