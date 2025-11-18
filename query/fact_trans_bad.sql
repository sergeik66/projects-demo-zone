WITH wcpayrollbase AS (
        SELECT a.code, classcode, classuffix, state, SUM(premium) AS prem, SUM(exposure) AS exposure
        , ROW_NUMBER() OVER (PARTITION BY a.code ORDER BY SUM(exposure) DESC, SUM(premium) DESC, state ASC) AS rn
        FROM (
          SELECT code, classcode, classuffix, state, premium, exposure, primeseq, seqcode FROM wcpayrol 
          UNION ALL 
          SELECT code, classcode, classuffix, state, premium, exposure, primeseq, seqcode FROM archive_wcpayrol
        ) a
        INNER JOIN wcinfo b ON lower(trim(a.code)) = lower(trim(b.code)) AND lower(trim(a.classcode)) = lower(trim(b.govclass))
        WHERE lower(trim(a.primeseq)) = '1' AND lower(trim(a.seqcode)) = '000'
        GROUP BY a.code, classcode, classuffix, state
      )
      , wcprl AS (
        SELECT code, classcode, classuffix, state FROM wcpayrollbase WHERE rn = 1
      )
      , uwbuildingsbase AS (
        SELECT code, class, classsuffix, bpclass_id
        , ROW_NUMBER() OVER (PARTITION BY code ORDER BY premium DESC, tiv DESC) AS rn
        FROM (
          SELECT code, class, classsuffix, bpclass_id, premium, tiv FROM uwbuildings
          UNION ALL
          SELECT code, class, classsuffix, bpclass_id, premium, tiv FROM archive_uwbuildings
        ) b
      )
      , uwb AS (
        SELECT code, class, classsuffix, bpclass_id FROM uwbuildingsbase WHERE rn = 1
      )
      , poltransinfo AS (
        SELECT DISTINCT code, trancnt FROM mgtrans
        UNION
        SELECT DISTINCT code, trancnt FROM mgnotrans
      )
      , fact_policy_transaction_data AS (
        SELECT DISTINCT
          i.code AS policy_num_bus_key
        , i.pobegin AS policy_effec_start_date
        , i.poexpir AS policy_effec_end_date 
        , i.pobegin AS policy_coverage_start_date
        , (CASE WHEN lower(trim(i.stat5)) = 'c' THEN i.pocancel ELSE i.poexpir END) AS policy_coverage_end_date
        , i.lob AS lob_cd
        , i.agency AS agency_cd
        , i.code AS insured_code
        , e.naics AS naics_cd
        , CASE WHEN i.decsbr IS NOT NULL THEN i.decsbr ELSE i.decuw END AS dec_uw_emp_cd
        , i.markettype AS market_type_cd
        , i.productionsrc AS production_src_cd
        , i.carrier AS carrier_cd
        , CASE WHEN b.code IS NOT NULL THEN b.transcode ELSE c.transcode END AS policy_trans_type_cd
        , CASE WHEN b.code IS NOT NULL THEN b.effdate ELSE COALESCE(c.effdate, pa.effdate) END AS policy_trans_effec_date
        , CASE WHEN b.code IS NOT NULL THEN b.transdate ELSE c.enteredon END AS policy_trans_date
        , p.trancnt AS policy_trans_seq_num
        , CASE WHEN b.code IS NOT NULL THEN 1 ELSE 2 END AS src_trans_id_type
        , CASE WHEN b.code IS NOT NULL THEN b.transno ELSE c.id END AS src_trans_id
        , CASE WHEN b.code IS NOT NULL THEN b.writtenon ELSE c.enteredon END AS policy_trans_written_on_date
        , b.prem_amt
        , b.fsa_amt
        , b.paa_amt
        , b.sf3_amt
        , b.sf4_amt
        , b.sf5_amt
        , b.sf6_amt
        , b.sf7_amt
        , b.comm_amt
        , b.non_comm_prem_amt
        , CASE WHEN b.code IS NOT NULL THEN b.state ELSE c.state END AS policy_trans_state
        , CASE 
            WHEN lower(trim(i.lob)) = 'wc' THEN CONCAT(wcprl.classcode, wcprl.state)
            WHEN lower(trim(i.lob)) = 'bp' THEN uwb.class 
            ELSE 'not applicable'
          END AS class_cd_bus_key
        , CASE 
            WHEN lower(trim(i.lob)) = 'wc' THEN wcprl.classuffix
            WHEN lower(trim(i.lob)) = 'bp' THEN uwb.classsuffix 
            ELSE 'not applicable'
          END AS class_suffix_bus_key
        , CASE 
            WHEN lower(trim(i.lob)) = 'wc' THEN '0'
            WHEN lower(trim(i.lob)) = 'bp' THEN CAST(uwb.bpclass_id AS STRING)
            ELSE 'not applicable'
          END AS class_src_id_bus_key,
          pa.code
        FROM poltransinfo p
        INNER JOIN insured i ON lower(trim(p.code)) = lower(trim(i.code))
        LEFT OUTER JOIN (
          SELECT code, trancnt, m.writtenon, m.effdate, m.transcode, m.transdate, m.transno, m.state
          , m.amount AS prem_amt, m.fsamount AS fsa_amt, m.paamount AS paa_amt, m.sf3amount AS sf3_amt
          , m.sf4amount AS sf4_amt, m.sf5amount AS sf5_amt, m.sf6amount AS sf6_amt, m.sf7amount AS sf7_amt
          , m.commisamt AS comm_amt, m.nocommprem AS non_comm_prem_amt 
          FROM mgtrans m
        ) b ON lower(trim(p.code)) = lower(trim(b.code)) AND p.trancnt = b.trancnt
        LEFT OUTER JOIN mgextra e ON lower(trim(p.code)) = lower(trim(e.code))
        LEFT OUTER JOIN mgnotrans c ON lower(trim(p.code)) = lower(trim(c.code)) AND p.trancnt = c.trancnt
        LEFT OUTER JOIN (
          SELECT code, trancnt, MAX(effdate) AS effdate
          FROM polact
          GROUP BY code, trancnt
        ) pa ON lower(trim(pa.code)) = lower(trim(p.code)) AND pa.trancnt = p.trancnt
        LEFT OUTER JOIN wcprl ON lower(trim(wcprl.code)) = lower(trim(i.code))
        LEFT OUTER JOIN uwb ON lower(trim(uwb.code)) = lower(trim(i.code))
        WHERE lower(trim(i.code)) = lower(trim(i.basemgacode)) AND lower(trim(i.agency)) <> 'pafake10'
      )
      SELECT
        COALESCE(dpol.policy_key, -1) AS policy_key, 
        COALESCE(dd_eff_start.date_key, -1) AS policy_effec_start_dt_key,
        COALESCE(dd_eff_end.date_key, -1) AS policy_effec_end_dt_key,
        COALESCE(dd_cov_start.date_key, -1) AS policy_coverage_start_dt_key,
        COALESCE(dd_cov_end.date_key, -1) AS policy_coverage_end_dt_key,
        COALESCE(dlob.lob_key, -1) AS lob_key,
        COALESCE(ddc.dist_chnl_key, -1) AS dist_chnl_key,
        COALESCE(dins.insd_key, -1) AS insd_key,
        COALESCE(dnai.naics_key, -1) AS naics_key,
        COALESCE(demp_dec.emp_key, -1) AS dec_uw_emp_key,
        COALESCE(dmkt.mkt_type_key, -1) AS mkt_type_key,
        COALESCE(dpro.prod_src_key, -1) AS prod_src_key,
        COALESCE(dcar.carrier_key, -1) AS carrier_key,
        COALESCE(dtrans.policy_trans_type_key, -1) AS policy_trans_type_key,
        COALESCE(dd_trans_effec.date_key, -1) AS policy_trans_effec_dt_key,
        COALESCE(dd_trans.date_key, -1) AS policy_trans_dt_key,
        fd.policy_trans_seq_num,
        fd.src_trans_id_type,
        fd.src_trans_id,
        COALESCE(dd_written.date_key, -1) AS policy_trans_written_on_dt_key,
        fd.prem_amt,
        fd.fsa_amt,
        fd.paa_amt,
        fd.sf3_amt,
        fd.sf4_amt,
        fd.sf5_amt,
        fd.sf6_amt,
        fd.sf7_amt,
        fd.comm_amt,
        fd.non_comm_prem_amt,
        fd.policy_trans_state,
        COALESCE(dbc.bus_class_key, -1) AS bus_class_key
        , True as dl_is_current_flag
        ,'1901-01-01' as dl_row_effective_date
        ,'9999-12-31' as dl_row_expiration_date
        ,False as dl_is_deleted_flag
        , case when lower(trim(dlob.lob_cd_bus_key)) in ('wc', 'ca', 'db', 'pl', 'um', 'up', 'cx', 'cp') and lower(trim(dmkt.mkt_type_desc)) = 'e&s' then 0 else 1 end as dq_rule_542
        , case when lower(trim(dcar.carrier_cd_bus_key)) = 'paazgu10' and lower(trim(dmkt.mkt_type_desc)) <> 'e&s' then 0 else 1 end as dq_rule_543
        , case when SUM(fd.prem_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.amount) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_544
        , case when SUM(fd.fsa_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.fsamount) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_545
        , case when SUM(fd.paa_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.paamount) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_546
        , case when SUM(fd.sf3_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.sf3amount) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_547
        , case when SUM(fd.sf4_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.sf4amount) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_548
        , case when SUM(fd.sf5_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.sf5amount) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_549
        , case when SUM(fd.sf6_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.sf6amount) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_550
        , case when SUM(fd.sf7_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.sf7amount) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_551
        , case when SUM(fd.comm_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.commisamt) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_552
        , case when SUM(fd.non_comm_prem_amt) OVER (PARTITION BY lower(trim(fd.code))) = ( 
              select sum(m4dq.nocommprem) from mgtrans m4dq where lower(trim(m4dq.code)) = lower(trim(fd.code)) 
            ) then 1 else 0
          end as dq_rule_553
        , case when lower(trim(dlob.lob_cd_bus_key)) not in ('wc', 'bp') and lower(trim(dbc.bus_class_cd_bus_key)) is not null then 0 else 1 end as dq_rule_554
      FROM fact_policy_transaction_data fd
      LEFT OUTER JOIN dim_policy dpol ON lower(trim(fd.policy_num_bus_key)) = lower(trim(dpol.policy_num_bus_key))
      LEFT OUTER JOIN dim_date dd_eff_start ON CAST(fd.policy_effec_start_date AS DATE) = dd_eff_start.date
      LEFT OUTER JOIN dim_date dd_eff_end ON CAST(fd.policy_effec_end_date AS DATE) = dd_eff_end.date
      LEFT OUTER JOIN dim_date dd_cov_start ON CAST(fd.policy_coverage_start_date AS DATE) = dd_cov_start.date
      LEFT OUTER JOIN dim_date dd_cov_end ON CAST(fd.policy_coverage_end_date AS DATE) = dd_cov_end.date
      LEFT OUTER JOIN dim_lob dlob ON lower(trim(fd.lob_cd)) = lower(trim(dlob.lob_cd_bus_key))
      LEFT OUTER JOIN dim_dist_chnl ddc ON lower(trim(fd.agency_cd)) = lower(trim(ddc.agcy_cd_bus_key))
      LEFT OUTER JOIN dim_insured dins ON lower(trim(fd.insured_code)) = lower(trim(dins.policy_num_bus_key))
      LEFT OUTER JOIN dim_naics dnai ON lower(trim(fd.naics_cd)) = lower(trim(dnai.naics_bus_key))
      LEFT OUTER JOIN dim_employee demp_dec ON lower(trim(fd.dec_uw_emp_cd)) = lower(trim(demp_dec.emp_cd_bus_key))
      LEFT OUTER JOIN dim_mkt_type dmkt ON lower(trim(fd.market_type_cd)) = lower(trim(dmkt.mkt_type_cd_bus_key))
      LEFT OUTER JOIN dim_prod_src dpro ON lower(trim(fd.production_src_cd)) = lower(trim(dpro.prod_src_cd_bus_key))
      LEFT OUTER JOIN dim_carrier dcar ON lower(trim(fd.carrier_cd)) = lower(trim(dcar.carrier_cd_bus_key))
      LEFT OUTER JOIN dim_policy_trans_type dtrans ON lower(trim(fd.policy_trans_type_cd)) = lower(trim(dtrans.policy_trans_type_cd_bus_key))
      LEFT OUTER JOIN dim_date dd_trans_effec ON CAST(fd.policy_trans_effec_date AS DATE) = dd_trans_effec.date
      LEFT OUTER JOIN dim_date dd_trans ON CAST(fd.policy_trans_date AS DATE) = dd_trans.date
      LEFT OUTER JOIN dim_date dd_written ON CAST(fd.policy_trans_written_on_date AS DATE) = dd_written.date
      LEFT OUTER JOIN dim_business_class dbc ON 
          lower(trim(fd.class_cd_bus_key)) = lower(trim(dbc.bus_class_cd_bus_key))
          AND lower(trim(fd.class_suffix_bus_key)) = lower(trim(dbc.bus_class_suffix_bus_key))
          AND lower(trim(fd.class_src_id_bus_key)) = lower(trim(dbc.bus_class_src_id_bus_key))
