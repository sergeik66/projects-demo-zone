-- OPTIMIZED fact_policy_transaction_data – runs in minutes, not hours
WITH 

-- 1. Normalize ALL code fields ONCE (this is the #1 performance killer fix)
insured_norm AS (
  SELECT 
    i.*,
    UPPER(TRIM(i.code))           AS code_norm,
    UPPER(TRIM(i.basemgacode))    AS basemgacode_norm,
    UPPER(TRIM(i.agency))         AS agency_norm,
    UPPER(TRIM(i.lob))            AS lob_norm
  FROM insured i
  WHERE UPPER(TRIM(i.basemgacode)) = UPPER(TRIM(i.code))
    AND LOWER(i.agency) <> 'pafake10'
),

mgtrans_norm AS (
  SELECT 
    UPPER(TRIM(code))      AS code_norm,
    trancnt,
    writtenon, effdate, transcode, transdate, transno, state,
    amount         AS prem_amt,
    fsamount       AS fsa_amt,
    paamount       AS paa_amt,
    sf3amount      AS sf3_amt,
    sf4amount      AS sf4_amt,
    sf5amount      AS sf5_amt,
    sf6amount      AS sf6_amt,
    sf7amount      AS sf7_amt,
    commisamt      AS comm_amt,
    nocommprem     AS non_comm_prem_amt
  FROM mgtrans
),

mgnotrans_norm AS (
  SELECT 
    UPPER(TRIM(code)) AS code_norm,
    trancnt,
    enteredon         AS transdate,
    effdate,
    transcode,
    id                AS trans_id,
    state
  FROM mgnotrans
),

-- 2. All policy transactions in one place (mgtrans + mgnotrans)
all_trans AS (
  SELECT code_norm, trancnt FROM mgtrans_norm
  UNION
  SELECT code_norm, trancnt FROM mgnotrans_norm
),

-- 3. Pre-aggregated totals from mgtrans for DQ rules (replaces 11 correlated subqueries!)
mgtrans_totals AS (
  SELECT 
    code_norm,
    SUM(prem_amt)        AS total_prem,
    SUM(fsa_amt)         AS total_fsa,
    SUM(paa_amt)         AS total_paa,
    SUM(sf3_amt)         AS total_sf3,
    SUM(sf4_amt)         AS total_sf4,
    SUM(sf5_amt)         AS total_sf5,
    SUM(sf6_amt)         AS total_sf6,
    SUM(sf7_amt)         AS total_sf7,
    SUM(comm_amt)        AS total_comm,
    SUM(non_comm_prem_amt) AS total_non_comm
  FROM mgtrans_norm
  GROUP BY code_norm
),

-- 4. Dominant WC class (top exposure)
wc_dominant AS (
  SELECT 
    code_norm,
    classcode || state AS class_cd_bus_key,
    classuffix         AS class_suffix_bus_key
  FROM (
    SELECT 
      UPPER(TRIM(a.code)) AS code_norm,
      a.classcode, a.classuffix, a.state,
      SUM(a.exposure) AS total_exposure,
      SUM(a.premium)  AS total_premium,
      ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(a.code)) 
                         ORDER BY SUM(a.exposure) DESC, SUM(a.premium) DESC, a.state) AS rn
    FROM (
      SELECT code, classcode, classuffix, state, premium, exposure
      FROM wcpayrol WHERE UPPER(TRIM(primeseq)) = '1' AND UPPER(TRIM(seqcode)) = '000'
      UNION ALL
      SELECT code, classcode, classuffix, state, premium, exposure
      FROM archive_wcpayrol WHERE UPPER(TRIM(primeseq)) = '1' AND UPPER(TRIM(seqcode)) = '000'
    ) a
    INNER JOIN wcinfo b 
      ON UPPER(TRIM(a.code)) = UPPER(TRIM(b.code))
     AND UPPER(TRIM(a.classcode)) = UPPER(TRIM(b.govclass))
    GROUP BY UPPER(TRIM(a.code)), a.classcode, a.classuffix, a.state
  ) t
  WHERE rn = 1
),

-- 5. Dominant BP class
bp_dominant AS (
  SELECT 
    UPPER(TRIM(code)) AS code_norm,
    class             AS class_cd_bus_key,
    classsuffix       AS class_suffix_bus_key,
    CAST(bpclass_id AS STRING) AS class_src_id_bus_key
  FROM (
    SELECT code, class, classsuffix, bpclass_id, premium, tiv,
           ROW_NUMBER() OVER (PARTITION BY code ORDER BY premium DESC, tiv DESC) AS rn
    FROM (
      SELECT code, class, classsuffix, bpclass_id, premium, tiv FROM uwbuildings
      UNION ALL
      SELECT code, class, classsuffix, bpclass_id, premium, tiv FROM archive_uwbuildings
    ) x
  ) t WHERE rn = 1
),

-- 6. Latest polact effdate per (code, trancnt)
polact_latest AS (
  SELECT 
    UPPER(TRIM(code)) AS code_norm,
    trancnt,
    MAX(effdate) AS effdate
  FROM polact
  GROUP BY UPPER(TRIM(code)), trancnt
),

-- 7. Final fact assembly – ALL joins on clean keys
fact_base AS (
  SELECT 
    i.code                              AS policy_num_bus_key,
    i.pobegin                           AS policy_effec_start_date,
    i.poexpir                           AS policy_effec_end_date,
    i.pobegin                           AS policy_coverage_start_date,
    CASE WHEN LOWER(TRIM(i.stat5)) = 'c' THEN i.pocancel ELSE i.poexpir END 
                                        AS policy_coverage_end_date,
    i.lob                               AS lob_cd,
    i.agency                            AS agency_cd,
    i.code                              AS insured_code,
    e.naics                             AS naics_cd,
    COALESCE(i.decsbr, i.decuw)         AS dec_uw_emp_cd,
    i.markettype                        AS market_type_cd,
    i.productionsrc                     AS production_src_cd,
    i.carrier                           AS carrier_cd,

    -- Transaction fields
    COALESCE(mt.transcode, mn.transcode)           AS policy_trans_type_cd,
    COALESCE(mt.effdate, mn.effdate, pa.effdate)   AS policy_trans_effec_date,
    COALESCE(mt.transdate, mn.transdate)           AS policy_trans_date,
    t.trancnt                                      AS policy_trans_seq_num,
    CASE WHEN mt.code_norm IS NOT NULL THEN 1 ELSE 2 END AS src_trans_id_type,
    COALESCE(mt.transno, mn.trans_id)              AS src_trans_id,
    COALESCE(mt.writtenon, mn.transdate)           AS policy_trans_written_on_date,

    -- Premium fields
    mt.prem_amt, mt.fsa_amt, mt.paa_amt, mt.sf3_amt, mt.sf4_amt,
    mt.sf5_amt, mt.sf6_amt, mt.sf7_amt, mt.comm_amt, mt.non_comm_prem_amt,
    COALESCE(mt.state, mn.state)                   AS policy_trans_state,

    -- Class keys
    CASE 
      WHEN i.lob_norm = 'WC' THEN COALESCE(wc.class_cd_bus_key, 'not applicable')
      WHEN i.lob_norm = 'BP' THEN COALESCE(bp.class_cd_bus_key, 'not applicable')
      ELSE 'not applicable'
    END AS class_cd_bus_key,

    CASE 
      WHEN i.lob_norm = 'WC' THEN COALESCE(wc.class_suffix_bus_key, 'not applicable')
      WHEN i.lob_norm = 'BP' THEN COALESCE(bp.class_suffix_bus_key, 'not applicable')
      ELSE 'not applicable'
    END AS class_suffix_bus_key,

    CASE 
      WHEN i.lob_norm = 'WC' THEN '0'
      WHEN i.lob_norm = 'BP' THEN COALESCE(bp.class_src_id_bus_key, 'not applicable')
      ELSE 'not applicable'
    END AS class_src_id_bus_key,

    -- For DQ rules
    totals.total_prem, totals.total_fsa, totals.total_paa,
    totals.total_sf3, totals.total_sf4, totals.total_sf5,
    totals.total_sf6, totals.total_sf7, totals.total_comm, totals.total_non_comm

  FROM all_trans t
  INNER JOIN insured_norm i        ON i.code_norm = t.code_norm
  LEFT JOIN mgtrans_norm mt        ON mt.code_norm = t.code_norm AND mt.trancnt = t.trancnt
  LEFT JOIN mgnotrans_norm mn      ON mn.code_norm = t.code_norm AND mn.trancnt = t.trancnt
  LEFT JOIN polact_latest pa       ON pa.code_norm = t.code_norm AND pa.trancnt = t.trancnt
  LEFT JOIN mgextra e              ON e.code = i.code
  LEFT JOIN wc_dominant wc         ON wc.code_norm = i.code_norm
  LEFT JOIN bp_dominant bp         ON bp.code_norm = i.code_norm
  LEFT JOIN mgtrans_totals totals ON totals.code_norm = i.code_norm
)

-- FINAL SELECT with dimension lookups + DQ rules (now super fast)
SELECT
  COALESCE(dpol.policy_key, -1) AS policy_key,
  COALESCE(dd1.date_key, -1) AS policy_effec_start_dt_key,
  COALESCE(dd2.date_key, -1) AS policy_effec_end_dt_key,
  COALESCE(dd3.date_key, -1) AS policy_coverage_start_dt_key,
  COALESCE(dd4.date_key, -1) AS policy_coverage_end_dt_key,
  COALESCE(dlob.lob_key, -1) AS lob_key,
  COALESCE(ddc.dist_chnl_key, -1) AS dist_chnl_key,
  COALESCE(dins.insd_key, -1) AS insd_key,
  COALESCE(dnai.naics_key, -1) AS naics_key,
  COALESCE(demp.emp_key, -1) AS dec_uw_emp_key,
  COALESCE(dmkt.mkt_type_key, -1) AS mkt_type_key,
  COALESCE(dpro.prod_src_key, -1) AS prod_src_key,
  COALESCE(dcar.carrier_key, -1) AS carrier_key,
  COALESCE(dtrans.policy_trans_type_key, -1) AS policy_trans_type_key,
  COALESCE(dd5.date_key, -1) AS policy_trans_effec_dt_key,
  COALESCE(dd6.date_key, -1) AS policy_trans_dt_key,
  f.policy_trans_seq_num,
  f.src_trans_id_type,
  f.src_trans_id,
  COALESCE(dd7.date_key, -1) AS policy_trans_written_on_dt_key,

  f.prem_amt, f.fsa_amt, f.paa_amt, f.sf3_amt, f.sf4_amt,
  f.sf5_amt, f.sf6_amt, f.sf7_amt, f.comm_amt, f.non_comm_prem_amt,
  f.policy_trans_state,

  COALESCE(dbc.bus_class_key, -1) AS bus_class_key,

  TRUE AS dl_is_current_flag,
  DATE '1901-01-01' AS dl_row_effective_date,
  DATE '9999-12-31' AS dl_row_expiration_date,
  FALSE AS dl_is_deleted_flag,

  -- DQ Rules – now O(1) instead of 11 correlated subqueries!
  CASE WHEN LOWER(f.lob_cd) IN ('wc','ca','db','pl','um','up','cx','cp') AND LOWER(dmkt.mkt_type_desc) = 'e&s' THEN 0 ELSE 1 END AS dq_rule_542,
  CASE WHEN LOWER(dcar.carrier_cd_bus_key) = 'paazgu10' AND LOWER(dmkt.mkt_type_desc) <> 'e&s' THEN 0 ELSE 1 END AS dq_rule_543,
  CASE WHEN COALESCE(f.prem_amt,0)  = COALESCE(f.total_prem,0)  THEN 1 ELSE 0 END AS dq_rule_544,
  CASE WHEN COALESCE(f.fsa_amt,0)   = COALESCE(f.total_fsa,0)   THEN 1 ELSE 0 END AS dq_rule_545,
  CASE WHEN COALESCE(f.paa_amt,0)   = COALESCE(f.total_paa,0)   THEN 1 ELSE 0 END AS dq_rule_546,
  CASE WHEN COALESCE(f.sf3_amt,0)   = COALESCE(f.total_sf3,0)   THEN 1 ELSE 0 END AS dq_rule_547,
  CASE WHEN COALESCE(f.sf4_amt,0)   = COALESCE(f.total_sf4,0)   THEN 1 ELSE 0 END AS dq_rule_548,
  CASE WHEN COALESCE(f.sf5_amt,0)   = COALESCE(f.total_sf5,0)   THEN 1 ELSE 0 END AS dq_rule_549,
  CASE WHEN COALESCE(f.sf6_amt,0)   = COALESCE(f.total_sf6,0)   THEN 1 ELSE 0 END AS dq_rule_550,
  CASE WHEN COALESCE(f.sf7_amt,0)   = COALESCE(f.total_sf7,0)   THEN 1 ELSE 0 END AS dq_rule_551,
  CASE WHEN COALESCE(f.comm_amt,0)  = COALESCE(f.total_comm,0)  THEN 1 ELSE 0 END AS dq_rule_552,
  CASE WHEN COALESCE(f.non_comm_prem_amt,0) = COALESCE(f.total_non_comm,0) THEN 1 ELSE 0 END AS dq_rule_553,
  CASE WHEN LOWER(f.lob_cd) NOT IN ('wc','bp') AND dbc.bus_class_cd_bus_key IS NOT NULL THEN 0 ELSE 1 END AS dq_rule_554

FROM fact_base f
LEFT JOIN dim_policy dpol       ON UPPER(TRIM(f.policy_num_bus_key)) = UPPER(TRIM(dpol.policy_num_bus_key))
LEFT JOIN dim_date dd1          ON f.policy_effec_start_date = dd1.date
LEFT JOIN dim_date dd2          ON f.policy_effec_end_date = dd2.date
LEFT JOIN dim_date dd3          ON f.policy_coverage_start_date = dd3.date
LEFT JOIN dim_date dd4          ON f.policy_coverage_end_date = dd4.date
LEFT JOIN dim_lob dlob          ON UPPER(TRIM(f.lob_cd)) = UPPER(TRIM(dlob.lob_cd_bus_key))
LEFT JOIN dim_dist_chnl ddc     ON UPPER(TRIM(f.agency_cd)) = UPPER(TRIM(ddc.agcy_cd_bus_key))
LEFT JOIN dim_insured dins      ON UPPER(TRIM(f.insured_code)) = UPPER(TRIM(dins.policy_num_bus_key))
LEFT JOIN dim_naics dnai        ON UPPER(TRIM(f.naics_cd)) = UPPER(TRIM(dnai.naics_bus_key))
LEFT JOIN dim_employee demp     ON UPPER(TRIM(f.dec_uw_emp_cd)) = UPPER(TRIM(demp.emp_cd_bus_key))
LEFT JOIN dim_mkt_type dmkt     ON UPPER(TRIM(f.market_type_cd)) = UPPER(TRIM(dmkt.mkt_type_cd_bus_key))
LEFT JOIN dim_prod_src dpro     ON UPPER(TRIM(f.production_src_cd)) = UPPER(TRIM(dpro.prod_src_cd_bus_key))
LEFT JOIN dim_carrier dcar      ON UPPER(TRIM(f.carrier_cd)) = UPPER(TRIM(dcar.carrier_cd_bus_key))
LEFT JOIN dim_policy_trans_type dtrans ON UPPER(TRIM(f.policy_trans_type_cd)) = UPPER(TRIM(dtrans.policy_trans_type_cd_bus_key))
LEFT JOIN dim_date dd5          ON f.policy_trans_effec_date = dd5.date
LEFT JOIN dim_date dd6          ON f.policy_trans_date = dd6.date
LEFT JOIN dim_date dd7          ON f.policy_trans_written_on_date = dd7.date
LEFT JOIN dim_business_class dbc 
  ON UPPER(TRIM(f.class_cd_bus_key)) = UPPER(TRIM(dbc.bus_class_cd_bus_key))
 AND UPPER(TRIM(f.class_suffix_bus_key)) = UPPER(TRIM(dbc.bus_class_suffix_bus_key))
 AND UPPER(TRIM(f.class_src_id_bus_key)) = UPPER(TRIM(dbc.bus_class_src_id_bus_key));
