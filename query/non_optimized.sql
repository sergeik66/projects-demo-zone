WITH
        autowindbase AS (
          SELECT
            ar.keyfield,
            ar.autouwstatus,
            ROW_NUMBER() OVER (
              PARTITION BY
                ar.keyfield
              ORDER BY
                ar.updateon DESC
            ) AS RN
          FROM
            autouw_results ar
          WHERE
            ar.systemkey <> 'IIS_AUTHORITY_BP'
        ),
        autowind AS (
          SELECT
            keyfield,
            autouwstatus
          FROM
            autowindbase
          WHERE
            RN = 1
        ),
        nextpolicycheck1 AS (
          SELECT
            code,
            renewedbycode
          FROM
            insured
          WHERE
            code = basemgacode
            AND LOWER(agency <> 'pafake10')
        ),
        nxtpolchk2base AS (
          SELECT
            npc2.code AS code,
            npc2.renewof AS renewof,
            npc2.enteredon AS enteredon,
            ROW_NUMBER() OVER (
              PARTITION BY
                npc2.renewof
              ORDER BY
                npc2.enteredon DESC
            ) AS npc2rn
          FROM
            insured npc2
          WHERE
            COALESCE(LOWER(npc2.flags04, '')) = ''
            AND npc2.code = npc2.basemgacode
        ),
        nxtpolchk2 AS (
          SELECT
            code,
            renewof
          FROM
            nxtpolchk2base
          WHERE
            npc2rn = 1
        ),
        combine AS (
          SELECT
            n1.code AS code,
            (
              CASE
                WHEN n1.renewedbycode IS NOT NULL THEN n1.renewedbycode
                ELSE n2.code
              END
            ) AS nextpolicy
          FROM
            nextpolicycheck1 n1
            LEFT OUTER JOIN nxtpolchk2 n2 ON n2.renewof = n1.code
        ),
        np_combined_withstatuses AS (
          SELECT
            c.code AS currpol,
            c.nextpolicy AS nextpol,
            i.stat2 AS nextpolstat2,
            i.stat7 AS nextpolstat7
          FROM
            combine c
            JOIN insured i ON c.nextpolicy = i.code
        )
      SELECT
        i.code AS policy_num_bus_key,
        CASE
          WHEN LOWER(trim(i.flags14)) = 'n' THEN 'New Business'
          ELSE 'Renewal'
        END AS policy_bus_type,
        w.govstate AS gov_state,
        i.pobegin AS policy_effec_start_dt,
        i.poexpir AS policy_effec_end_dt,
        i.pobegin AS policy_coverage_start_dt,
        CASE
          WHEN LOWER(trim(i.stat5)) = 'c' THEN i.pocancel
          ELSE i.poexpir
        END AS policy_coverage_end_dt,
        CASE
          WHEN LOWER(trim(i.stat5)) = 'c' THEN i.pocancel
          ELSE NULL
        END AS policy_cancel_dt,
        CASE
          WHEN LOWER(trim(i.stat5)) = 'c' THEN i.trcancel
          ELSE NULL
        END AS policy_cancel_trans_dt,
        i.poaudit AS policy_audit_dt,
        i.enteredon AS policy_sub_dt,
        i.poissue AS policy_issue_dt,
        CASE
          WHEN LOWER(trim(i.offerren)) = 'y' THEN 1
          ELSE 0
        END AS offer_rn,
        i.renewalofcode AS rn_of,
        i.renewedbycode AS rn_by,
        i.chainid AS chain_id,
        b.firstpaid AS first_dwnpymnt_recd_dt,
        (
          CASE
            WHEN i.flags39 IN ('w', 'x') THEN i.flags39
          END
        ) AS rewrite_reissue,
        (
          CASE
            WHEN i.stat2 = 'i'
            AND i.stat5 <> 'c' THEN 1
            ELSE 0
          END
        ) AS is_expiring_flag,
        (
          CASE
            WHEN i.stat2 = 'i'
            AND i.stat5 = 'c' THEN 'n'
            WHEN npcw.nextpol IS NOT NULL
            AND (
              npcw.nextpolstat2 = 'i'
              OR (
                npcw.nextpolstat2 <> 'i'
                AND i.offerren = 'y'
              )
            ) THEN 'y'
            WHEN (
              i.offerren <> 'y'
              AND i.offerren IS NOT NULL
            )
            OR (
              npcw.nextpolstat2 <> 'i'
              AND ln.descrip = 'declined by ca/uw'
            )
            OR l.descrip IN ('declined by agency', 'declined by ca/uw') THEN 'n'
            ELSE 'U'
          END
        ) AS is_renewable_flag,
        (
          CASE
            WHEN m.code IS NOT NULL THEN 1
            ELSE 0
          END
        ) AS is_prev_policy_ubetag_flag,
        (
          CASE
            WHEN npcw.nextpol IS NOT NULL THEN 1
            ELSE 0
          END
        ) AS is_cloned_flag,
        l.descrip AS src_policy_status_descrip,
        dpol.policy_status_key AS latest_policy_status_key,
        TRUE AS dl_is_current_flag,
        '1901-01-01' AS dl_row_effective_date,
        '9999-12-31' AS dl_row_expiration_date,
        FALSE AS dl_is_deleted_flag 
      FROM
        insured i
        LEFT OUTER JOIN billings b ON LOWER(trim(b.code)) = LOWER(trim(i.code))
        LEFT OUTER JOIN wcinfo w ON LOWER(trim(w.code)) = LOWER(trim(i.code))
        LEFT OUTER JOIN lookups l ON LOWER(trim(l.keyfield)) = 'stat7'
        AND LOWER(trim(l.programid)) = 'insured'
        AND LOWER(trim(l.vals)) = LOWER(trim(i.stat7))
        LEFT OUTER JOIN autowind ar ON LOWER(trim(ar.keyfield)) = LOWER(trim(i.code))
        LEFT OUTER JOIN (
          SELECT DISTINCT
            code
          FROM
            mgnotrans
          WHERE
            reason = '13.00'
        ) mg ON LOWER(trim(mg.code)) = LOWER(trim(i.code))
        LEFT OUTER JOIN insured p ON LOWER(trim(p.code)) = LOWER(trim(i.renewalofcode))
        LEFT JOIN (
          SELECT DISTINCT
            mn.code
          FROM
            mgnotrans mn
            INNER JOIN mgnotrans mn2 ON mn.code = mn2.code
            AND mn2.typeid = 143 LATERAL VIEW explode (split (mn.extra, '\\|')) x AS VALUE
          WHERE
            LOWER(trim(mn.transcode)) = 'ex'
            AND VALUE <> ''
        ) m ON LOWER(m.code) = LOWER(p.code)
        LEFT OUTER JOIN np_combined_withstatuses npcw ON LOWER(trim(i.code)) = LOWER(trim(npcw.currpol))
        LEFT OUTER JOIN lookups ln ON LOWER(trim(ln.keyfield)) = 'stat7'
        AND LOWER(ln.programid) = 'insured'
        AND LOWER(ln.vals) = LOWER(npcw.nextpolstat7)
        LEFT OUTER JOIN dim_policy_status dpol ON LOWER(trim(dpol.policy_status_cd_bus_key)) = CASE
          WHEN LOWER(i.flags39) = 'w'
          AND COALESCE(LOWER(i.flags14), '') = 'n'
          AND COALESCE(LOWER(i.stat5, '')) = 'c'
          AND COALESCE(LOWER(i.typcancel, '')) = 'p' THEN 'NB - Canceled Pro-rata'
          WHEN LOWER(i.flags39) = 'x'
          AND COALESCE(LOWER(i.flags14, '')) = 'n'
          AND COALESCE(LOWER(i.stat5, '')) = 'c'
          AND COALESCE(LOWER(i.typcancel, '')) = 'p' THEN 'NB - Canceled Pro-rata'
          WHEN LOWER(i.flags39) = 'w'
          AND COALESCE(LOWER(i.flags14, '')) = 'r'
          AND COALESCE(LOWER(i.stat5, '')) = 'c'
          AND COALESCE(LOWER(i.typcancel, '')) = 'p' THEN 'RN - Canceled Pro-rata'
          WHEN LOWER(i.flags39) = 'x'
          AND COALESCE(LOWER(i.flags14, '')) = 'r'
          AND COALESCE(LOWER(i.stat5, '')) = 'c'
          AND COALESCE(LOWER(i.typcancel, '')) = 'p' THEN 'RN - Canceled Pro-rata'
          WHEN LOWER(trim(i.flags39)) = 'w' THEN 're-write'
          WHEN LOWER(trim(i.flags39)) = 'x' THEN 're-issue'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.stat5)), '') = 'c'
          AND COALESCE(LOWER(trim(i.typcancel)), '') = 'f' THEN 'nb - canceled flat'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.stat5)), '') = 'c'
          AND COALESCE(LOWER(trim(i.typcancel)), '') = 's' THEN 'nb - canceled short rate'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.stat5)), '') = 'c'
          AND COALESCE(LOWER(trim(i.typcancel)), '') = 'p' THEN 'nb - canceled pro-rata'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') = 'p'
          AND LOWER(trim(l.descrip)) = 'declined by agency'
          AND i.poytdprem IS NULL
          AND LOWER(trim(ar.autouwstatus)) <> 'decline' THEN 'nb - abandoned before prem est'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') = 'p'
          AND LOWER(trim(l.descrip)) = 'declined by agency'
          AND i.poytdprem IS NULL
          AND LOWER(trim(ar.autouwstatus)) = 'decline' THEN 'nb - decline - class of exposure'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') = 'p'
          AND LOWER(trim(l.descrip)) = 'declined by agency'
          AND i.poytdprem IS NOT NULL
          AND LOWER(trim(ar.autouwstatus)) <> 'decline' THEN 'nb - agent withdrew after initial prem estimate'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') = 'p'
          AND LOWER(trim(l.descrip)) = 'declined by agency'
          AND i.poytdprem IS NOT NULL
          AND LOWER(trim(ar.autouwstatus)) = 'decline' THEN 'nb - decline - questions'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') = 'p'
          AND LOWER(trim(l.descrip)) = 'declined by ca/uw' THEN 'nb - decline - broker of record'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') <> 'p'
          AND LOWER(trim(l.descrip)) = 'declined by agency'
          AND LOWER(trim(i.stat0)) <> 'p'
          AND mg.code IS NOT NULL THEN 'nb - not responding to uw'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') <> 'p'
          AND LOWER(trim(l.descrip)) = 'declined by agency'
          AND LOWER(trim(i.stat0)) = 'p' THEN 'nb - quote not taken'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') <> 'p'
          AND LOWER(trim(l.descrip)) = 'declined by agency'
          AND LOWER(trim(i.stat0)) <> 'p' THEN 'nb - agent withdrew before quote'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') <> 'p'
          AND LOWER(trim(l.descrip)) = 'declined by ca/uw' THEN 'nb - decline - uw'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') <> 'p'
          AND LOWER(trim(i.stat2)) = 'i'
          AND COALESCE(LOWER(trim(i.stat5)), '') <> 'c' THEN 'nb - issued'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') <> 'p'
          AND (
            (
              LOWER(trim(l.descrip)) IN ('pending agy decision', 'accepted by agency')
              AND LOWER(trim(i.stat2)) <> 'i'
            )
            OR (
              LOWER(l.descrip) = 'waiting for info'
              AND LOWER(i.stat0) = 'p'
            )
          ) THEN 'nb - open quote'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND (
            (
              COALESCE(LOWER(trim(i.flags33)), '') = 'p'
              AND i.poytdprem IS NOT NULL
            )
            OR COALESCE(LOWER(trim(i.flags33)), '') <> 'p'
          ) THEN 'nb - open completed submission'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n'
          AND COALESCE(LOWER(trim(i.flags33)), '') = 'p' THEN 'nb - open submission'
          WHEN COALESCE(LOWER(trim(i.flags14)), '') = 'n' THEN 'nb - other exception'
          WHEN LOWER(.flags14) = 'r'
          AND LOWER(i.stat2) = 'i'
          AND COALESCE(LOWER(i.stat5, '')) <> 'c'
          AND b.firstpaid IS NOT NULL THEN 'RN - Issued Paid'
          WHEN LOWER(i.flags14) = 'r'
          AND LOWER(i.stat2) = 'i'
          AND COALESCE(LOWER(i.stat5, '')) <> 'c'
          AND b.firstpaid IS NULL THEN 'RN - Issued Not Paid'
          WHEN LOWER(i.flags14) = 'r'
          AND LOWER(i.stat2) = 'i'
          AND COALESCE(LOWER(i.stat5, '')) = 'c'
          AND COALESCE(LOWER(i.typcancel, '')) = 'S' THEN 'RN - Canceled Short Rate'
          WHEN LOWER(i.flags14) = 'r'
          AND LOWER(i.stat2) = 'i'
          AND COALESCE(LOWER(i.stat5, '')) = 'c'
          AND COALESCE(LOWER(i.typcancel, '')) = 'p' THEN 'RN - Canceled Pro-rata'
          WHEN LOWER(i.flags14) = 'r'
          AND LOWER(i.stat2) <> 'i'
          AND LOWER(trim(l.descrip)) = 'declined by ca/uw' THEN 'RN - GUARD Declined'
          WHEN LOWER(i.flags14) = 'r'
          AND (
            LOWER(i.stat2) <> 'i'
            AND LOWER(trim(l.descrip)) = 'declined by agency'
            AND LOWER(p.offerren) = 'y'
          )
          OR (
            LOWER(i.stat2) = 'i'
            AND COALESCE(LOWER(i.stat5, '')) = 'c'
            AND COALESCE(LOWER(i.typcancel, '')) = 'F'
          ) THEN 'RN - Quote Not Taken'
          WHEN LOWER(i.flags14) = 'r'
          AND LOWER(i.stat2) <> 'i'
          AND LOWER(p.offerren) = 'y' THEN 'RN - Open Quote'
          WHEN LOWER(i.flags14) = 'r'
          AND LOWER(i.stat2) <> 'i'
          AND LOWER(p.offerren) <> 'y' THEN 'RN - Open Renewal'
          WHEN LOWER(i.flags14) = 'r' THEN 'RN - Other Exception'
          ELSE 'other - exception'
        END
      WHERE
        LOWER(trim(i.code)) = LOWER(trim(i.basemgacode))
        AND LOWER(i.agency) <> 'pafake10'
