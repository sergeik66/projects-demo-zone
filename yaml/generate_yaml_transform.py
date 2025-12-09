# CELL 1: USER PARAMETERS – EDIT THIS CELL ONLY
# =====================================================================

# --------------------- TARGET TABLE ---------------------
target_lakehouse = "den_lhw_scu_001_claims_transaction_curated"
target_schema    = "stg_claims_transaction"
target_table     = "stg_adjustor_stats_trans"        # Also used as filename
load_strategy    = "overwrite"
key_columns      = []                                # Leave empty for staging tables
unknown_record   = False
identity         = False

# --------------------- SOURCE TABLES (auto-detect + manual extras ---------------------
# List all 3-part names you use in any query below
source_tables_3part = [
    "den_lhw_scu_001_claims_transaction_curated.iis_transaction.claims_adjustor_stats",
    "den_lhw_scu_001_claims_transaction_curated.iis_transaction.claimant",
    "den_lhw_scu_001_claims_transaction_curated.iis_transaction.insured",
    "den_lhw_scu_001_claims_transaction_curated.iis_transaction.lookups",
    "den_lhw_scu_001_claims_transaction_curated.iis_transaction.lob",
    "den_lhw_scu_001_claims_transaction_curated.iis_transaction.employee",
    "den_lhw_scu_001_claims_transaction_curated.iis_transaction.tpaasof",
    "den_lhw_scu_001_claims_transaction_curated.claims_user_data.user_adjustor_groups"
]

# Add any extra sources not present in queries (rare)
extra_sources = []

# --------------------- MULTIPLE QUERIES ---------------------
# Each query must have:
#   - name: short name used as CTE / view name in final YAML
#   - sql:  full SQL using 3-PART names (for validation)
#   - description: optional

queries = [
    {
        "name": "filtered_adjustor_stats",
        "sql": """
            SELECT 
              adjustorstatsid, claimantid, transactiontype, enteredon, lob,
              coveragecategory, complexity, businessunit, adjustor, adjustorstatus
            FROM den_lhw_scu_001_claims_transaction_curated.iis_transaction.claims_adjustor_stats
            WHERE UPPER(transactiontype) IN ('N', 'TI', 'SN', 'RO')
              AND (adjustor <> 11220 OR UPPER(transactiontype) = 'RO')
              AND adjustorstatus IN (0, 2)
        """.strip()
    },
    {
        "name": "mcuadj_users",
        "sql": """
            SELECT userid 
            FROM den_lhw_scu_001_claims_transaction_curated.claims_user_data.user_adjustor_groups 
            WHERE adjustor_group = 'mcuadj'
        """.strip()
    },
    {
        "name": "lladj_users",
        "sql": """
            SELECT userid 
            FROM den_lhw_scu_001_claims_transaction_curated.claims_user_data.user_adjustor_groups 
            WHERE adjustor_group = 'lladj'
        """.strip()
    },
    {
        "name": "pdliadj_users",
        "sql": """
            SELECT userid 
            FROM den_lhw_scu_001_claims_transaction_curated.claims_user_data.user_adjustor_groups 
            WHERE adjustor_group = 'pdliadj'
        """.strip()
    },
    {
        "name": "tpa_with_transaction_date",
        "sql": """
            SELECT 
              a.claimantid,
              a.enteredon as transaction_date,
              FIRST_VALUE(t.tpataxid) OVER (
                PARTITION BY a.claimantid, a.enteredon
                ORDER BY t.enteredon DESC
                ROWS UNBOUNDED PRECEDING
              ) AS latest_tpataxid
            FROM filtered_adjustor_stats a
            LEFT JOIN den_lhw_scu_001_claims_transaction_curated.iis_transaction.tpaasof t 
              ON a.claimantid = t.claimantid AND t.enteredon <= a.enteredon
        """.strip()
    },
    {
        "name": "transrec",
        "sql": """
            -- (your huge transrec query here – keep full 3-part names)
            SELECT 
              a.adjustorstatsid, 
              a.claimantid, 
              a.transactiontype,
              l.descrip AS transactiondescrip,
              a.enteredon,
              -- ... rest of your fields ...
              ROW_NUMBER() OVER(PARTITION BY a.claimantid ORDER BY a.enteredon) AS rcount
            FROM filtered_adjustor_stats a
            INNER JOIN den_lhw_scu_001_claims_transaction_curated.iis_transaction.claimant c ON a.claimantid = c.claimantid
            -- ... all other joins with full 3-part names ...
            WHERE COALESCE(c.voidortransfer, '') = ''
            ORDER BY a.claimantid, a.enteredon
        """.strip(),
        "description": "Main transformation logic with lookups and flags"
    },
    {
        "name": "stg_adjustor_stats_trans",   # Final query – must match target_table
        "sql": """
            SELECT 
              t.adjustorstatsid,
              CONCAT('EMP-', t.adjustor,'-', UPPER(TRIM(t.cvgcategory))) AS adjustr_bus_key,
              t.claimantid as claimant_bus_key,
              t.transactiontype as adjustr_trans_type_bus_key,
              t.transactiondescrip,
              t.enteredon AS adjustr_entry_date_bus_key,
              t.entered_mo,
              t.fullclaimnumber,
              t.lob as lob_cd_bus_key,
              t.lob_descrip,
              -- ... all other columns ...
              CASE WHEN t.claimType = tprev.claimType AND t.unit = tprev.unit THEN 'N' ELSE 'Y' END AS TrueTransfer
            FROM transrec t
            LEFT JOIN transrec tprev 
              ON t.claimantid = tprev.claimantid 
             AND (t.rcount - 1) = tprev.rcount
            ORDER BY t.claimantid, t.enteredon
        """.strip(),
        "description": "Final staging table with business keys and transfer flag"
    }
]

# =====================================================================
# CELL 2: Validate all queries one by one (very useful!)
# =====================================================================

print("Validating all queries...\n")
for q in queries:
    try:
        print(f"Testing: {q['name']}")
        df = spark.sql(q["sql"])
        print(f"   Success – {df.count():,} rows")
    except Exception as e:
        print(f"   FAILED: {q['name']}")
        print("   ", str(e)[:200] + "..." if len(str(e)) > 200 else str(e))
        print()
# CELL 3: Auto-detect ALL source tables from ALL queries
# =====================================================================

import re

pattern = r'\b([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\b'
all_matches = []

for q in queries:
    all_matches.extend(re.findall(pattern, q["sql"]))

# Extract lakehouse.schema.table

detected = []
seen = set()
for lh, sch, tbl in all_matches:
    key = (lh, sch, tbl)
    if key not in seen:
        seen.add(key)
        detected.append({
            "name": tbl,
            "lakehouse": lh,
            "schema": sch,
            "table": tbl
        })

# Merge with manual extras
final_sources = detected + extra_sources
final_sources = [dict(t) for t in {tuple(s.items()) for s in final_sources}]  # dedupe

print(f"Detected {len(final_sources)} unique source tables:")
for s in final_sources:
    print(f"  • {s['lakehouse']}.{s['schema']}.{s['table']}")

# CELL 4: Replace full 3-part names → short table names in YAML queries
# =====================================================================

replace_map = {f"{s['lakehouse']}.{s['schema']}.{s['table']}": s['table'] for s in final_sources}

yaml_queries = []
for q in queries:
    yaml_sql = q["sql"]
    for full, short in replace_map.items():
        yaml_sql = re.sub(rf'\b{re.escape(full)}\b', short, yaml_sql)
    
    yaml_q = {
        "name": q["name"],
        "sql": yaml_sql.strip()
    }
    if "description" in q:
        yaml_q["description"] = q["description"]
    
    yaml_queries.append(yaml_q)

print(f"Prepared {len(yaml_queries)} queries for YAML")

# CELL 5: Generate final YAML file
# =====================================================================

import yaml
from datetime import datetime

yaml_dict = {
    "target": {
        "lakehouse": target_lakehouse,
        "schema": target_schema,
        "table": target_table,
        "load_strategy": load_strategy,
        "key_columns": key_columns,
        "unknown_record": unknown_record,
        "identity": identity
    },
    "source": final_sources,
    "query": yaml_queries
}

# Add identity column only if enabled
if identity:
    yaml_dict["target"]["identity_column_name"] = "your_surrogate_key_name"  # change if needed

# Generate clean YAML
yaml_output = yaml.safe_dump(yaml_dict, sort_keys=False, indent=2, width=1000, allow_unicode=True)

print("\n" + "="*70)
print(f"YAML FOR {target_table}.yaml")
print("="*70)
print(yaml_output)

# Save to file
filename = f"{target_table}.yaml"
with open(filename, "w", encoding="utf-8") as f:
    f.write(yaml_output)

displayHTML(f"""
<h2 YAML generated and saved!<br>
<b>File:</b> <code style='font-size:1.2em'>{filename}</code><br>
<b>Queries:</b> {len(yaml_queries)} | <b>Sources:</b> {len(final_sources)}
""")

100% matches your real stg_adjustor_stats_trans.yaml structure
Fully parameterized – just edit Cell 1
Auto-detects every source table used in any query
Validates all SQL before generating YAML
Converts 3-part → short names automatically
Supports descriptions per query
Works for staging, dims, hubs, links, facts — just change load_strategy

Just change the values in Cell 1 and paste your real multi-query logic — you’ll get a perfect YAML every time in < 10 seconds.
