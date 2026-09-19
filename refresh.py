#!/usr/bin/env python3
"""Auto-refresh TM Weekly Trend Dashboard data from Databricks SQL API."""

import json
import os
import re
import sys
import time
import requests
from datetime import datetime, date

HOST = os.environ.get("DATABRICKS_HOST", "https://4460961511039885.5.gcp.databricks.com")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "76e1e5ab77cf3fda")


def _get_token():
    """Use env var if set (CI), otherwise fall back to Databricks CLI OAuth."""
    env_token = os.environ.get("DATABRICKS_TOKEN")
    if env_token:
        return env_token
    import subprocess
    try:
        result = subprocess.run(
            ["databricks", "auth", "token", "--profile", "DEFAULT"],
            capture_output=True, text=True, timeout=10
        )
        data = json.loads(result.stdout)
        return data["access_token"]
    except Exception as e:
        print(f"Failed to get token from Databricks CLI: {e}")
        sys.exit(1)


TOKEN = _get_token()
HTML_PATH = os.path.join(os.path.dirname(__file__), "index.html")


def run_query(sql, label="query"):
    """Execute SQL via Databricks Statement API, return list of dicts."""
    url = f"{HOST}/api/2.0/sql/statements"
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    print(f"[{label}] Submitting query to {HOST}...")
    resp = requests.post(url, json=payload, headers=headers, timeout=60)
    if resp.status_code != 200:
        print(f"[{label}] HTTP {resp.status_code}: {resp.text[:500]}")
        return []
    data = resp.json()

    if "statement_id" not in data:
        print(f"[{label}] Unexpected response: {json.dumps(data)[:500]}")
        return []

    for attempt in range(60):
        state = data.get("status", {}).get("state", "UNKNOWN")
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELED", "CLOSED"):
            print(f"[{label}] FAILED: {data.get('status', {}).get('error', {})}")
            return []
        time.sleep(5)
        stmt_id = data["statement_id"]
        resp = requests.get(f"{url}/{stmt_id}", headers=headers, timeout=30)
        data = resp.json()
    else:
        print(f"[{label}] Timed out waiting for query")
        return []

    cols = [c["name"] for c in data["manifest"]["schema"]["columns"]]
    rows = data.get("result", {}).get("data_array", [])
    print(f"[{label}] Got {len(rows)} rows")
    return [dict(zip(cols, row)) for row in rows]


def fmt_week(ds):
    """'2026-07-13 00:00:00' or '2026-07-13' → 'Jul 13'"""
    d = ds.split("T")[0].split(" ")[0]
    dt = datetime.strptime(d, "%Y-%m-%d")
    return dt.strftime("%b %-d")


def safe_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def safe_int(v, default=0):
    try:
        return int(float(v)) if v is not None else default
    except (ValueError, TypeError):
        return default


# ── SQL Queries ──────────────────────────────────────────────────────────

WEEKLY_CORE_SQL = """
WITH test_accounts AS (
  SELECT DISTINCT account_id FROM `tp-prod-sg`.dim_account_service.testing_accounts
  WHERE account_id IS NOT NULL
),
sf_seg AS (
  SELECT client_legal_entity_id AS cle_id, MAX(sf_customer_segment) AS seg
  FROM `data-prod-sg`.dim_commercial.commercial_account
  WHERE client_legal_entity_id IS NOT NULL GROUP BY 1
),
base AS (
  SELECT
    date_trunc('week', t.transaction_created_time) AS week_start,
    CASE WHEN s.seg LIKE '%Enterprise%' OR s.seg = 'T1 SME' THEN 'ENT_T1'
         WHEN s.seg IN ('T2 SME','T3 SME','T4 SME') THEN 'T24'
         ELSE 'OTH' END AS tier,
    t.if_alert, t.if_rfi, t.if_reject,
    t.final_transaction_amount_usd,
    t.uar_case_reviewed_as_fraudulent
  FROM `risk-prod-sg`.dws_risk.risk_tm_real_time_transaction_results t
  LEFT JOIN sf_seg s ON s.cle_id = t.client_legal_entity_id
  WHERE t.transaction_created_time >= date_add(date_trunc('week', current_date()), -84)
    AND t.transaction_created_time < current_date()
    AND t.transaction_type IN ('DEPOSIT','PAYMENT','DIRECT_DEBIT')
    AND t.account_id NOT IN (SELECT account_id FROM test_accounts)
    AND t.final_transaction_amount_usd IS NOT NULL
)
SELECT week_start, tier AS segment,
  COUNT(*) AS txns,
  SUM(CASE WHEN if_alert THEN 1 ELSE 0 END) AS alerts,
  SUM(CASE WHEN if_rfi THEN 1 ELSE 0 END) AS rfis,
  SUM(CASE WHEN if_reject THEN 1 ELSE 0 END) AS rejects,
  SUM(CASE WHEN if_alert AND NOT if_reject THEN 1 ELSE 0 END) AS adj_alerts,
  ROUND(SUM(CASE WHEN if_reject THEN final_transaction_amount_usd ELSE 0 END),0) AS reject_amt,
  ROUND(SUM(final_transaction_amount_usd),0) AS total_amt,
  SUM(CASE WHEN uar_case_reviewed_as_fraudulent=1 THEN 1 ELSE 0 END) AS fraud_txns,
  ROUND(SUM(CASE WHEN uar_case_reviewed_as_fraudulent=1 THEN final_transaction_amount_usd ELSE 0 END),0) AS fraud_amt
FROM base
WHERE tier IN ('ENT_T1','T24')
GROUP BY week_start, tier

UNION ALL

SELECT week_start, 'ALL' AS segment,
  COUNT(*) AS txns,
  SUM(CASE WHEN if_alert THEN 1 ELSE 0 END) AS alerts,
  SUM(CASE WHEN if_rfi THEN 1 ELSE 0 END) AS rfis,
  SUM(CASE WHEN if_reject THEN 1 ELSE 0 END) AS rejects,
  SUM(CASE WHEN if_alert AND NOT if_reject THEN 1 ELSE 0 END) AS adj_alerts,
  ROUND(SUM(CASE WHEN if_reject THEN final_transaction_amount_usd ELSE 0 END),0) AS reject_amt,
  ROUND(SUM(final_transaction_amount_usd),0) AS total_amt,
  SUM(CASE WHEN uar_case_reviewed_as_fraudulent=1 THEN 1 ELSE 0 END) AS fraud_txns,
  ROUND(SUM(CASE WHEN uar_case_reviewed_as_fraudulent=1 THEN final_transaction_amount_usd ELSE 0 END),0) AS fraud_amt
FROM base
GROUP BY week_start
ORDER BY week_start, segment
"""

CV_HITS_SQL = """
WITH verification_rules AS (
  SELECT DISTINCT rule_id
  FROM (SELECT rule_id,
               array_distinct(regexp_extract_all(CAST(actions AS string),'"type":"([A-Z_]+)"',1)) AS action_types,
               ROW_NUMBER() OVER (PARTITION BY profile_id, rule_id ORDER BY created_at DESC) AS rn
        FROM `risk-prod-sg`.silver.riskrulemanagementservice__pr_relation_p_tenant_airwallex
        WHERE checkpoint='transaction' AND status=1)
  WHERE rn=1 AND array_contains(action_types,'ACCEPT') AND size(action_types)>1
    AND NOT array_contains(action_types,'THREE_FACTOR_AUTH_ACTION')
),
sf_seg AS (
  SELECT client_legal_entity_id AS cle_id, MAX(sf_customer_segment) AS seg
  FROM `data-prod-sg`.dim_commercial.commercial_account
  WHERE client_legal_entity_id IS NOT NULL GROUP BY 1
),
cv_txns AS (
  SELECT DISTINCT r.data:transactionId::string AS txn_id
  FROM `risk-prod-sg`.silver.compliance__cs_tm_rule_result r
  LATERAL VIEW explode(cast(r.data:results AS array<struct<errMessage:string,ruleId:string,ruleName:string>>)) x AS res
  WHERE r.create_time >= date_add(date_trunc('week', current_date()), -84)
    AND r.create_time < current_date()
    AND get_json_object(res.errMessage,'$.hit')='true'
    AND get_json_object(res.errMessage,'$.ruleId') IN (SELECT rule_id FROM verification_rules)
)
SELECT date_trunc('week', t.transaction_created_time) AS week_start,
       CASE WHEN s.seg LIKE '%Enterprise%' OR s.seg='T1 SME' THEN 'ENT_T1'
            WHEN s.seg IN ('T2 SME','T3 SME','T4 SME') THEN 'T24'
            ELSE 'OTH' END AS segment,
       COUNT(DISTINCT cv.txn_id) AS cv_hits
FROM cv_txns cv
JOIN `risk-prod-sg`.dws_risk.risk_tm_real_time_transaction_results t
  ON t.transaction_id = cv.txn_id
  AND t.transaction_created_time >= date_add(date_trunc('week', current_date()), -84)
  AND t.transaction_created_time < current_date()
  AND t.transaction_type IN ('DEPOSIT','PAYMENT','DIRECT_DEBIT')
LEFT JOIN sf_seg s ON s.cle_id = t.client_legal_entity_id
GROUP BY 1, 2
ORDER BY 1, 2
"""

TOP_RULES_SQL = """
WITH test_accounts AS (
  SELECT DISTINCT account_id FROM `tp-prod-sg`.dim_account_service.testing_accounts
  WHERE account_id IS NOT NULL
),
verification_rules AS (
  SELECT DISTINCT rule_id
  FROM (SELECT rule_id,
               array_distinct(regexp_extract_all(CAST(actions AS string),'"type":"([A-Z_]+)"',1)) AS action_types,
               ROW_NUMBER() OVER (PARTITION BY profile_id, rule_id ORDER BY created_at DESC) AS rn
        FROM `risk-prod-sg`.silver.riskrulemanagementservice__pr_relation_p_tenant_airwallex
        WHERE checkpoint='transaction' AND status=1)
  WHERE rn=1 AND array_contains(action_types,'ACCEPT') AND size(action_types)>1
    AND NOT array_contains(action_types,'THREE_FACTOR_AUTH_ACTION')
),
rt_rules AS (
  SELECT date_trunc('week', t.transaction_created_time) AS week_start,
    CAST(t.rule_ids AS STRING) AS rule_id, t.rule_names AS rule_name,
    t.client_legal_entity_id AS cle_id,
    1 AS alerts, CASE WHEN t.if_rfi THEN 1 ELSE 0 END AS rfis,
    CASE WHEN t.if_reject THEN 1 ELSE 0 END AS rejects, 0 AS is_cv
  FROM `risk-prod-sg`.dws_risk.risk_tm_real_time_transaction_results t
  WHERE t.transaction_created_time >= date_add(date_trunc('week', current_date()), -84)
    AND t.transaction_created_time < current_date()
    AND t.if_alert=true AND t.transaction_type IN ('DEPOSIT','PAYMENT','DIRECT_DEBIT')
    AND t.account_id NOT IN (SELECT account_id FROM test_accounts)
    AND t.final_transaction_amount_usd IS NOT NULL AND t.rule_ids IS NOT NULL
),
cv_exploded AS (
  SELECT r.create_time, r.data:transactionId::string AS txn_id,
    get_json_object(res.errMessage,'$.ruleId') AS rule_id, res.ruleName AS rule_name
  FROM `risk-prod-sg`.silver.compliance__cs_tm_rule_result r
  LATERAL VIEW explode(cast(r.data:results AS array<struct<errMessage:string,ruleId:string,ruleName:string>>)) x AS res
  WHERE r.create_time >= date_add(date_trunc('week', current_date()), -84)
    AND r.create_time < current_date()
    AND get_json_object(res.errMessage,'$.hit')='true'
    AND get_json_object(res.errMessage,'$.ruleId') IN (SELECT rule_id FROM verification_rules)
),
cv_with_cle AS (
  SELECT date_trunc('week', c.create_time) AS week_start, c.rule_id, c.rule_name,
    t.client_legal_entity_id AS cle_id, 1 AS alerts, 1 AS rfis, 0 AS rejects, 1 AS is_cv
  FROM cv_exploded c
  LEFT JOIN `risk-prod-sg`.dws_risk.risk_tm_real_time_transaction_results t
    ON t.transaction_id=c.txn_id
    AND t.transaction_created_time >= date_add(date_trunc('week', current_date()), -91)
    AND t.transaction_created_time < current_date()
),
combined AS (
  SELECT * FROM rt_rules UNION ALL SELECT * FROM cv_with_cle
),
weekly_agg AS (
  SELECT week_start, rule_id, MAX(rule_name) AS rule_name,
    SUM(alerts) AS alerts, SUM(rfis) AS rfis, SUM(rejects) AS rejects,
    COUNT(DISTINCT cle_id) AS clients, MAX(is_cv) AS is_cv
  FROM combined GROUP BY week_start, rule_id
),
ranked AS (
  SELECT rule_id, SUM(alerts) AS total_alerts
  FROM weekly_agg GROUP BY rule_id ORDER BY total_alerts DESC LIMIT 25
)
SELECT w.week_start, w.rule_id, w.rule_name, w.alerts, w.rfis, w.rejects, w.clients, w.is_cv
FROM weekly_agg w INNER JOIN ranked r ON r.rule_id=w.rule_id
ORDER BY w.week_start, w.alerts DESC
"""

MONTHLY_TREND_SQL = """
WITH test_accounts AS (
  SELECT DISTINCT account_id FROM `tp-prod-sg`.dim_account_service.testing_accounts
  WHERE account_id IS NOT NULL
),
sf_seg AS (
  SELECT client_legal_entity_id AS cle_id, MAX(sf_customer_segment) AS seg
  FROM `data-prod-sg`.dim_commercial.commercial_account
  WHERE client_legal_entity_id IS NOT NULL GROUP BY 1
),
base AS (
  SELECT
    date_add(DATE'2025-12-01', CAST(floor(datediff(date_trunc('week',t.transaction_created_time), DATE'2025-12-01')/28)*28 AS INT)) AS period_start,
    CASE WHEN s.seg LIKE '%Enterprise%' OR s.seg='T1 SME' THEN 'ENT_T1'
         WHEN s.seg IN ('T2 SME','T3 SME','T4 SME') THEN 'T24'
         ELSE 'OTH' END AS tier,
    t.if_alert, t.if_reject, t.final_transaction_amount_usd
  FROM `risk-prod-sg`.dws_risk.risk_tm_real_time_transaction_results t
  LEFT JOIN sf_seg s ON s.cle_id = t.client_legal_entity_id
  WHERE t.transaction_created_time >= DATE'2025-12-01'
    AND t.transaction_created_time < current_date()
    AND t.transaction_type IN ('DEPOSIT','PAYMENT','DIRECT_DEBIT')
    AND t.account_id NOT IN (SELECT account_id FROM test_accounts)
    AND t.final_transaction_amount_usd IS NOT NULL
)
SELECT period_start,
  SUM(CASE WHEN tier IN ('ENT_T1','T24','OTH') THEN 1 ELSE 0 END) AS txns,
  ROUND(100.0*SUM(CASE WHEN if_alert AND NOT if_reject THEN 1 ELSE 0 END)/COUNT(*),3) AS overall,
  ROUND(100.0*SUM(CASE WHEN tier='ENT_T1' AND if_alert AND NOT if_reject THEN 1 ELSE 0 END)
    /NULLIF(SUM(CASE WHEN tier='ENT_T1' THEN 1 ELSE 0 END),0),3) AS enterprise,
  ROUND(100.0*SUM(CASE WHEN tier='T24' AND if_alert AND NOT if_reject THEN 1 ELSE 0 END)
    /NULLIF(SUM(CASE WHEN tier='T24' THEN 1 ELSE 0 END),0),3) AS sme
FROM base
GROUP BY period_start
ORDER BY period_start
"""

FRAUD_YTD_SQL = """
WITH test_accounts AS (
  SELECT DISTINCT account_id FROM `tp-prod-sg`.dim_account_service.testing_accounts
  WHERE account_id IS NOT NULL
),
sf_seg AS (
  SELECT client_legal_entity_id AS cle_id, MAX(sf_customer_segment) AS seg
  FROM `data-prod-sg`.dim_commercial.commercial_account
  WHERE client_legal_entity_id IS NOT NULL GROUP BY 1
),
base AS (
  SELECT
    date_trunc('week', t.transaction_created_time) AS week_start,
    CASE WHEN s.seg LIKE '%Enterprise%' OR s.seg='T1 SME' THEN 'ENT_T1'
         WHEN s.seg IN ('T2 SME','T3 SME','T4 SME') THEN 'T24'
         ELSE 'OTH' END AS tier,
    t.final_transaction_amount_usd,
    t.uar_case_reviewed_as_fraudulent
  FROM `risk-prod-sg`.dws_risk.risk_tm_real_time_transaction_results t
  LEFT JOIN sf_seg s ON s.cle_id = t.client_legal_entity_id
  WHERE t.transaction_created_time >= DATE'2026-01-01'
    AND t.transaction_created_time < current_date()
    AND t.transaction_type IN ('DEPOSIT','PAYMENT','DIRECT_DEBIT')
    AND t.account_id NOT IN (SELECT account_id FROM test_accounts)
    AND t.final_transaction_amount_usd IS NOT NULL
)
SELECT week_start,
  ROUND(100.0*SUM(CASE WHEN uar_case_reviewed_as_fraudulent=1 THEN final_transaction_amount_usd ELSE 0 END)
    /NULLIF(SUM(final_transaction_amount_usd),0),4) AS fraud_rate,
  ROUND(100.0*SUM(CASE WHEN tier='ENT_T1' AND uar_case_reviewed_as_fraudulent=1 THEN final_transaction_amount_usd ELSE 0 END)
    /NULLIF(SUM(CASE WHEN tier='ENT_T1' THEN final_transaction_amount_usd ELSE 0 END),0),4) AS ent_fraud_rate,
  ROUND(100.0*SUM(CASE WHEN tier='T24' AND uar_case_reviewed_as_fraudulent=1 THEN final_transaction_amount_usd ELSE 0 END)
    /NULLIF(SUM(CASE WHEN tier='T24' THEN final_transaction_amount_usd ELSE 0 END),0),4) AS sme_fraud_rate
FROM base
GROUP BY week_start
ORDER BY week_start
"""

AMOUNT_BAND_SQL = """
WITH test_accounts AS (
  SELECT DISTINCT account_id FROM `tp-prod-sg`.dim_account_service.testing_accounts
  WHERE account_id IS NOT NULL
),
sf_seg AS (
  SELECT client_legal_entity_id AS cle_id, MAX(sf_customer_segment) AS seg
  FROM `data-prod-sg`.dim_commercial.commercial_account
  WHERE client_legal_entity_id IS NOT NULL GROUP BY 1
),
base AS (
  SELECT
    CASE WHEN t.final_transaction_amount_usd < 10000 THEN '0-10k'
         WHEN t.final_transaction_amount_usd < 100000 THEN '10k-100k'
         WHEN t.final_transaction_amount_usd < 1000000 THEN '100k-1m'
         ELSE '1m+' END AS band,
    CASE WHEN s.seg LIKE '%Enterprise%' THEN 'ENT'
         WHEN s.seg = 'T1 SME' THEN 'T1'
         WHEN s.seg IN ('T2 SME','T3 SME','T4 SME') THEN 'T24'
         ELSE 'OTH' END AS tier,
    t.if_alert, t.if_rfi, t.if_reject
  FROM `risk-prod-sg`.dws_risk.risk_tm_real_time_transaction_results t
  LEFT JOIN sf_seg s ON s.cle_id = t.client_legal_entity_id
  WHERE t.transaction_created_time >= date_trunc('month', date_add(current_date(), -90))
    AND t.transaction_created_time < current_date()
    AND t.transaction_type IN ('DEPOSIT','PAYMENT','DIRECT_DEBIT')
    AND t.account_id NOT IN (SELECT account_id FROM test_accounts)
    AND t.final_transaction_amount_usd IS NOT NULL
),
labelled AS (
  SELECT b.*, lbl.seg_label
  FROM base b
  LATERAL VIEW explode(array(
      CASE WHEN b.tier = 'ENT' THEN 'Enterprise' END,
      CASE WHEN b.tier IN ('ENT','T1') THEN 'Enterprise + T1 SME' END,
      CASE WHEN b.tier = 'T24' THEN 'T2-T4 SME' END,
      'TOTAL'
  )) lbl AS seg_label
  WHERE lbl.seg_label IS NOT NULL
)
SELECT seg_label, COALESCE(band,'ALL') AS band,
  COUNT(*) AS txns,
  SUM(CASE WHEN if_alert THEN 1 ELSE 0 END) AS alerts,
  SUM(CASE WHEN if_rfi THEN 1 ELSE 0 END) AS rfis,
  SUM(CASE WHEN if_reject THEN 1 ELSE 0 END) AS rejects,
  ROUND(100.0*SUM(CASE WHEN if_alert THEN 1 ELSE 0 END)/COUNT(*),3) AS alert_pct,
  ROUND(100.0*SUM(CASE WHEN if_rfi THEN 1 ELSE 0 END)/COUNT(*),3) AS rfi_pct,
  ROUND(100.0*SUM(CASE WHEN if_rfi THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN if_alert THEN 1 ELSE 0 END),0),1) AS alert_to_rfi,
  ROUND(100.0*SUM(CASE WHEN if_reject THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN if_rfi THEN 1 ELSE 0 END),0),1) AS rfi_to_reject,
  ROUND(100.0*SUM(CASE WHEN if_reject THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN if_alert THEN 1 ELSE 0 END),0),1) AS alert_to_reject,
  ROUND(100.0*SUM(CASE WHEN if_rfi OR if_reject THEN 1 ELSE 0 END)/NULLIF(SUM(CASE WHEN if_alert THEN 1 ELSE 0 END),0),1) AS alert_to_action
FROM labelled
GROUP BY seg_label, GROUPING SETS ((band), ())
ORDER BY seg_label, band
"""


# ── Data Assembly ────────────────────────────────────────────────────────

def build_weekly_data(core_rows, cv_rows):
    """Build weeklyAlert, weeklySegment, weeklyRfi, weeklyFraud from query results."""
    weeks = {}
    for r in core_rows:
        w = r["week_start"]
        seg = r["segment"]
        if w not in weeks:
            weeks[w] = {}
        weeks[w][seg] = r

    cv_by_week = {}
    for r in cv_rows:
        w = r["week_start"]
        seg = r["segment"]
        if w not in cv_by_week:
            cv_by_week[w] = {}
        cv_by_week[w][seg] = safe_int(r["cv_hits"])

    sorted_weeks = sorted(weeks.keys())

    weekly_alert = []
    weekly_segment = []
    weekly_rfi = []
    weekly_fraud = []

    for w in sorted_weeks:
        d = weeks[w]
        a = d.get("ALL", {})
        e = d.get("ENT_T1", {})
        s = d.get("T24", {})
        cv = cv_by_week.get(w, {})

        all_txns = safe_int(a.get("txns"))
        all_adj = safe_int(a.get("adj_alerts"))
        ent_txns = safe_int(e.get("txns"))
        ent_adj = safe_int(e.get("adj_alerts"))
        sme_txns = safe_int(s.get("txns"))
        sme_adj = safe_int(s.get("adj_alerts"))

        wk = fmt_week(w)

        weekly_alert.append({
            "week": wk,
            "overall": round(all_adj / all_txns * 100, 3) if all_txns else 0,
            "enterprise": round(ent_adj / ent_txns * 100, 3) if ent_txns else 0,
            "sme": round(sme_adj / sme_txns * 100, 3) if sme_txns else 0,
            "txns": all_txns,
            "alerts": all_adj,
            "rejects": safe_int(a.get("rejects")),
            "rejectAmt": safe_int(a.get("reject_amt")),
        })

        ent_cv = cv.get("ENT_T1", 0)
        sme_cv = cv.get("T24", 0)
        total_cv = sum(cv.values())

        weekly_segment.append({
            "week": wk,
            "entTxns": ent_txns,
            "entAlerts": safe_int(e.get("alerts")),
            "entRfis": safe_int(e.get("rfis")),
            "entRejects": safe_int(e.get("rejects")),
            "entRejectAmt": safe_int(e.get("reject_amt")),
            "smeTxns": sme_txns,
            "smeAlerts": safe_int(s.get("alerts")),
            "smeRfis": safe_int(s.get("rfis")),
            "smeRejects": safe_int(s.get("rejects")),
            "smeRejectAmt": safe_int(s.get("reject_amt")),
            "entOpsRfis": safe_int(e.get("rfis")),
            "entCvHits": ent_cv,
            "smeOpsRfis": safe_int(s.get("rfis")),
            "smeCvHits": sme_cv,
        })

        all_alerts_raw = safe_int(a.get("alerts"))
        all_rfis = safe_int(a.get("rfis"))
        weekly_rfi.append({
            "week": wk,
            "alerts": all_alerts_raw,
            "rfis": all_rfis,
            "rfiRate": round(all_rfis / all_alerts_raw * 100, 1) if all_alerts_raw else 0,
            "opsRfis": all_rfis,
            "cvHits": total_cv,
        })

        all_total_amt = safe_float(a.get("total_amt"))
        ent_total_amt = safe_float(e.get("total_amt"))
        sme_total_amt = safe_float(s.get("total_amt"))
        all_fraud_amt = safe_float(a.get("fraud_amt"))
        ent_fraud_amt = safe_float(e.get("fraud_amt"))
        sme_fraud_amt = safe_float(s.get("fraud_amt"))

        weekly_fraud.append({
            "week": wk,
            "fraudAmt": int(all_fraud_amt),
            "entFraudAmt": int(ent_fraud_amt),
            "smeFraudAmt": int(sme_fraud_amt),
            "depositUsd": int(all_total_amt),
            "entDepositUsd": int(ent_total_amt),
            "smeDepositUsd": int(sme_total_amt),
            "fraudRate": round(all_fraud_amt / all_total_amt * 100, 4) if all_total_amt else 0,
            "entFraudRate": round(ent_fraud_amt / ent_total_amt * 100, 4) if ent_total_amt else 0,
            "smeFraudRate": round(sme_fraud_amt / sme_total_amt * 100, 4) if sme_total_amt else 0,
        })

    return weekly_alert, weekly_segment, weekly_rfi, weekly_fraud


def build_monthly_trend(rows):
    result = []
    for r in rows:
        result.append({
            "period": fmt_week(r["period_start"]),
            "overall": safe_float(r["overall"]),
            "sme": safe_float(r["sme"]),
            "enterprise": safe_float(r["enterprise"]),
            "txns": safe_int(r["txns"]),
        })
    return result


def build_fraud_ytd(rows):
    result = []
    for r in rows:
        result.append({
            "week": fmt_week(r["week_start"]),
            "fraudRate": safe_float(r["fraud_rate"]),
            "entFraudRate": safe_float(r["ent_fraud_rate"]),
            "smeFraudRate": safe_float(r["sme_fraud_rate"]),
        })
    return result


def build_top_rules(rows):
    result = []
    for r in rows:
        result.append({
            "week": fmt_week(r["week_start"]),
            "ruleId": str(r["rule_id"]),
            "ruleName": r["rule_name"],
            "alerts": safe_int(r["alerts"]),
            "rfis": safe_int(r["rfis"]),
            "rejects": safe_int(r["rejects"]),
            "clients": safe_int(r["clients"]),
            "cv": safe_int(r["is_cv"]) == 1,
        })
    return result


def build_amount_band(rows):
    today = date.today()
    month_start = today.replace(day=1)
    quarter_start = month_start.replace(month=((month_start.month - 1) // 3) * 3 + 1)
    quarter_end_month = quarter_start.month + 2
    period_label = f"{quarter_start.strftime('%b')}–{today.strftime('%b %Y')}"

    seg_order = ["Enterprise", "Enterprise + T1 SME", "T2-T4 SME", "TOTAL"]
    band_order = ["0-10k", "10k-100k", "100k-1m", "1m+", "ALL"]
    seg_data = {}

    for r in rows:
        seg = r["seg_label"]
        band = r["band"]
        if seg not in seg_data:
            seg_data[seg] = {}
        seg_data[seg][band] = {
            "band": band,
            "alertPct": safe_float(r["alert_pct"]),
            "rfiPct": safe_float(r["rfi_pct"]),
            "txns": safe_int(r["txns"]),
            "alerts": safe_int(r["alerts"]),
            "rfis": safe_int(r["rfis"]),
            "rejects": safe_int(r["rejects"]),
            "alertToRfi": safe_float(r["alert_to_rfi"]),
            "rfiToReject": safe_float(r["rfi_to_reject"]),
            "alertToReject": safe_float(r["alert_to_reject"]),
            "alertToAction": safe_float(r["alert_to_action"]),
        }

    segments = []
    for seg_name in seg_order:
        if seg_name in seg_data:
            seg_rows = []
            for band in band_order:
                if band in seg_data[seg_name]:
                    seg_rows.append(seg_data[seg_name][band])
            segments.append({"name": seg_name, "rows": seg_rows})

    return {"period": period_label, "segments": segments}


# ── HTML Update ──────────────────────────────────────────────────────────

def js_val(v):
    """Convert Python value to JS literal string."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, str):
        return json.dumps(v)
    if v is None:
        return "null"
    raise ValueError(f"Unsupported type: {type(v)}")


def js_obj(d, keys=None):
    """Render dict as JS object literal { key: val, ... }"""
    if keys is None:
        keys = list(d.keys())
    parts = []
    for k in keys:
        v = d[k]
        if isinstance(v, bool):
            parts.append(f"{k}: {js_val(v)}")
        elif isinstance(v, (int, float)):
            parts.append(f"{k}: {v}")
        elif isinstance(v, str):
            parts.append(f"{k}: {json.dumps(v)}")
        elif v is None:
            parts.append(f"{k}: null")
        elif isinstance(v, list):
            parts.append(f"{k}: {js_array(v)}")
        elif isinstance(v, dict):
            parts.append(f"{k}: {js_obj(v)}")
    return "{ " + ", ".join(parts) + " }"


def js_array(lst, indent=4):
    """Render list as JS array with one object per line."""
    if not lst:
        return "[]"
    prefix = " " * indent
    lines = [prefix + js_obj(item) for item in lst]
    return "[\n" + ",\n".join(lines) + "\n  ]"


def extract_preserved(html):
    """Extract manually-curated sections from existing DATA block."""
    data_match = re.search(r'var DATA = \{(.+?)\n\};', html, re.DOTALL)
    if not data_match:
        return {}, {}, {}, {}

    block = data_match.group(1)

    def extract_section(name):
        pattern = rf'{name}:\s*(\{{[^}}]+\}}|\[.*?\])'
        m = re.search(pattern, block, re.DOTALL)
        return m.group(1) if m else None

    fraud_recall_raw = extract_section("fraudRecall")
    targets_raw = extract_section("targets")
    takeaways_raw = extract_section("takeaways")
    initiatives_raw = extract_section("nextInitiatives")

    return fraud_recall_raw, targets_raw, takeaways_raw, initiatives_raw


def build_data_block(monthly, weekly_alert, weekly_segment, weekly_rfi,
                     weekly_fraud, fraud_ytd, top_rules, amount_band,
                     fraud_recall_raw, targets_raw, takeaways_raw, initiatives_raw):
    """Assemble the complete var DATA = { ... }; block."""
    today = date.today().isoformat()

    lines = []
    lines.append(f'var DATA = {{')
    lines.append(f'  lastUpdated: "{today}",\n')

    lines.append(f'  monthlyTrend: {js_array(monthly)},\n')
    lines.append(f'  weeklyAlert: {js_array(weekly_alert)},\n')
    lines.append(f'  weeklySegment: {js_array(weekly_segment)},\n')
    lines.append(f'  weeklyRfi: {js_array(weekly_rfi)},\n')
    lines.append(f'  weeklyFraud: {js_array(weekly_fraud)},\n')
    lines.append(f'  fraudTrendFull: {js_array(fraud_ytd)},\n')

    lines.append(f'  fraudRecall: {fraud_recall_raw},\n')

    # Amount band
    ab = amount_band
    ab_lines = []
    ab_lines.append(f'  amountBand: {{')
    ab_lines.append(f'    period: {json.dumps(ab["period"])},')
    ab_lines.append(f'    segments: [')
    for seg in ab["segments"]:
        row_strs = [js_obj(r) for r in seg["rows"]]
        ab_lines.append(f'      {{ name: {json.dumps(seg["name"])}, rows: [')
        for rs in row_strs:
            ab_lines.append(f'        {rs},')
        ab_lines.append(f'      ]}},')
    ab_lines.append(f'    ]')
    ab_lines.append(f'  }},\n')
    lines.extend(ab_lines)

    lines.append(f'  targets: {targets_raw},\n')
    lines.append(f'  topRulesByWeek: {js_array(top_rules)},\n')
    lines.append(f'  takeaways: {takeaways_raw},\n')
    lines.append(f'  nextInitiatives: {initiatives_raw}')
    lines.append(f'}};')

    return "\n".join(lines)


def main():
    print(f"=== TM Dashboard Refresh — {date.today().isoformat()} ===\n")

    # Read existing HTML and extract preserved sections
    with open(HTML_PATH, "r") as f:
        html = f.read()

    fraud_recall_raw, targets_raw, takeaways_raw, initiatives_raw = extract_preserved(html)
    if not targets_raw:
        print("ERROR: Could not parse existing DATA block. Aborting.")
        sys.exit(1)

    # Run queries
    core_rows = run_query(WEEKLY_CORE_SQL, "weekly-core")
    cv_rows = run_query(CV_HITS_SQL, "cv-hits")
    top_rules_rows = run_query(TOP_RULES_SQL, "top-rules")
    monthly_rows = run_query(MONTHLY_TREND_SQL, "monthly-trend")
    fraud_ytd_rows = run_query(FRAUD_YTD_SQL, "fraud-ytd")
    amount_band_rows = run_query(AMOUNT_BAND_SQL, "amount-band")

    if not core_rows:
        print("ERROR: Weekly core query returned no data. Aborting.")
        sys.exit(1)

    # Build data structures
    weekly_alert, weekly_segment, weekly_rfi, weekly_fraud = build_weekly_data(core_rows, cv_rows)
    monthly_trend = build_monthly_trend(monthly_rows)
    fraud_ytd = build_fraud_ytd(fraud_ytd_rows)
    top_rules = build_top_rules(top_rules_rows)
    amount_band = build_amount_band(amount_band_rows)

    print(f"\nData assembled: {len(weekly_alert)} weeks, {len(monthly_trend)} months, "
          f"{len(top_rules)} rule-week rows, {len(fraud_ytd)} fraud-ytd weeks")

    # Build new DATA block
    new_data = build_data_block(
        monthly_trend, weekly_alert, weekly_segment, weekly_rfi,
        weekly_fraud, fraud_ytd, top_rules, amount_band,
        fraud_recall_raw, targets_raw, takeaways_raw, initiatives_raw,
    )

    # Replace DATA block in HTML
    new_html = re.sub(
        r'var DATA = \{.+?\n\};',
        lambda m: new_data,
        html,
        count=1,
        flags=re.DOTALL,
    )

    with open(HTML_PATH, "w") as f:
        f.write(new_html)

    print(f"\n✓ Updated {HTML_PATH}")
    print(f"  lastUpdated: {date.today().isoformat()}")
    print(f"  weeklyAlert: {len(weekly_alert)} weeks ({weekly_alert[0]['week']} – {weekly_alert[-1]['week']})")
    print(f"  topRules: {len(set(r['ruleId'] for r in top_rules))} unique rules")


if __name__ == "__main__":
    main()
