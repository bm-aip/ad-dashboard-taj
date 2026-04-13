import os
import json
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, render_template, request, jsonify
from flask_caching import Cache
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.exceptions import FacebookRequestError
from extract_frames_endpoint import frames_bp

# Direct TrueClicks MCP SSE client (bypasses Anthropic API for raw data fetch)
import sys, os as _os
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from trueclicks_direct import call_trueclicks_gaql, _base_url

# Load .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)

from analytics import analytics_bp, init_analytics_db
init_analytics_db()
app.register_blueprint(analytics_bp)

# ─── CACHE CONFIG ──────────────────────────────────────────
# Cache API results for 15 minutes — avoids re-hitting Meta on every refresh
app.config["CACHE_TYPE"]             = "SimpleCache"
app.config["CACHE_DEFAULT_TIMEOUT"]  = 900  # 15 minutes
app.register_blueprint(frames_bp)
cache = Cache(app)

# ─── CONFIG ────────────────────────────────────────────────
ACCESS_TOKEN      = os.environ.get("META_ACCESS_TOKEN", "").strip()
APP_ID            = os.environ.get("META_APP_ID", "").strip()
APP_SECRET        = os.environ.get("META_APP_SECRET", "").strip()
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
GOOGLE_ADS_MCP_URL= os.environ.get("GOOGLE_ADS_MCP_URL", "").strip()
GOOGLE_ADS_CID    = "7478826652"  # Taj Sky View
GOOGLE_ADS_LOGIN_CID = "2214006484"  # MCC that owns Taj Sky View account

ACCOUNTS = {
    "Taj Sky View": {
        "id": "act_785911403473589",
        "color": "gold",
        "badge": "TS",
    },
}

INSIGHT_FIELDS = [
    "campaign_id",
    "campaign_name",
    "impressions",
    "reach",
    "clicks",
    "ctr",
    "spend",
    "actions",
    "cost_per_action_type",
    "date_start",
]

ADSET_INSIGHT_FIELDS = [
    "adset_id",
    "adset_name",
    "campaign_id",
    "campaign_name",
    "impressions",
    "reach",
    "clicks",
    "ctr",
    "spend",
    "actions",
    "cost_per_action_type",
]

AD_INSIGHT_FIELDS = [
    "ad_id",
    "ad_name",
    "adset_id",
    "adset_name",
    "campaign_id",
    "campaign_name",
    "impressions",
    "reach",
    "clicks",
    "ctr",
    "spend",
    "actions",
    "cost_per_action_type",
]


def init_api():
    FacebookAdsApi.init(access_token=ACCESS_TOKEN)


def extract_leads(actions):
    """Extract lead count from actions list."""
    if not actions:
        return 0
    for a in actions:
        if a.get("action_type") == "lead":
            return int(float(a.get("value", 0)))
    return 0


def extract_cpl(cost_per_action, lead_count):
    """Extract CPL from cost_per_action_type list."""
    if not cost_per_action or lead_count == 0:
        return None
    for a in cost_per_action:
        if a.get("action_type") == "lead":
            return round(float(a.get("value", 0)), 2)
    return None


def parse_insights(row, account_name, account_info, level="campaign"):
    """Parse a single insight row into a clean dict."""
    leads = extract_leads(row.get("actions", []))
    cpl   = extract_cpl(row.get("cost_per_action_type", []), leads)
    spend = round(float(row.get("spend", 0)), 2)
    impr  = int(row.get("impressions", 0))
    reach = int(row.get("reach", 0))
    clicks= int(row.get("clicks", 0))
    ctr   = round(float(row.get("ctr", 0)), 4)

    base = {
        "account_name": account_name,
        "account_color": account_info["color"],
        "account_badge": account_info["badge"],
        "impressions": impr,
        "reach": reach,
        "clicks": clicks,
        "ctr": ctr,
        "spend": spend,
        "leads": leads,
        "cpl": cpl,
    }

    if level == "campaign":
        base.update({
            "id": row.get("campaign_id"),
            "name": row.get("campaign_name"),
        })
    elif level == "adset":
        base.update({
            "id": row.get("adset_id"),
            "name": row.get("adset_name"),
            "campaign_id": row.get("campaign_id"),
            "campaign_name": row.get("campaign_name"),
        })
    elif level == "ad":
        base.update({
            "id": row.get("ad_id"),
            "name": row.get("ad_name"),
            "adset_id": row.get("adset_id"),
            "adset_name": row.get("adset_name"),
            "campaign_id": row.get("campaign_id"),
            "campaign_name": row.get("campaign_name"),
        })

    return base


def get_insights(date_start, date_end, level="campaign", campaign_id=None):
    """Fetch insights from all accounts for a date range."""
    if not ACCESS_TOKEN:
        raise ValueError("META_ACCESS_TOKEN is not set. Please add it to your .env file.")

    init_api()
    all_rows = []
    errors = []

    params = {
        "time_range": {"since": date_start, "until": date_end},
        "level": level,
        "attribution_setting": "7d_click,1d_view",
        "limit": 500,
    }

    if campaign_id:
        params["filtering"] = [{"field": "campaign.id", "operator": "IN", "value": [campaign_id]}]

    fields = {
        "campaign": INSIGHT_FIELDS,
        "adset":    ADSET_INSIGHT_FIELDS,
        "ad":       AD_INSIGHT_FIELDS,
    }[level]

    for acct_name, acct_info in ACCOUNTS.items():
        try:
            account = AdAccount(acct_info["id"])
            insights = account.get_insights(fields=fields, params=params)

            # Fetch campaign created times for "New" badge
            camp_created = {}
            if level == "campaign":
                campaigns_list = account.get_campaigns(fields=["id", "created_time"])
                for c in campaigns_list:
                    camp_created[c["id"]] = c.get("created_time", "")

            for row in insights:
                parsed = parse_insights(dict(row), acct_name, acct_info, level=level)
                # Attach is_new flag at campaign level
                if level == "campaign":
                    created_str = camp_created.get(parsed["id"], "")
                    if created_str:
                        try:
                            created_dt = datetime.strptime(created_str[:10], "%Y-%m-%d")
                            parsed["is_new"] = (datetime.today() - created_dt).days < 4
                        except Exception:
                            parsed["is_new"] = False
                    else:
                        parsed["is_new"] = False
                all_rows.append(parsed)
        except FacebookRequestError as e:
            msg = f"{acct_name}: {e.api_error_message()} (code {e.api_error_code()})"
            print(f"[Meta API Error] {msg}")
            errors.append(msg)
        except Exception as e:
            msg = f"{acct_name}: {str(e)}"
            print(f"[Error] {msg}")
            errors.append(msg)

    if not all_rows and errors:
        raise RuntimeError(" | ".join(errors))

    return all_rows


def compute_kpis(campaigns):
    """Compute summary KPIs from campaign list."""
    total_leads  = sum(c["leads"] for c in campaigns)
    total_spend  = sum(c["spend"] for c in campaigns)
    total_impr   = sum(c["impressions"] for c in campaigns)
    avg_cpl      = round(total_spend / total_leads, 2) if total_leads else None

    best = None
    for c in campaigns:
        if c["cpl"] is not None:
            if best is None or c["cpl"] < best["cpl"]:
                best = c

    gt_spend = sum(c["spend"] for c in campaigns if c["account_badge"] == "GT")
    bm_spend = sum(c["spend"] for c in campaigns if c["account_badge"] == "BM")

    return {
        "total_leads":  total_leads,
        "total_spend":  total_spend,
        "avg_cpl":      avg_cpl,
        "total_impr":   total_impr,
        "best_cpl":     best["cpl"] if best else None,
        "best_cpl_name": best["name"] if best else None,
        "best_cpl_badge": best["account_badge"] if best else None,
        "gt_spend":     gt_spend,
        "bm_spend":     bm_spend,
    }


def fmt_inr(val):
    if val is None:
        return "—"
    if val >= 100000:
        return f"₹{val/100000:.2f}L"
    if val >= 1000:
        return f"₹{val/1000:.1f}K"
    return f"₹{int(val)}"


def fmt_num(val):
    if val is None:
        return "—"
    if val >= 1_000_000:
        return f"{val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"{val/1_000:.1f}K"
    return str(int(val))


def cpl_color(cpl):
    if cpl is None:
        return "var(--muted)"
    if cpl < 300:
        return "var(--green)"
    if cpl < 600:
        return "var(--amber)"
    return "var(--red)"


def enrich(rows):
    """Add display helpers to each row."""
    for r in rows:
        r["cpl_color"]   = cpl_color(r["cpl"])
        r["cpl_fmt"]     = f"₹{int(r['cpl'])}" if r["cpl"] else "—"
        r["spend_fmt"]   = fmt_inr(r["spend"])
        r["impr_fmt"]    = fmt_num(r["impressions"])
        r["reach_fmt"]   = fmt_num(r["reach"])
        r["ctr_fmt"]     = f"{r['ctr']:.2f}%"
    return rows


def get_previous_period(date_start, date_end):
    """Return the same-length period immediately before date_start."""
    ds = datetime.strptime(date_start, "%Y-%m-%d")
    de = datetime.strptime(date_end,   "%Y-%m-%d")
    delta = de - ds
    prev_end   = ds - timedelta(days=1)
    prev_start = prev_end - delta
    return prev_start.strftime("%Y-%m-%d"), prev_end.strftime("%Y-%m-%d")


def get_breakdown_insights(date_start, date_end, breakdown):
    """Fetch insights with a breakdown (e.g. age, gender) across all accounts."""
    if not ACCESS_TOKEN:
        return []
    init_api()
    all_rows = []
    params = {
        "time_range": {"since": date_start, "until": date_end},
        "level": "account",
        "breakdowns": [breakdown],
        "limit": 500,
    }
    fields = ["impressions", "reach", "clicks", "spend", "actions", "cost_per_action_type"]
    for acct_name, acct_info in ACCOUNTS.items():
        try:
            account = AdAccount(acct_info["id"])
            insights = account.get_insights(fields=fields, params=params)
            for row in insights:
                r = dict(row)
                leads = extract_leads(r.get("actions", []))
                all_rows.append({
                    "account":     acct_name,
                    "badge":       acct_info["badge"],
                    "color":       acct_info["color"],
                    "segment":     r.get(breakdown, "unknown"),
                    "impressions": int(r.get("impressions", 0)),
                    "clicks":      int(r.get("clicks", 0)),
                    "spend":       round(float(r.get("spend", 0)), 2),
                    "leads":       leads,
                    "cpl":         extract_cpl(r.get("cost_per_action_type", []), leads),
                })
        except Exception as e:
            print(f"[Breakdown Error] {acct_name} ({breakdown}): {e}")
    return all_rows


def get_daily_leads(date_start, date_end):
    """Fetch day-by-day lead counts across all accounts, broken out by campaign."""
    if not ACCESS_TOKEN:
        return [], [], []
    init_api()
    # Aggregate by date across both accounts
    date_totals = {}   # date -> total leads
    camp_daily  = {}   # campaign_name -> {date -> leads}

    params = {
        "time_range": {"since": date_start, "until": date_end},
        "time_increment": 1,
        "level": "campaign",
        "attribution_setting": "7d_click,1d_view",
        "limit": 500,
    }
    fields = ["campaign_name", "date_start", "actions", "spend"]

    for acct_name, acct_info in ACCOUNTS.items():
        try:
            account = AdAccount(acct_info["id"])
            insights = account.get_insights(fields=fields, params=params)
            for row in insights:
                r = dict(row)
                date  = r.get("date_start", "")
                leads = extract_leads(r.get("actions", []))
                camp  = r.get("campaign_name", "Unknown")

                date_totals[date] = date_totals.get(date, 0) + leads
                if camp not in camp_daily:
                    camp_daily[camp] = {}
                camp_daily[camp][date] = camp_daily[camp].get(date, 0) + leads
        except Exception as e:
            print(f"[Daily leads error] {acct_name}: {e}")

    # Sort by date
    sorted_dates = sorted(date_totals.keys())
    daily_total  = [{"date": d, "leads": date_totals[d]} for d in sorted_dates]

    # Build per-campaign series (top 5 by total leads only)
    camp_totals = {c: sum(v.values()) for c, v in camp_daily.items()}
    top_camps   = sorted(camp_totals, key=lambda x: camp_totals[x], reverse=True)[:5]
    camp_series = []
    for camp in top_camps:
        camp_series.append({
            "name": camp,
            "data": [camp_daily[camp].get(d, 0) for d in sorted_dates],
        })

    return daily_total, camp_series, sorted_dates


def merge_wow(cw_list, pw_list):
    """Attach previous-period metrics to current-period campaign rows."""
    pw_map = {c["name"]: c for c in pw_list}
    for c in cw_list:
        pw = pw_map.get(c["name"])
        c["pw_leads"]    = pw["leads"]    if pw else None
        c["pw_spend"]    = pw["spend"]    if pw else None
        c["pw_cpl"]      = pw["cpl"]      if pw else None
        c["pw_cpl_fmt"]  = f"₹{int(pw['cpl'])}" if pw and pw["cpl"] else "—"
        c["pw_spend_fmt"]= fmt_inr(pw["spend"]) if pw else "—"
        if pw and pw["leads"]:
            delta = c["leads"] - pw["leads"]
            pct   = round(delta / pw["leads"] * 100, 1)
            c["wow_leads_delta"] = f"{'↑' if delta >= 0 else '↓'} {abs(pct)}%"
            c["wow_leads_up"]    = delta >= 0
        else:
            c["wow_leads_delta"] = "—"
            c["wow_leads_up"]    = None
        if pw and pw["cpl"] and c["cpl"]:
            delta = c["cpl"] - pw["cpl"]
            pct   = round(abs(delta) / pw["cpl"] * 100, 1)
            c["wow_cpl_delta"] = f"{'↑' if delta >= 0 else '↓'} {abs(pct)}%"
            c["wow_cpl_up"]    = delta < 0
        else:
            c["wow_cpl_delta"] = "—"
            c["wow_cpl_up"]    = None
    return cw_list


# ─── GOOGLE ADS (via Anthropic API + TrueClicks MCP) ───────

def _call_mcp(prompt, max_tokens=4000, timeout=50, system_override=None):
    """Anthropic API call for AI inference (targeting reco). No MCP — Google Ads uses direct SSE client."""
    if not ANTHROPIC_API_KEY:
        return None
    try:
        payload = {
            "model": "claude-sonnet-4-20250514",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system_override:
            payload["system"] = system_override
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )
        if resp.status_code != 200:
            print(f"[MCP] HTTP {resp.status_code}: {resp.text[:500]}")
            return None
        data = resp.json()
        # Log stop reason and content block types for debugging
        stop_reason = data.get("stop_reason", "unknown")
        block_types = [b.get("type") for b in data.get("content", [])]
        print(f"[MCP] stop_reason={stop_reason} blocks={block_types}")
        text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
        if not text:
            # Log full response if no text block found
            print(f"[MCP] No text block. Full response: {json.dumps(data)[:800]}")
            return None
        # Strip markdown fences
        if "```" in text:
            for part in text.split("```"):
                part = part.strip().lstrip("json").strip()
                if part.startswith(("{", "[")):
                    text = part
                    break
        return json.loads(text)
    except Exception as e:
        print(f"[MCP] Error: {e}")
        return None


def get_google_ads_data(date_start, date_end):
    """
    Fetches Google Ads data for Taj Sky View (CID 7478826652) via DIRECT TrueClicks SSE connection.
    Bypasses Anthropic API — calls TrueClicks MCP server directly for reliability.
    """
    if not GOOGLE_ADS_MCP_URL:
        print("[Google Ads] Skipped — GOOGLE_ADS_MCP_URL not set.")
        return None

    cid       = int(GOOGLE_ADS_CID)
    login_cid = int(GOOGLE_ADS_LOGIN_CID)

    # ── Query 1: Campaign metrics ──
    camp_gaql = (
        f"SELECT campaign.name, metrics.cost_micros, metrics.conversions, "
        f"metrics.cost_per_conversion, metrics.clicks, metrics.impressions, metrics.ctr "
        f"FROM campaign "
        f"WHERE segments.date BETWEEN '{date_start}' AND '{date_end}' "
        f"AND campaign.status = 'ENABLED' "
        f"AND metrics.impressions > 0 "
        f"ORDER BY metrics.conversions DESC"
    )

    # ── Query 2: Age demographics ──
    age_gaql = (
        f"SELECT ad_group_criterion.age_range.type, metrics.cost_micros, metrics.conversions, "
        f"metrics.impressions, metrics.clicks "
        f"FROM age_range_view "
        f"WHERE segments.date BETWEEN '{date_start}' AND '{date_end}' "
        f"AND campaign.status = 'ENABLED' "
        f"ORDER BY metrics.conversions DESC"
    )

    # ── Query 3: Gender demographics ──
    gender_gaql = (
        f"SELECT ad_group_criterion.gender.type, metrics.cost_micros, metrics.conversions, "
        f"metrics.impressions, metrics.clicks "
        f"FROM gender_view "
        f"WHERE segments.date BETWEEN '{date_start}' AND '{date_end}' "
        f"AND campaign.status = 'ENABLED'"
    )

    # Fire queries directly against TrueClicks SSE — no Anthropic intermediary
    print(f"[Google Ads Direct] Fetching campaigns for {date_start} → {date_end}")
    camp_rows  = call_trueclicks_gaql(GOOGLE_ADS_MCP_URL, cid, login_cid, camp_gaql,  timeout=30)
    age_rows_r = call_trueclicks_gaql(GOOGLE_ADS_MCP_URL, cid, login_cid, age_gaql,   timeout=30)
    gen_rows_r = call_trueclicks_gaql(GOOGLE_ADS_MCP_URL, cid, login_cid, gender_gaql, timeout=30)

    if not camp_rows:
        print("[Google Ads Direct] Campaign query returned None")
        return None

    print(f"[Google Ads Direct] Got {len(camp_rows)} campaign rows")

    # ── Parse campaign rows ──
    AGE_MAP = {
        "AGE_RANGE_18_24": "18-24", "AGE_RANGE_25_34": "25-34",
        "AGE_RANGE_35_44": "35-44", "AGE_RANGE_45_54": "45-54",
        "AGE_RANGE_55_64": "55-64", "AGE_RANGE_65_UP": "65+",
        "AGE_RANGE_UNDETERMINED": "Unknown",
    }
    GENDER_MAP = {
        "MALE": "Male", "FEMALE": "Female", "UNDETERMINED": "Unknown",
    }

    def micros_to_inr(val):
        try:
            return round(float(val) / 1_000_000, 2)
        except Exception:
            return 0.0

    def safe_float(val, default=0.0):
        try:
            return float(val)
        except Exception:
            return default

    def safe_int(val, default=0):
        try:
            return int(float(val))
        except Exception:
            return default

    campaigns = []
    total_spend = 0.0
    total_conv  = 0
    total_clicks = 0
    total_impr  = 0

    for row in (camp_rows if isinstance(camp_rows, list) else []):
        # TrueClicks returns nested dicts: row["campaign"]["name"], row["metrics"]["cost_micros"]
        camp  = row.get("campaign", {}) if isinstance(row, dict) else {}
        met   = row.get("metrics",  {}) if isinstance(row, dict) else {}
        name  = camp.get("name", "Unknown")
        spend = micros_to_inr(met.get("cost_micros") or met.get("costMicros", 0))
        conv  = safe_int(met.get("conversions", 0))
        clicks = safe_int(met.get("clicks", 0))
        impr  = safe_int(met.get("impressions", 0))
        ctr_raw = safe_float(met.get("ctr", 0))
        ctr   = ctr_raw * 100 if ctr_raw < 1 else ctr_raw  # handle both decimal and %
        cpl   = round(spend / conv, 2) if conv > 0 else None

        total_spend  += spend
        total_conv   += conv
        total_clicks += clicks
        total_impr   += impr

        campaigns.append({
            "name":       name,
            "spend":      spend,
            "spend_fmt":  fmt_inr(spend),
            "conversions": conv,
            "cpl":        cpl,
            "cpl_fmt":    f"₹{int(cpl)}" if cpl else "—",
            "cpl_color":  cpl_color(cpl),
            "clicks":     clicks,
            "impressions": impr,
            "ctr":        round(ctr, 2),
            "ctr_fmt":    f"{ctr:.2f}%",
        })

    total_cpl = round(total_spend / total_conv, 2) if total_conv > 0 else None
    totals = {
        "spend":      total_spend,
        "spend_fmt":  fmt_inr(total_spend),
        "conversions": total_conv,
        "cpl":        total_cpl,
        "cpl_fmt":    f"₹{int(total_cpl)}" if total_cpl else "—",
        "cpl_color":  cpl_color(total_cpl),
        "clicks":     total_clicks,
        "impressions": total_impr,
        "ctr":        round(total_clicks / total_impr * 100, 2) if total_impr > 0 else 0,
        "ctr_fmt":    f"{round(total_clicks / total_impr * 100, 2):.2f}%" if total_impr > 0 else "0.00%",
    }

    # ── Parse age rows ──
    age_agg = {}
    for row in (age_rows_r if isinstance(age_rows_r, list) else []):
        crit  = row.get("ad_group_criterion", row.get("adGroupCriterion", {})) if isinstance(row, dict) else {}
        met   = row.get("metrics",            {}) if isinstance(row, dict) else {}
        age_obj = crit.get("age_range", crit.get("ageRange", {})) if isinstance(crit, dict) else {}
        seg_raw = age_obj.get("type", "Unknown") if isinstance(age_obj, dict) else "Unknown"
        seg   = AGE_MAP.get(seg_raw, "Unknown")
        if seg not in age_agg:
            age_agg[seg] = {"conversions": 0, "spend": 0.0, "impressions": 0}
        age_agg[seg]["conversions"] += safe_int(met.get("conversions", 0))
        age_agg[seg]["spend"]       += micros_to_inr(met.get("cost_micros") or met.get("costMicros", 0))
        age_agg[seg]["impressions"] += safe_int(met.get("impressions", 0))

    max_conv_age = max((v["conversions"] for v in age_agg.values()), default=1) or 1
    age_rows = []
    for seg, v in sorted(age_agg.items()):
        if seg == "Unknown" and v["conversions"] == 0:
            continue
        cpl_val = round(v["spend"] / v["conversions"], 2) if v["conversions"] > 0 else None
        age_rows.append({
            "segment":     seg,
            "conversions": v["conversions"],
            "spend_fmt":   fmt_inr(v["spend"]),
            "impr_fmt":    fmt_num(v["impressions"]),
            "cpl_fmt":     f"₹{int(cpl_val)}" if cpl_val else "—",
            "cpl_color":   cpl_color(cpl_val),
            "bar_pct":     int(v["conversions"] / max_conv_age * 100),
        })

    # ── Parse gender rows ──
    gender_agg = {}
    for row in (gen_rows_r if isinstance(gen_rows_r, list) else []):
        crit  = row.get("ad_group_criterion", row.get("adGroupCriterion", {})) if isinstance(row, dict) else {}
        met   = row.get("metrics",            {}) if isinstance(row, dict) else {}
        gen_obj = crit.get("gender", {}) if isinstance(crit, dict) else {}
        seg_raw = gen_obj.get("type", "Unknown") if isinstance(gen_obj, dict) else "Unknown"
        seg   = GENDER_MAP.get(seg_raw, "Unknown")
        if seg == "Unknown":
            continue
        if seg not in gender_agg:
            gender_agg[seg] = {"conversions": 0, "spend": 0.0}
        gender_agg[seg]["conversions"] += safe_int(met.get("conversions", 0))
        gender_agg[seg]["spend"]       += micros_to_inr(met.get("cost_micros") or met.get("costMicros", 0))

    total_gender_conv = sum(v["conversions"] for v in gender_agg.values()) or 1
    gender_rows = []
    for seg, v in gender_agg.items():
        cpl_val = round(v["spend"] / v["conversions"], 2) if v["conversions"] > 0 else None
        gender_rows.append({
            "segment":     seg,
            "conversions": v["conversions"],
            "pct":         round(v["conversions"] / total_gender_conv * 100, 1),
            "spend_fmt":   fmt_inr(v["spend"]),
            "cpl_fmt":     f"₹{int(cpl_val)}" if cpl_val else "—",
            "cpl_color":   cpl_color(cpl_val),
        })

    return {
        "campaigns":   campaigns,
        "totals":      totals,
        "age_rows":    age_rows,
        "gender_rows": gender_rows,
    }


# ─── ROUTES ────────────────────────────────────────────────

@app.route("/")
@cache.cached(timeout=900, query_string=True)  # Cache per unique date range, 15 min
def index():
    today = datetime.today()
    date_end   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    date_start = (today - timedelta(days=7)).strftime("%Y-%m-%d")

    date_start = request.args.get("date_start", date_start)
    date_end   = request.args.get("date_end",   date_end)
    prev_start, prev_end = get_previous_period(date_start, date_end)

    errors = []
    campaigns      = []
    prev_campaigns = []
    age_data       = []
    gender_data    = []
    all_ads        = []
    all_adsets     = []
    daily_total    = []
    camp_series    = []
    daily_dates    = []

    # ── Parallel API fetches ──────────────────────────────────
    # All 6 independent calls fire simultaneously, cutting load
    # time from ~15s sequential to ~3-4s parallel.
    def fetch_campaigns():
        r = get_insights(date_start, date_end, level="campaign")
        r = enrich(r)
        r.sort(key=lambda x: x["leads"], reverse=True)
        return r

    def fetch_prev_campaigns():
        r = get_insights(prev_start, prev_end, level="campaign")
        r = enrich(r)
        return r

    def fetch_age():
        return get_breakdown_insights(date_start, date_end, "age")

    def fetch_gender():
        return get_breakdown_insights(date_start, date_end, "gender")

    def fetch_ads():
        r = get_insights(date_start, date_end, level="ad")
        r = enrich(r)
        r.sort(key=lambda x: x["leads"], reverse=True)
        return r

    def fetch_adsets():
        r = get_insights(date_start, date_end, level="adset")
        r = enrich(r)
        return r

    def fetch_daily():
        return get_daily_leads(date_start, date_end)

    def fetch_google_ads():
        return get_google_ads_data(date_start, date_end)

    tasks = {
        "campaigns":      fetch_campaigns,
        "prev_campaigns": fetch_prev_campaigns,
        "age":            fetch_age,
        "gender":         fetch_gender,
        "ads":            fetch_ads,
        "adsets":         fetch_adsets,
        "daily":          fetch_daily,
        "google_ads":     fetch_google_ads,
    }

    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        future_map = {executor.submit(fn): name for name, fn in tasks.items()}
        for future in as_completed(future_map):
            name = future_map[future]
            try:
                results[name] = future.result()
            except Exception as e:
                errors.append(f"{name.replace('_',' ').title()}: {e}")
                results[name] = None

    # Unpack results
    campaigns      = results.get("campaigns")      or []
    prev_campaigns = results.get("prev_campaigns") or []
    age_data       = results.get("age")            or []
    gender_data    = results.get("gender")         or []
    all_ads        = results.get("ads")            or []
    all_adsets     = results.get("adsets")         or []
    daily_result   = results.get("daily")
    if daily_result:
        daily_total, camp_series, daily_dates = daily_result
    else:
        daily_total, camp_series, daily_dates = [], [], []

    gads_data = results.get("google_ads") or None

    # ── Merge WoW (depends on both campaign fetches) ──────────
    try:
        campaigns = merge_wow(campaigns, prev_campaigns)
    except Exception as e:
        errors.append(f"Previous period: {e}")
        for c in campaigns:
            c.setdefault("pw_leads", None); c.setdefault("pw_cpl", None)
            c.setdefault("pw_spend", None); c.setdefault("pw_cpl_fmt", "—")
            c.setdefault("pw_spend_fmt", "—"); c.setdefault("wow_leads_delta", "—")
            c.setdefault("wow_leads_up", None); c.setdefault("wow_cpl_delta", "—")
            c.setdefault("wow_cpl_up", None)

    error = "; ".join(errors) if errors else None

    kpis = compute_kpis(campaigns)

    # Group adsets by campaign_id for appendix
    adsets_by_campaign = {}
    for a in all_adsets:
        cid = a.get("campaign_id", "")
        adsets_by_campaign.setdefault(cid, []).append(a)

    # Top / bottom by leads and CPL
    top_by_leads  = campaigns[:5]
    worst_by_leads = list(reversed(sorted(campaigns, key=lambda x: x["leads"])[:5]))
    cpl_ranked    = sorted([c for c in campaigns if c["cpl"]], key=lambda x: x["cpl"])
    best_by_cpl   = cpl_ranked[:5]
    worst_by_cpl  = list(reversed(cpl_ranked[-5:])) if cpl_ranked else []

    # Age breakdown — aggregate across accounts, sort by segment
    age_agg = {}
    for r in age_data:
        seg = r["segment"]
        if seg not in age_agg:
            age_agg[seg] = {"leads": 0, "spend": 0.0, "impressions": 0}
        age_agg[seg]["leads"]       += r["leads"]
        age_agg[seg]["spend"]       += r["spend"]
        age_agg[seg]["impressions"] += r["impressions"]
    age_rows = []
    for seg, v in sorted(age_agg.items()):
        cpl_val = round(v["spend"] / v["leads"], 2) if v["leads"] else None
        age_rows.append({
            "segment":  seg,
            "leads":    v["leads"],
            "spend":    v["spend"],
            "spend_fmt": fmt_inr(v["spend"]),
            "impressions": v["impressions"],
            "cpl":      cpl_val,
            "cpl_fmt":  f"₹{int(cpl_val)}" if cpl_val else "—",
            "cpl_color": cpl_color(cpl_val),
        })
    max_age_leads = max((r["leads"] for r in age_rows), default=1) or 1

    # Gender breakdown
    gender_agg = {}
    for r in gender_data:
        seg = r["segment"]
        if seg not in gender_agg:
            gender_agg[seg] = {"leads": 0, "spend": 0.0}
        gender_agg[seg]["leads"] += r["leads"]
        gender_agg[seg]["spend"] += r["spend"]
    gender_rows = []
    total_gender_leads = sum(v["leads"] for v in gender_agg.values()) or 1
    for seg, v in gender_agg.items():
        cpl_val = round(v["spend"] / v["leads"], 2) if v["leads"] else None
        gender_rows.append({
            "segment": seg.capitalize(),
            "leads":   v["leads"],
            "spend_fmt": fmt_inr(v["spend"]),
            "pct":     round(v["leads"] / total_gender_leads * 100, 1),
            "cpl_fmt": f"₹{int(cpl_val)}" if cpl_val else "—",
            "cpl_color": cpl_color(cpl_val),
        })

    # Creative analysis — top 5 and bottom 5 ads
    top_ads    = all_ads[:5]
    bottom_ads = [a for a in all_ads if a["cpl"]]
    bottom_ads = sorted(bottom_ads, key=lambda x: x["cpl"], reverse=True)[:5]

    # Previous period ad-level data for creative charts
    prev_ads_raw = []
    try:
        prev_ads_raw = get_insights(prev_start, prev_end, level="ad")
        prev_ads_raw = enrich(prev_ads_raw)
    except Exception:
        pass
    prev_ads_map = {a["name"]: a for a in prev_ads_raw}
    for a in top_ads:
        pa = prev_ads_map.get(a["name"])
        a["pw_leads"] = pa["leads"] if pa else 0
        a["pw_cpl"]   = (pa["cpl"] or 0) if pa else 0

    # KPI deltas vs previous period
    prev_kpis = compute_kpis(prev_campaigns)
    def pct_delta(curr, prev):
        if not prev or not curr:
            return None
        return round((curr - prev) / prev * 100, 1)
    kpi_deltas = {
        "leads": pct_delta(kpis["total_leads"], prev_kpis["total_leads"]),
        "spend": pct_delta(kpis["total_spend"], prev_kpis["total_spend"]),
        "avg_cpl": pct_delta(kpis.get("avg_cpl"), prev_kpis.get("avg_cpl")),
        "impr":  pct_delta(kpis["total_impr"],  prev_kpis["total_impr"]),
        "best_cpl": pct_delta(kpis.get("best_cpl"), prev_kpis.get("best_cpl")),
    }

    # Period label for WoW section title
    days = (datetime.strptime(date_end, "%Y-%m-%d") - datetime.strptime(date_start, "%Y-%m-%d")).days + 1
    if days == 1:
        period_label = "Day-on-Day"
    elif days == 7:
        period_label = "Week-on-Week"
    elif days == 14:
        period_label = "Fortnight-on-Fortnight"
    elif 28 <= days <= 31:
        period_label = "Month-on-Month"
    elif days <= 3:
        period_label = "Day-on-Day"
    else:
        period_label = "Period-on-Period"

    # WoW chart data
    wow_names    = [c["name"][:20] for c in campaigns]
    wow_cw_leads = [c["leads"]     for c in campaigns]
    wow_pw_leads = [c.get("pw_leads") or 0 for c in campaigns]
    wow_cw_cpl   = [c["cpl"] or 0  for c in campaigns]
    wow_pw_cpl   = [c.get("pw_cpl") or 0 for c in campaigns]

    return render_template(
        "dashboard.html",
        campaigns=campaigns,
        kpis=kpis,
        kpi_deltas=kpi_deltas,
        error=error,
        # Top / bottom
        top_by_leads=top_by_leads,
        worst_by_leads=worst_by_leads,
        best_by_cpl=best_by_cpl,
        worst_by_cpl=worst_by_cpl,
        # WoW
        prev_start=prev_start,
        prev_end=prev_end,
        period_label=period_label,
        wow_names=wow_names,
        wow_cw_leads=wow_cw_leads,
        wow_pw_leads=wow_pw_leads,
        wow_cw_cpl=wow_cw_cpl,
        wow_pw_cpl=wow_pw_cpl,
        # Targeting
        age_rows=age_rows,
        max_age_leads=max_age_leads,
        gender_rows=gender_rows,
        # Creative
        top_ads=top_ads,
        bottom_ads=bottom_ads,
        # Appendix
        adsets_by_campaign=adsets_by_campaign,
        # Dates
        date_start=date_start,
        date_end=date_end,
        fmt_inr=fmt_inr,
        fmt_num=fmt_num,
        today=today.strftime("%Y-%m-%d"),
        # Daily trend
        daily_dates=daily_dates,
        daily_total=daily_total,
        camp_series=camp_series,
        # Google Ads
        gads_data=gads_data,
    )


@app.route("/campaign/<campaign_id>")
def campaign_detail(campaign_id):
    date_start = request.args.get("date_start")
    date_end   = request.args.get("date_end")
    campaign_name = request.args.get("name", "Campaign Detail")

    if not date_start or not date_end:
        today      = datetime.today()
        date_end   = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        date_start = (today - timedelta(days=7)).strftime("%Y-%m-%d")

    # Ad Set level
    adsets = get_insights(date_start, date_end, level="adset", campaign_id=campaign_id)
    adsets = enrich(adsets)
    adsets.sort(key=lambda x: x["leads"], reverse=True)

    # Ad level
    ads = get_insights(date_start, date_end, level="ad", campaign_id=campaign_id)
    ads = enrich(ads)
    ads.sort(key=lambda x: x["leads"], reverse=True)

    kpis = compute_kpis(adsets)

    return render_template(
        "campaign.html",
        campaign_id=campaign_id,
        campaign_name=campaign_name,
        adsets=adsets,
        ads=ads,
        kpis=kpis,
        date_start=date_start,
        date_end=date_end,
        fmt_inr=fmt_inr,
        fmt_num=fmt_num,
    )


@app.route("/debug")
def debug():
    """Quick diagnostic page — visit this first if data isn't loading."""
    status = {
        "ACCESS_TOKEN_set": bool(ACCESS_TOKEN),
        "ACCESS_TOKEN_preview": ACCESS_TOKEN[:12] + "…" if ACCESS_TOKEN else "❌ NOT SET",
        "APP_ID_set": bool(APP_ID),
        "APP_SECRET_set": bool(APP_SECRET),
    }
    try:
        init_api()
        for acct_name, acct_info in ACCOUNTS.items():
            account = AdAccount(acct_info["id"])
            info = account.api_get(fields=["name", "account_status", "currency"])
            status[acct_name] = {
                "id": acct_info["id"],
                "name": info.get("name"),
                "status": info.get("account_status"),
                "currency": info.get("currency"),
                "connection": "✅ OK",
            }
    except Exception as e:
        status["api_error"] = str(e)

    html = "<pre style='background:#111;color:#eee;padding:30px;font-size:13px;line-height:1.8'>"
    html += "<b style='color:#F97316;font-size:16px'>Meta Ads API Debug</b>\n\n"
    for k, v in status.items():
        if isinstance(v, dict):
            html += f"<b style='color:#F97316'>{k}</b>\n"
            for kk, vv in v.items():
                html += f"  {kk}: {vv}\n"
        else:
            color = "#4ade80" if "✅" in str(v) or v is True else "#f56060" if ("❌" in str(v) or v is False) else "#eee"
            html += f"<span style='color:{color}'>{k}: {v}</span>\n"
    html += "\n<a href='/' style='color:#F97316'>← Back to Dashboard</a></pre>"
    return html



def api_summary():
    """JSON endpoint for AJAX refresh."""
    date_start = request.args.get("date_start")
    date_end   = request.args.get("date_end")
    if not date_start or not date_end:
        return jsonify({"error": "date_start and date_end required"}), 400

    campaigns = get_insights(date_start, date_end, level="campaign")
    campaigns = enrich(campaigns)
    campaigns.sort(key=lambda x: x["leads"], reverse=True)
    kpis = compute_kpis(campaigns)
    return jsonify({"campaigns": campaigns, "kpis": kpis})


@app.route("/api/campaign/<campaign_id>")
def api_campaign(campaign_id):
    date_start = request.args.get("date_start")
    date_end   = request.args.get("date_end")
    if not date_start or not date_end:
        return jsonify({"error": "date_start and date_end required"}), 400

    adsets = get_insights(date_start, date_end, level="adset", campaign_id=campaign_id)
    adsets = enrich(adsets)
    ads    = get_insights(date_start, date_end, level="ad",    campaign_id=campaign_id)
    ads    = enrich(ads)
    return jsonify({"adsets": adsets, "ads": ads})




@app.route("/api/targeting-reco")
def api_targeting_reco():
    """AI-generated targeting recommendations for Taj Sky View Hotel and Residences."""
    date_start = request.args.get("date_start")
    date_end   = request.args.get("date_end")
    if not date_start or not date_end:
        return jsonify({"error": "date_start and date_end required"}), 400

    # Fetch data needed for recommendations
    try:
        campaigns  = enrich(get_insights(date_start, date_end, level="campaign"))
        campaigns.sort(key=lambda x: x["leads"], reverse=True)
        age_data   = get_breakdown_insights(date_start, date_end, "age")
        gender_data = get_breakdown_insights(date_start, date_end, "gender")
        gads_data  = get_google_ads_data(date_start, date_end)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Build age summary
    age_agg = {}
    for r in age_data:
        seg = r["segment"]
        age_agg.setdefault(seg, {"leads": 0, "spend": 0.0})
        age_agg[seg]["leads"] += r["leads"]
        age_agg[seg]["spend"] += r["spend"]
    age_summary = []
    for seg, v in sorted(age_agg.items()):
        cpl = round(v["spend"] / v["leads"], 0) if v["leads"] else None
        age_summary.append({"age": seg, "leads": v["leads"], "cpl": cpl})

    # Build gender summary
    gender_agg = {}
    for r in gender_data:
        seg = r["segment"]
        gender_agg.setdefault(seg, {"leads": 0, "spend": 0.0})
        gender_agg[seg]["leads"] += r["leads"]
        gender_agg[seg]["spend"] += r["spend"]
    gender_summary = []
    for seg, v in gender_agg.items():
        cpl = round(v["spend"] / v["leads"], 0) if v["leads"] else None
        gender_summary.append({"gender": seg, "leads": v["leads"], "cpl": cpl})

    # Build campaign summary
    camp_summary = []
    for c in campaigns:
        camp_summary.append({
            "name": c.get("name", ""),
            "leads": c.get("leads", 0),
            "spend": c.get("spend", 0),
            "cpl": c.get("cpl"),
            "impressions": c.get("impressions", 0),
            "ctr": c.get("ctr", 0),
            "wow_leads_delta": c.get("wow_leads_delta", "—"),
            "wow_cpl_delta": c.get("wow_cpl_delta", "—"),
        })

    # Build Google Ads summary if available
    gads_summary = None
    if gads_data:
        gads_summary = {
            "campaigns": [
                {"name": c.get("name"), "spend": c.get("spend"), "conversions": c.get("conversions"), "cpl": c.get("cpl")}
                for c in gads_data.get("campaigns", [])
            ],
            "totals": gads_data.get("totals", {})
        }

    # Build prompt for Claude
    system_prompt = """You are a Meta Ads targeting strategist for Taj Sky View Hotel and Residences, Chennai — the first-ever branded residences by Taj, developed by the Ampa Group. Location: Nelson Manickam Road, Aminjikarai, Chennai CBD (among the tallest hotel + residential towers in the city's CBD). Asset class: ultra-luxury branded condo-hotels and serviced residences — a brand-new category in India. Inventory: 3 BHK apartments (1,555–2,623 sqft RERA carpet) and 4 BHK apartments (2,582–3,699 sqft RERA carpet) across Tower 1 and Tower 2, plus boutique offices. Price segment: ultra-luxury (ticket size ₹8Cr+). USPs: Taj-branded hospitality (dedicated butler, professional housekeeping, global gourmet F&B, highly trained security), half-Olympic podium swimming pool, rooftop waterscape, Taj Spectre theatre, 3,500 sqft club lounge, 6,500 sqft fitness centre, bowling alley, virtual golf simulator, squash/tennis/air-conditioned badminton/futsal/half-basketball courts, 100m high garden, J Wellness Circle, steam & sauna, 11'6"–14'6" ceiling heights, marble-clad toilets, chiller AC, double-glazed heat-reduction windows, HT power, private lift lobby, bed-sized medical evacuation lift. RERA: TN/029 Building/0303/2023. Target audience: (a) NRIs in US, UK, Singapore, UAE, Australia seeking a trophy Indian asset with Taj-branded hospitality; (b) Indian UHNIs and HNIs with ₹5Cr+ investable net worth — senior corporate executives, promoters, business owners, legacy real estate investors, C-suite professionals; (c) Ultra-wealthy Indians looking at branded residences as a new asset class. NRI angle is strong and should be prioritized in geo/interest targeting recommendations. When generating targeting recommendations, lean into luxury-brand affinities (Taj, Oberoi, Ritz-Carlton, private banking, luxury automotive, fine dining, business class travel), high-income job titles, and NRI diaspora geo-targeting.

YOUR TASK:
Analyse the provided campaign performance data and return ONLY a valid JSON object (no markdown, no backticks, no explanation) with this exact structure:

{
  "cross_channel": {
    "title": "string — one insight spanning Meta + Google",
    "body": "string — 2-3 sentences, specific to the data and Taj Sky View project",
    "type": "insight|warning|opportunity"
  },
  "meta_campaigns": [
    {
      "campaign": "campaign name",
      "headline": "one-line diagnosis",
      "recs": [
        {"label": "Urgent|High|Test|Preserve", "text": "specific actionable recommendation grounded in Taj Sky View project details"},
        {"label": "High", "text": "..."},
        {"label": "Test", "text": "..."}
      ]
    }
  ],
  "google_summary": {
    "headline": "one-line Google Ads diagnosis or null if no data",
    "recs": [
      {"label": "Urgent|High|Test", "text": "recommendation"}
    ]
  }
}

RULES:
- Every recommendation must reference actual Taj Sky View USPs: Taj-branded hospitality, Chennai CBD location, ultra-luxury amenities, NRI appeal, branded residences as new asset class
- Reference actual numbers from the data (CPL, spend, leads, age segments, WoW deltas)
- Lean into luxury-brand affinities and NRI diaspora geo-targeting
- If Google data is null, set google_summary.headline to null and google_summary.recs to []
- Never give generic digital marketing advice — every point must be specific to selling ultra-luxury branded residences in Chennai"""

    data_prompt = f"""Date range: {date_start} to {date_end}

META CAMPAIGNS:
{camp_summary}

AGE BREAKDOWN:
{age_summary}

GENDER BREAKDOWN:
{gender_summary}

GOOGLE ADS:
{gads_summary if gads_summary else "No Google Ads data available for this period."}

Generate targeting recommendations for the Taj Sky View ultra-luxury branded residences project based on this data."""

    result = _call_mcp(data_prompt, system_override=system_prompt, max_tokens=2000, timeout=55)
    if not result:
        return jsonify({"error": "AI analysis failed"}), 500

    return jsonify(result)

@app.route("/debug/google-ads")
def debug_google_ads():
    """Step-by-step Google Ads SSE connection test — results shown inline."""
    import threading, queue as q_mod, time

    date_start = request.args.get("date_start", "2026-04-03")
    date_end   = request.args.get("date_end",   "2026-04-09")

    steps = []
    ok    = "<b style='color:#4ade80'>✅</b>"
    fail  = "<b style='color:#f56060'>❌</b>"

    steps.append(f"CID: {GOOGLE_ADS_CID}")
    steps.append(f"Login CID: {GOOGLE_ADS_LOGIN_CID}")
    steps.append(f"MCP URL set: {bool(GOOGLE_ADS_MCP_URL)} {'  ' + ok if GOOGLE_ADS_MCP_URL else '  ' + fail}")
    steps.append(f"API Key set: {bool(ANTHROPIC_API_KEY)}")
    steps.append(f"Date range: {date_start} → {date_end}")
    steps.append("")

    if not GOOGLE_ADS_MCP_URL:
        steps.append(f"{fail} GOOGLE_ADS_MCP_URL not set in Railway Variables")
        html = "<pre style='background:#111;color:#eee;padding:20px;font-family:monospace'>"
        html += f"<b style='color:#F97316'>Google Ads Direct SSE Debug</b>\n\n"
        html += "\n".join(steps)
        html += f"\n\n<a href='/' style='color:#F97316'>← Back</a></pre>"
        return html

    # Step 1: Can we reach the MCP URL at all?
    steps.append("Step 1 — Reaching TrueClicks MCP URL...")
    try:
        probe = requests.get(
            GOOGLE_ADS_MCP_URL,
            stream=True,
            headers={"Accept": "text/event-stream"},
            timeout=8,
        )
        steps.append(f"  HTTP status: {probe.status_code}")
        if probe.status_code == 200:
            steps.append(f"  {ok} Connected — reading first 10 SSE lines...")
            lines_read = 0
            endpoint_uri = None
            for raw in probe.iter_lines():
                if lines_read >= 10:
                    break
                lines_read += 1
                line = raw.decode("utf-8") if isinstance(raw, bytes) else (raw or "")
                steps.append(f"  Line {lines_read}: {repr(line[:150])}")
                if line.startswith("data:"):
                    try:
                        data = json.loads(line[5:].strip())
                        if "uri" in data:
                            endpoint_uri = data.get("uri")
                    except Exception:
                        pass
            probe.close()
            if endpoint_uri:
                steps.append(f"  {ok} Found endpoint URI: {endpoint_uri[:80]}")
            else:
                steps.append(f"  ⚠️  No endpoint URI found in first {lines_read} lines")
        else:
            steps.append(f"  {fail} HTTP {probe.status_code}: {probe.text[:200]}")
    except Exception as exc:
        steps.append(f"  {fail} Connection error: {exc}")

    steps.append("")

    # Step 2: Full GAQL query via direct SSE client
    steps.append("Step 2 — Running campaign query via direct SSE client...")
    import time as _time
    t0 = _time.time()
    gaql = (
        f"SELECT campaign.name, metrics.cost_micros, metrics.conversions, "
        f"metrics.clicks, metrics.impressions FROM campaign "
        f"WHERE segments.date BETWEEN '{date_start}' AND '{date_end}' "
        f"AND metrics.impressions > 0 LIMIT 5"
    )
    try:
        rows = call_trueclicks_gaql(
            GOOGLE_ADS_MCP_URL, int(GOOGLE_ADS_CID), int(GOOGLE_ADS_LOGIN_CID),
            gaql, timeout=30
        )
        elapsed = round(_time.time() - t0, 1)
        if rows is None:
            steps.append(f"  {fail} Returned None after {elapsed}s — check Railway deploy logs for [TrueClicks] lines")
        elif isinstance(rows, list):
            steps.append(f"  {ok} Got {len(rows)} rows in {elapsed}s")
            if rows:
                first = rows[0]
                steps.append(f"  Sample keys: {list(first.keys()) if isinstance(first, dict) else type(first)}")
                steps.append(f"  First row: {json.dumps(first)[:300]}")
        else:
            steps.append(f"  ⚠️ Unexpected: {type(rows)} → {str(rows)[:200]}")
    except Exception as exc:
        elapsed = round(_time.time() - t0, 1)
        steps.append(f"  {fail} Exception after {elapsed}s: {exc}")

    steps.append("")
    steps.append("<a href='/' style='color:#F97316'>← Back to Dashboard</a>")

    html = "<pre style='background:#111;color:#eee;padding:20px;font-family:monospace;line-height:1.7'>"
    html += f"<b style='color:#F97316;font-size:15px'>Google Ads Direct SSE Debug</b>\n\n"
    html += "\n".join(steps)
    html += "</pre>"
    return html

if __name__ == "__main__":
    app.run(debug=True, port=5050)
