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

# Load .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

app = Flask(__name__)

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
GOOGLE_ADS_CID    = "9714507656"  # Suncrest
GOOGLE_ADS_LOGIN_CID = "2214006484"  # MCC that owns Suncrest account

ACCOUNTS = {
    "Suncrest": {
        "id": "act_810710292075125",
        "color": "orange",
        "badge": "SC",
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
    """Shared helper: call Anthropic API with TrueClicks MCP, return parsed JSON or None."""
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
        # Only attach MCP server when needed (Google Ads calls)
        if GOOGLE_ADS_MCP_URL and not system_override:
            payload["mcp_servers"] = [{"type": "url", "url": GOOGLE_ADS_MCP_URL, "name": "google-ads"}]
            payload["anthropic-beta"] = "mcp-client-2025-04-04"
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "mcp-client-2025-04-04",
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
    Fetches Google Ads data for Suncrest (9714507656):
      - Campaign metrics (ENABLED campaigns only)
      - Age + gender demographic breakdowns
    Returns a dict with 'campaigns', 'totals', 'age_rows', 'gender_rows', or None on failure.
    """
    if not ANTHROPIC_API_KEY or not GOOGLE_ADS_MCP_URL:
        print("[Google Ads] Skipped — ANTHROPIC_API_KEY or GOOGLE_ADS_MCP_URL not set.")
        return None

    # ── Query 1: Campaign metrics — ENABLED campaigns only ──
    camp_gaql = (
        f"SELECT campaign.name, metrics.cost_micros, metrics.conversions, "
        f"metrics.cost_per_conversion, metrics.clicks, metrics.impressions, metrics.ctr "
        f"FROM campaign "
        f"WHERE segments.date BETWEEN '{date_start}' AND '{date_end}' "
        f"AND campaign.status = 'ENABLED' "
        f"AND metrics.impressions > 0 "
        f"ORDER BY metrics.conversions DESC"
    )
    camp_prompt = (
        f"Use the Google Ads MCP tool to download a report for customer ID {GOOGLE_ADS_CID} with loginCustomerId {GOOGLE_ADS_LOGIN_CID}.\n\n"
        f"Run this GAQL query:\n{camp_gaql}\n\n"
        "Return ONLY a valid JSON object — no markdown, no backticks, no explanation — using this exact structure:\n"
        '{"campaigns":[{"name":"","spend":0.0,"conversions":0,"cpl":null,"clicks":0,"impressions":0,"ctr":0.0}],'
        '"totals":{"spend":0.0,"conversions":0,"cpl":null,"clicks":0,"impressions":0,"ctr":0.0}}\n\n'
        "Rules: divide cost_micros by 1000000 for spend. "
        "cpl = spend/conversions if conversions > 0 else null. "
        "ctr is a percentage value (e.g. 2.5 for 2.5%). "
        "Only include ENABLED campaigns."
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
    age_prompt = (
        f"Use the Google Ads MCP tool to download a report for customer ID {GOOGLE_ADS_CID} with loginCustomerId {GOOGLE_ADS_LOGIN_CID}.\n\n"
        f"Run this GAQL query:\n{age_gaql}\n\n"
        "Return ONLY a valid JSON array — no markdown, no backticks, no explanation — using this exact structure:\n"
        '[{"segment":"18-24","conversions":0,"spend":0.0,"impressions":0,"cpl":null}]\n\n'
        "Rules: divide cost_micros by 1000000 for spend. "
        "Map age_range.type to readable labels: AGE_RANGE_18_24 -> '18-24', AGE_RANGE_25_34 -> '25-34', "
        "AGE_RANGE_35_44 -> '35-44', AGE_RANGE_45_54 -> '45-54', AGE_RANGE_55_64 -> '55-64', "
        "AGE_RANGE_65_UP -> '65+', AGE_RANGE_UNDETERMINED -> 'Unknown'. "
        "Aggregate across all campaigns. cpl = spend/conversions if conversions > 0 else null."
    )

    # ── Query 3: Gender demographics ──
    gender_gaql = (
        f"SELECT ad_group_criterion.gender.type, metrics.cost_micros, metrics.conversions, "
        f"metrics.impressions, metrics.clicks "
        f"FROM gender_view "
        f"WHERE segments.date BETWEEN '{date_start}' AND '{date_end}' "
        f"AND campaign.status = 'ENABLED'"
    )
    gender_prompt = (
        f"Use the Google Ads MCP tool to download a report for customer ID {GOOGLE_ADS_CID} with loginCustomerId {GOOGLE_ADS_LOGIN_CID}.\n\n"
        f"Run this GAQL query:\n{gender_gaql}\n\n"
        "Return ONLY a valid JSON array — no markdown, no backticks, no explanation — using this exact structure:\n"
        '[{"segment":"Male","conversions":0,"spend":0.0,"impressions":0,"cpl":null,"pct":0.0}]\n\n'
        "Rules: divide cost_micros by 1000000 for spend. "
        "Map gender.type: MALE -> 'Male', FEMALE -> 'Female', UNDETERMINED -> 'Unknown'. "
        "Aggregate across all campaigns. cpl = spend/conversions if conversions > 0 else null. "
        "pct = conversions / total_conversions * 100 (rounded to 1 decimal). Exclude UNDETERMINED if 0 conversions."
    )

    # Fire all three queries
    camp_data    = _call_mcp(camp_prompt, max_tokens=3000, timeout=50)
    age_data     = _call_mcp(age_prompt,  max_tokens=2000, timeout=50)
    gender_data  = _call_mcp(gender_prompt, max_tokens=1500, timeout=45)

    if not camp_data:
        return None

    # Enrich campaign rows
    for c in camp_data.get("campaigns", []):
        c["spend_fmt"] = fmt_inr(c.get("spend"))
        cpl = c.get("cpl")
        c["cpl_fmt"]   = f"₹{int(cpl)}" if cpl else "—"
        c["cpl_color"] = cpl_color(cpl)
        c["ctr_fmt"]   = f"{c.get('ctr', 0):.2f}%"

    # Enrich totals
    t = camp_data.get("totals", {})
    t["spend_fmt"] = fmt_inr(t.get("spend"))
    tcpl = t.get("cpl")
    t["cpl_fmt"]   = f"₹{int(tcpl)}" if tcpl else "—"
    t["cpl_color"] = cpl_color(tcpl)
    t["ctr_fmt"]   = f"{t.get('ctr', 0):.2f}%"

    # Enrich age rows
    age_rows = []
    if isinstance(age_data, list):
        max_conv = max((r.get("conversions", 0) for r in age_data), default=1) or 1
        for r in age_data:
            if r.get("segment") == "Unknown" and r.get("conversions", 0) == 0:
                continue
            cpl_val = r.get("cpl")
            age_rows.append({
                "segment":    r.get("segment", ""),
                "conversions": r.get("conversions", 0),
                "spend_fmt":  fmt_inr(r.get("spend", 0)),
                "impr_fmt":   fmt_num(r.get("impressions", 0)),
                "cpl_fmt":    f"₹{int(cpl_val)}" if cpl_val else "—",
                "cpl_color":  cpl_color(cpl_val),
                "bar_pct":    int(r.get("conversions", 0) / max_conv * 100),
            })

    # Enrich gender rows
    gender_rows = []
    if isinstance(gender_data, list):
        for r in gender_data:
            if r.get("segment") == "Unknown":
                continue
            cpl_val = r.get("cpl")
            gender_rows.append({
                "segment":    r.get("segment", ""),
                "conversions": r.get("conversions", 0),
                "pct":        r.get("pct", 0),
                "spend_fmt":  fmt_inr(r.get("spend", 0)),
                "cpl_fmt":    f"₹{int(cpl_val)}" if cpl_val else "—",
                "cpl_color":  cpl_color(cpl_val),
            })

    camp_data["age_rows"]    = age_rows
    camp_data["gender_rows"] = gender_rows
    return camp_data


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
    """AI-generated targeting recommendations for Suncrest luxury villa/row house project."""
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
    system_prompt = """You are a senior performance marketing strategist specialising in luxury residential real estate in India.

PROJECT CONTEXT — SUNCREST by Navavedam Ventures:
- Type: Boutique luxury row houses — NOT apartments, NOT a large township
- Units: Only 40 exclusive homes — the scarcity and boutique nature is a key selling point
- Configuration: 4 BHK + Study, 3 floors, basement with 2 covered car parks
- Size: Saleable 3,566–3,590 sq.ft | Carpet 2,679 sq.ft
- Location: Behind Orion Uptown Mall, Old Madras Road, Bengaluru (Budigere Cross area)
- Theme: Earthy Scandinavian + biophilic design — natural materials, private backyard, central landscape walkway
- Clubhouse: 9,117 sq.ft exclusive clubhouse — temperature-controlled indoor pool, yoga studio, fitness centre, BBQ station, study & library, party hall
- Premium specs: Kohler bathrooms, rain showers, laminated wooden flooring in master bedroom, 8.5ft main door, home lift provision, EV charging provision, solar panel provision
- Sustainability: Rain water harvesting, sewage treatment plant, hydro-pneumatic water supply
- Developer: Navavedam Ventures (navavedamventures.com)
- Tagline: "Experience Soulful Living"

LOCATION ADVANTAGES (Old Madras Road / Budigere Cross):
- Brigade Signature Towers: 5 min | Bearys Global Research Triangle: 10 min
- ITPL / EPIP Zone: 35–40 min | Bagmane World Technology Centre: 30 min
- Kadugodi Metro: 20 min | Airport: 35 min
- Orion Uptown Mall: 5 min | Taj Vivanta: 25 min
- New Baldwin International School: 5 min | Vibgyor High: 5 min

BUYER PROFILES:
- Primary domestic buyer: IT/tech senior professional, aged 35–50, working at nearby tech corridors (Brigade, Bearys, ITPL), family of 4, seeking lifestyle upgrade from apartment to independent home with outdoor space
- Secondary domestic buyer: Established business owner / entrepreneur in East Bangalore, wants a legacy asset with privacy and nature
- NRI buyer: Indian diaspora in US or UK, buying for parents living in Bangalore OR as investment asset, attracted by the biophilic design and boutique exclusivity
- Key emotional triggers: "My kids need a backyard", "Done with apartment living", "Want something I can call mine", "Nature-connected lifestyle", "Only 40 homes — not a crowded township"

AD MESSAGING INTELLIGENCE:
- "SunCrest Blr Camp" = domestic Bangalore campaign targeting local IT professionals
- "NRI Campaign" = diaspora targeting in US/UK geographies
- The ₹914 CPL on SunCrest Blr Camp is strong for a 4 BHK at this price point
- NRI Campaign at ₹1,630 CPL is acceptable for international diaspora targeting but needs creative optimisation

YOUR TASK:
Analyse the provided campaign performance data and return ONLY a valid JSON object (no markdown, no backticks, no explanation) with this exact structure:

{
  "cross_channel": {
    "title": "string — one insight spanning Meta + Google",
    "body": "string — 2-3 sentences, specific to the data and SunCrest project",
    "type": "insight|warning|opportunity"
  },
  "meta_campaigns": [
    {
      "campaign": "campaign name",
      "headline": "one-line diagnosis",
      "recs": [
        {"label": "Urgent|High|Test|Preserve", "text": "specific actionable recommendation grounded in SunCrest project details"},
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
- Every recommendation must reference actual SunCrest USPs: 40 units exclusivity, private backyard, Earthy Scandinavian theme, Old Madras Road location, biophilic design, clubhouse amenities
- Reference actual numbers from the data (CPL, spend, leads, age segments, WoW deltas)
- For SunCrest Blr Camp: target IT professionals near Brigade/Bearys corridor, emphasise backyard + nature + independence from apartments, age 35–48
- For NRI Campaign: focus on US/UK diaspora, WhatsApp CTA preferred, emphasise boutique community (only 40 homes), investment angle (Old Madras Road appreciation), trust signals
- CPL benchmarks for this project: ₹800–1,200 acceptable, below ₹800 excellent, ₹1,200–1,800 needs attention, above ₹1,800 urgent
- Age 25–34 leads are likely pre-qualified curiosity — not the core buyer; flag if budget is heavy here
- Age 35–54 is the sweet spot for both domestic and NRI buyers
- If Google data is null, set google_summary.headline to null and google_summary.recs to []
- Never give generic digital marketing advice — every point must be specific to selling a 4 BHK boutique row house in Bangalore"""

    data_prompt = f"""Date range: {date_start} to {date_end}

META CAMPAIGNS:
{camp_summary}

AGE BREAKDOWN:
{age_summary}

GENDER BREAKDOWN:
{gender_summary}

GOOGLE ADS:
{gads_summary if gads_summary else "No Google Ads data available for this period."}

Generate targeting recommendations for the Suncrest luxury villa project based on this data."""

    result = _call_mcp(data_prompt, system_override=system_prompt, max_tokens=2000, timeout=55)
    if not result:
        return jsonify({"error": "AI analysis failed"}), 500

    return jsonify(result)

@app.route("/debug/google-ads")
def debug_google_ads():
    """Test Google Ads MCP connection in isolation."""
    date_start = request.args.get("date_start", "2026-04-03")
    date_end   = request.args.get("date_end",   "2026-04-09")
    html = "<pre style='background:#111;color:#eee;padding:20px;font-family:monospace'>"
    html += f"<b style='color:#F97316'>Google Ads MCP Debug</b>\n\n"
    html += f"CID: {GOOGLE_ADS_CID}\n"
    html += f"Login CID: {GOOGLE_ADS_LOGIN_CID}\n"
    html += f"MCP URL set: {bool(GOOGLE_ADS_MCP_URL)}\n"
    html += f"API Key set: {bool(ANTHROPIC_API_KEY)}\n"
    html += f"Date range: {date_start} → {date_end}\n\n"
    html += "Running campaign query...\n"
    try:
        result = get_google_ads_data(date_start, date_end)
        if result:
            html += f"<b style='color:#4ade80'>✅ SUCCESS</b>\n"
            html += f"Campaigns: {len(result.get('campaigns', []))}\n"
            html += f"Totals: {result.get('totals', {})}\n"
        else:
            html += f"<b style='color:#f56060'>❌ FAILED — returned None (check deploy logs for [MCP] lines)</b>\n"
    except Exception as e:
        html += f"<b style='color:#f56060'>❌ EXCEPTION: {e}</b>\n"
    html += f"\n<a href='/' style='color:#F97316'>← Back to Dashboard</a></pre>"
    return html

if __name__ == "__main__":
    app.run(debug=True, port=5050)
