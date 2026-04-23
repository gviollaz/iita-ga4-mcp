"""
IITA Google Analytics 4 MCP Server
Provides GA4 reporting data via MCP protocol.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Optional, List

from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field, ConfigDict

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest, RunRealtimeReportRequest, DateRange,
    Dimension, Metric, OrderBy, FilterExpression, Filter,
)
from google.oauth2.credentials import Credentials

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("iita-ga4-mcp")

PORT = int(os.environ.get("PORT", 8080))
PROPERTY_ID = os.environ.get("GA4_PROPERTY_ID", "")
CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")
TOKEN_URI = "https://oauth2.googleapis.com/token"

def _get_client():
    creds = Credentials(token=None, refresh_token=REFRESH_TOKEN, client_id=CLIENT_ID,
                        client_secret=CLIENT_SECRET, token_uri=TOKEN_URI)
    return BetaAnalyticsDataClient(credentials=creds)

def _resolve_dates(date_range, start_date, end_date):
    if start_date and end_date: return start_date, end_date
    presets = {"TODAY":(0,0),"YESTERDAY":(1,1),"LAST_7_DAYS":(7,0),"LAST_14_DAYS":(14,0),
               "LAST_28_DAYS":(28,0),"LAST_30_DAYS":(30,0),"LAST_90_DAYS":(90,0),"THIS_MONTH":("month",0)}
    today = datetime.now().date()
    if date_range in presets:
        val = presets[date_range]
        s = today.replace(day=1) if val[0]=="month" else today - timedelta(days=val[0])
        e = today if val[1]==0 else today - timedelta(days=val[1])
        return s.isoformat(), e.isoformat()
    return (today - timedelta(days=28)).isoformat(), today.isoformat()

def _format_report(response, dims_list, metrics_list):
    if not response.rows: return "No data found."
    headers = dims_list + metrics_list
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"]*len(headers)) + " |"]
    for row in response.rows:
        vals = [dv.value for dv in row.dimension_values] + [mv.value for mv in row.metric_values]
        lines.append("| " + " | ".join(vals) + " |")
    lines.append(f"\n**Rows**: {len(response.rows)} | **Row count**: {response.row_count}")
    return "\n".join(lines)

mcp = FastMCP(
    "iita_ga4_mcp",
    host="0.0.0.0",
    port=PORT,
    stateless_http=True,
    json_response=False,
)

class RunReportInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dimensions: List[str] = Field(default=["pagePath"], description="GA4 dimensions: pagePath, pageTitle, sessionSource, sessionMedium, country, city, deviceCategory, date, hour, landingPage, sessionDefaultChannelGroup")
    metrics: List[str] = Field(default=["screenPageViews","sessions","totalUsers"], description="GA4 metrics: screenPageViews, sessions, totalUsers, newUsers, bounceRate, averageSessionDuration, engagedSessions, conversions, eventCount")
    date_range: str = Field(default="LAST_28_DAYS")
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    limit: int = Field(default=25, ge=1, le=1000)
    order_by_metric: Optional[str] = Field(default=None)
    dimension_filter_name: Optional[str] = Field(default=None)
    dimension_filter_value: Optional[str] = Field(default=None)
    property_id: Optional[str] = Field(default=None)

@mcp.tool(name="ga4_run_report", annotations={"readOnlyHint":True,"destructiveHint":False,"idempotentHint":True})
async def ga4_run_report(params: RunReportInput) -> str:
    """Run a GA4 report with custom dimensions, metrics, date range, and filters."""
    prop = params.property_id or PROPERTY_ID
    if not prop: return "Error: No GA4 property ID configured."
    sd, ed = _resolve_dates(params.date_range, params.start_date, params.end_date)
    order_metric = params.order_by_metric or params.metrics[0]
    request = RunReportRequest(property=f"properties/{prop}", dimensions=[Dimension(name=d) for d in params.dimensions],
        metrics=[Metric(name=m) for m in params.metrics], date_ranges=[DateRange(start_date=sd, end_date=ed)],
        limit=params.limit, order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name=order_metric), desc=True)])
    if params.dimension_filter_name and params.dimension_filter_value:
        request.dimension_filter = FilterExpression(filter=Filter(field_name=params.dimension_filter_name,
            string_filter=Filter.StringFilter(match_type=Filter.StringFilter.MatchType.CONTAINS, value=params.dimension_filter_value, case_sensitive=False)))
    return f"### GA4 Report -- {sd} to {ed}\n**Property**: {prop}\n\n" + _format_report(_get_client().run_report(request), params.dimensions, params.metrics)

class RealtimeReportInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dimensions: List[str] = Field(default=["unifiedScreenName"])
    metrics: List[str] = Field(default=["activeUsers"])
    limit: int = Field(default=20, ge=1, le=100)
    property_id: Optional[str] = Field(default=None)

@mcp.tool(name="ga4_realtime_report", annotations={"readOnlyHint":True,"destructiveHint":False,"idempotentHint":True})
async def ga4_realtime_report(params: RealtimeReportInput) -> str:
    """Get real-time GA4 data showing active users and pages right now."""
    prop = params.property_id or PROPERTY_ID
    if not prop: return "Error: No GA4 property ID configured."
    request = RunRealtimeReportRequest(property=f"properties/{prop}", dimensions=[Dimension(name=d) for d in params.dimensions],
        metrics=[Metric(name=m) for m in params.metrics], limit=params.limit)
    return f"### GA4 Realtime\n" + _format_report(_get_client().run_realtime_report(request), params.dimensions, params.metrics)

class TrafficSourcesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    date_range: str = Field(default="LAST_28_DAYS")
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    limit: int = Field(default=20, ge=1, le=100)
    property_id: Optional[str] = Field(default=None)

@mcp.tool(name="ga4_traffic_sources", annotations={"readOnlyHint":True,"destructiveHint":False,"idempotentHint":True})
async def ga4_traffic_sources(params: TrafficSourcesInput) -> str:
    """Get traffic sources: channels, sources, mediums with session/user counts."""
    prop = params.property_id or PROPERTY_ID
    sd, ed = _resolve_dates(params.date_range, params.start_date, params.end_date)
    dims = ["sessionDefaultChannelGroup","sessionSource","sessionMedium"]
    mets = ["sessions","totalUsers","newUsers","bounceRate"]
    request = RunReportRequest(property=f"properties/{prop}", dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in mets], date_ranges=[DateRange(start_date=sd, end_date=ed)],
        limit=params.limit, order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)])
    return f"### GA4 Traffic Sources -- {sd} to {ed}\n\n" + _format_report(_get_client().run_report(request), dims, mets)

class TopPagesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    date_range: str = Field(default="LAST_28_DAYS")
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    limit: int = Field(default=20, ge=1, le=100)
    path_contains: Optional[str] = Field(default=None)
    property_id: Optional[str] = Field(default=None)

@mcp.tool(name="ga4_top_pages", annotations={"readOnlyHint":True,"destructiveHint":False,"idempotentHint":True})
async def ga4_top_pages(params: TopPagesInput) -> str:
    """Get top pages by views with engagement metrics."""
    prop = params.property_id or PROPERTY_ID
    sd, ed = _resolve_dates(params.date_range, params.start_date, params.end_date)
    dims = ["pagePath","pageTitle"]
    mets = ["screenPageViews","sessions","totalUsers","averageSessionDuration","bounceRate"]
    request = RunReportRequest(property=f"properties/{prop}", dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in mets], date_ranges=[DateRange(start_date=sd, end_date=ed)],
        limit=params.limit, order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="screenPageViews"), desc=True)])
    if params.path_contains:
        request.dimension_filter = FilterExpression(filter=Filter(field_name="pagePath",
            string_filter=Filter.StringFilter(match_type=Filter.StringFilter.MatchType.CONTAINS, value=params.path_contains, case_sensitive=False)))
    return f"### GA4 Top Pages -- {sd} to {ed}\n\n" + _format_report(_get_client().run_report(request), dims, mets)

class DailyTrendInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    metrics: List[str] = Field(default=["sessions","totalUsers","screenPageViews"])
    date_range: str = Field(default="LAST_28_DAYS")
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    property_id: Optional[str] = Field(default=None)

@mcp.tool(name="ga4_daily_trend", annotations={"readOnlyHint":True,"destructiveHint":False,"idempotentHint":True})
async def ga4_daily_trend(params: DailyTrendInput) -> str:
    """Get daily trend of sessions, users, and pageviews."""
    prop = params.property_id or PROPERTY_ID
    sd, ed = _resolve_dates(params.date_range, params.start_date, params.end_date)
    request = RunReportRequest(property=f"properties/{prop}", dimensions=[Dimension(name="date")],
        metrics=[Metric(name=m) for m in params.metrics], date_ranges=[DateRange(start_date=sd, end_date=ed)],
        limit=366, order_bys=[OrderBy(dimension=OrderBy.DimensionOrderBy(dimension_name="date"), desc=False)])
    return f"### GA4 Daily Trend -- {sd} to {ed}\n\n" + _format_report(_get_client().run_report(request), ["date"], params.metrics)

class LandingPagesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    date_range: str = Field(default="LAST_28_DAYS")
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    limit: int = Field(default=20, ge=1, le=100)
    property_id: Optional[str] = Field(default=None)

@mcp.tool(name="ga4_landing_pages", annotations={"readOnlyHint":True,"destructiveHint":False,"idempotentHint":True})
async def ga4_landing_pages(params: LandingPagesInput) -> str:
    """Get landing page performance: first-touch pages with sessions, bounce rate, conversions."""
    prop = params.property_id or PROPERTY_ID
    sd, ed = _resolve_dates(params.date_range, params.start_date, params.end_date)
    dims = ["landingPage"]
    mets = ["sessions","totalUsers","newUsers","bounceRate","averageSessionDuration","conversions"]
    request = RunReportRequest(property=f"properties/{prop}", dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in mets], date_ranges=[DateRange(start_date=sd, end_date=ed)],
        limit=params.limit, order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)])
    return f"### GA4 Landing Pages -- {sd} to {ed}\n\n" + _format_report(_get_client().run_report(request), dims, mets)

class DeviceGeoInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    breakdown: str = Field(default="device", description="'device', 'country', or 'city'")
    date_range: str = Field(default="LAST_28_DAYS")
    start_date: Optional[str] = Field(default=None)
    end_date: Optional[str] = Field(default=None)
    limit: int = Field(default=20, ge=1, le=100)
    property_id: Optional[str] = Field(default=None)

@mcp.tool(name="ga4_device_geo", annotations={"readOnlyHint":True,"destructiveHint":False,"idempotentHint":True})
async def ga4_device_geo(params: DeviceGeoInput) -> str:
    """Get traffic breakdown by device or geography."""
    prop = params.property_id or PROPERTY_ID
    sd, ed = _resolve_dates(params.date_range, params.start_date, params.end_date)
    dim_map = {"device":"deviceCategory","country":"country","city":"city"}
    dim_name = dim_map.get(params.breakdown, "deviceCategory")
    dims = [dim_name]
    mets = ["sessions","totalUsers","screenPageViews","bounceRate"]
    request = RunReportRequest(property=f"properties/{prop}", dimensions=[Dimension(name=d) for d in dims],
        metrics=[Metric(name=m) for m in mets], date_ranges=[DateRange(start_date=sd, end_date=ed)],
        limit=params.limit, order_bys=[OrderBy(metric=OrderBy.MetricOrderBy(metric_name="sessions"), desc=True)])
    return f"### GA4 {params.breakdown.title()} Breakdown -- {sd} to {ed}\n\n" + _format_report(_get_client().run_report(request), dims, mets)

if __name__ == "__main__":
    logger.info(f"Starting IITA GA4 MCP on 0.0.0.0:{PORT} (streamable HTTP at /mcp)")
    mcp.run(transport="streamable-http")
