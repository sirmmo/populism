# api/router.py
from ninja import Router
from django.http import HttpResponse
from django.db.models import Avg, Q
from django.db.models.functions import ExtractYear
from io import StringIO, BytesIO
import csv
import pandas as pd
from typing import List, Optional, Dict
from datetime import datetime

from .models import (
    PartyRegistry,
    PartyPositioning,
    PartyResults,
    ElectionEvent,
    DataQualityIssues,
)
from .schemas import (
    PartyOut, PositionPoint, TimeSeriesOut,
    PopulismPoint, PopulismSeriesOut, QualityIssueOut
)
from core.services.positioning import pick_value


router = Router(tags=["analysis"])

# --- util: default "indice di populismo" (configurabile) ---
DEFAULT_POP_WEIGHTS = {
    "people_v_elite":  0.5,
    "anti_elite_salience": 0.3,
    "corrupt_salience": 0.2,
}

# -----------------------------------------------------------
# 1) Parties search/list
# -----------------------------------------------------------
@router.get("/parties", response=List[PartyOut])
def list_parties(request,
                 country: Optional[str] = None,
                 q: Optional[str] = None,
                 limit: int = 100,
                 offset: int = 0):
    qs = PartyRegistry.objects.all().order_by("country_code", "short_name", "canonical_name")
    if country:
        qs = qs.filter(country_code__iexact=country)
    if q:
        qs = qs.filter(Q(short_name__icontains=q) | Q(canonical_name__icontains=q))
    qs = qs[offset: offset + min(limit, 500)]
    return [
        PartyOut(
            id=p.id,
            country_code=p.country_code,
            canonical_name=p.canonical_name,
            short_name=p.short_name
        ) for p in qs
    ]

# -----------------------------------------------------------
# 2) Time series per dimensione (CHES/Manifesto)
# -----------------------------------------------------------
@router.get("/positions/timeseries", response=List[TimeSeriesOut])
def positions_timeseries(request,
                         country: str,
                         dimension: str,
                         source_system: Optional[str] = None,
                         party_ids: Optional[List[int]] = None,
                         year_from: Optional[int] = None,
                         year_to: Optional[int] = None):
    qs = (PartyPositioning.objects
          .select_related("party")
          .filter(party__country_code__iexact=country, dimension=dimension))
    if source_system:
        qs = qs.filter(source_system=source_system)
    if party_ids:
        qs = qs.filter(party_id__in=party_ids)
    if year_from:
        qs = qs.filter(valid_from__year__gte=year_from)
    if year_to:
        qs = qs.filter(valid_from__year__lte=year_to)

    qs = qs.annotate(year=ExtractYear("valid_from")) \
           .values("party_id", "party__short_name", "source_system", "dimension", "year") \
           .annotate(value=Avg("value")) \
           .order_by("party_id", "year")

    series_map = {}
    for row in qs:
        key = (row["party_id"], row["party__short_name"], row["dimension"], row["source_system"])
        series = series_map.setdefault(key, [])
        series.append(PositionPoint(
            party_id=row["party_id"],
            party_short_name=row["party__short_name"] or "",
            source_system=row["source_system"],
            dimension=row["dimension"],
            value=float(row["value"]),
            year=int(row["year"])
        ))

    out = []
    for (pid, ps, dim, src), points in series_map.items():
        out.append(TimeSeriesOut(
            party_id=pid,
            party_short_name=ps or "",
            dimension=dim,
            source_system=src,
            points=points
        ))
    return out

# -----------------------------------------------------------
# 3) Populism index (configurabile + trend)
# -----------------------------------------------------------
@router.get("/positions/populism-index", response=List[PopulismSeriesOut])
def populism_index(request,
                   country: str,
                   party_ids: Optional[List[int]] = None,
                   weights: Optional[Dict[str, float]] = None,
                   source_system: str = "CHES",
                   year_from: Optional[int] = None,
                   year_to: Optional[int] = None):
    w = DEFAULT_POP_WEIGHTS.copy()
    if weights:
        w.update({k: float(v) for k, v in weights.items()})

    dims = list(w.keys())

    qs = (PartyPositioning.objects
          .select_related("party")
          .filter(party__country_code__iexact=country,
                  source_system=source_system,
                  dimension__in=dims))
    if party_ids:
        qs = qs.filter(party_id__in=party_ids)
    if year_from:
        qs = qs.filter(valid_from__year__gte=year_from)
    if year_to:
        qs = qs.filter(valid_from__year__lte=year_to)

    qs = qs.annotate(year=ExtractYear("valid_from")) \
           .values("party_id", "party__short_name", "dimension", "year") \
           .annotate(value=Avg("value")) \
           .order_by("party_id", "year", "dimension")

    by_party_year = {}
    for r in qs:
        key = (r["party_id"], r["party__short_name"], int(r["year"]))
        entry = by_party_year.setdefault(key, {"sum": 0.0, "count": 0})
        dim = r["dimension"]
        if dim in w and r["value"] is not None:
            entry["sum"] += float(w[dim]) * float(r["value"])
            entry["count"] += 1

    def slope_10y(points):
        if len(points) < 2:
            return 0.0
        years = [p["year"] for p in points]
        vals  = [p["index"] for p in points]
        span = max(years) - min(years) or 1
        return (vals[-1] - vals[0]) / span * 10.0

    def delta_latest_5y(points):
        if not points:
            return 0.0
        last = points[-1]
        y0 = last["year"] - 5
        prevs = [p for p in points if p["year"] <= y0]
        if not prevs:
            return 0.0
        return last["index"] - prevs[-1]["index"]

    series_by_party = {}
    for (pid, ps, y), acc in sorted(by_party_year.items(), key=lambda x: (x[0][0], x[0][2])):
        point = {"party_id": pid, "party_short_name": ps or "", "year": y, "index": acc["sum"]}
        series_by_party.setdefault((pid, ps), []).append(point)

    out = []
    for (pid, ps), points in series_by_party.items():
        out.append(PopulismSeriesOut(
            party_id=pid,
            party_short_name=ps or "",
            points=[PopulismPoint(**p) for p in points],
            slope_per_10y=slope_10y(points),
            delta_latest_5y=delta_latest_5y(points),
        ))
    return out

# -----------------------------------------------------------
# 4) Download CSV (stesse query dei time series)
# -----------------------------------------------------------
@router.get("/positions/download")
def download_positions_csv(request,
                           country: str,
                           dimension: str,
                           source_system: Optional[str] = None,
                           party_ids: Optional[List[int]] = None,
                           year_from: Optional[int] = None,
                           year_to: Optional[int] = None):
    qs = (PartyPositioning.objects
          .select_related("party")
          .filter(party__country_code__iexact=country, dimension=dimension))
    if source_system:
        qs = qs.filter(source_system=source_system)
    if party_ids:
        qs = qs.filter(party_id__in=party_ids)
    if year_from:
        qs = qs.filter(valid_from__year__gte=year_from)
    if year_to:
        qs = qs.filter(valid_from__year__lte=year_to)

    qs = qs.annotate(year=ExtractYear("valid_from")) \
           .values("party_id", "party__short_name", "source_system", "dimension", "year") \
           .annotate(value=Avg("value")) \
           .order_by("party_id", "year")

    buff = StringIO()
    writer = csv.writer(buff)
    writer.writerow(["party_id", "party_short_name", "source_system", "dimension", "year", "value"])
    for r in qs:
        writer.writerow([
            r["party_id"], r["party__short_name"] or "",
            r["source_system"], r["dimension"], int(r["year"]),
            float(r["value"]) if r["value"] is not None else ""
        ])
    response = HttpResponse(buff.getvalue(), content_type="text/csv")
    response["Content-Disposition"] = f'attachment; filename="positions_{country}_{dimension}.csv"'
    return response

# -----------------------------------------------------------
# 5) Download Parquet
# -----------------------------------------------------------
@router.get("/positions/download.parquet")
def download_positions_parquet(request,
                               country: str,
                               dimension: str,
                               source_system: Optional[str] = None,
                               party_ids: Optional[List[int]] = None,
                               year_from: Optional[int] = None,
                               year_to: Optional[int] = None):
    qs = (PartyPositioning.objects
          .select_related("party")
          .filter(party__country_code__iexact=country, dimension=dimension))
    if source_system:
        qs = qs.filter(source_system=source_system)
    if party_ids:
        qs = qs.filter(party_id__in=party_ids)
    if year_from:
        qs = qs.filter(valid_from__year__gte=year_from)
    if year_to:
        qs = qs.filter(valid_from__year__lte=year_to)

    qs = qs.annotate(year=ExtractYear("valid_from")) \
           .values("party_id", "party__short_name", "source_system", "dimension", "year") \
           .annotate(value=Avg("value")) \
           .order_by("party_id", "year")

    df = pd.DataFrame(list(qs))
    bio = BytesIO()
    try:
        import pyarrow as pa, pyarrow.parquet as pq
        table = pa.Table.from_pandas(df)
        pq.write_table(table, bio)
        bio.seek(0)
        resp = HttpResponse(bio.read(), content_type="application/octet-stream")
        resp["Content-Disposition"] = f'attachment; filename="positions_{country}_{dimension}.parquet"'
        return resp
    except ImportError:
        buff = StringIO()
        df.to_csv(buff, index=False)
        response = HttpResponse(buff.getvalue(), content_type="text/csv")
        response["Content-Disposition"] = f'attachment; filename="positions_{country}_{dimension}.csv"'
        return response

# -----------------------------------------------------------
# 6) Quality issues
# -----------------------------------------------------------
@router.get("/quality/issues", response=List[QualityIssueOut])
def quality_issues(request,
                   country: Optional[str] = None,
                   severity: Optional[str] = None,
                   limit: int = 200,
                   offset: int = 0):
    qs = DataQualityIssues.objects.select_related("party", "region", "election").all()
    if severity:
        qs = qs.filter(severity=severity)
    if country:
        qs = qs.filter(Q(party__country_code__iexact=country) | Q(region__country_code__iexact=country))
    qs = qs.order_by("-detected_at")[offset: offset + min(limit, 1000)]
    out = []
    for i in qs:
        out.append(QualityIssueOut(
            id=i.id,
            severity=i.severity,
            issue_type=i.issue_type,
            details=i.details,
            party_short_name=getattr(i.party, "short_name", None),
            region_name=getattr(i.region, "name_official", None),
            election_date=getattr(i.election, "election_date", None).isoformat() if i.election else None,
            source_system=i.source_system
        ))
    return out

# -----------------------------------------------------------
# 7) Matrix indicators CSV
# -----------------------------------------------------------
@router.get("/matrix/indicators.csv")
def matrix_indicators_csv(
    request,
    date_from: str = "1990-01-01",
    indicators: Optional[str] = "",
    country: Optional[str] = None,
    election_type: Optional[str] = None,
    nuts_level: Optional[int] = None,
    regions: Optional[str] = None,
    positioning_source: Optional[str] = None,
    fill_down: bool = False,
):
    """
    CSV version of /matrix/indicators:
    one row per (election, region, party) with results + chosen indicators.

    By default positioning values are matched by exact year only.
    Pass fill_down=true to carry the most recent prior value forward.
    """

    ppd = list(
        PartyPositioning.objects.all()
        .distinct("dimension")
        .values_list("dimension", flat=True)
    )
    if len(indicators) == 0:
        indicators = ppd
    else:
        indicators = indicators.split("|")
    if regions:
        regions = regions.split("|")

    try:
        dt_from = datetime.fromisoformat(date_from).date()
    except ValueError:
        try:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
        except ValueError:
            return HttpResponse("date_from must be YYYY-MM-DD", status=400)

    indicator_list = list(set(indicators))

    ev_qs = ElectionEvent.objects.filter(
        Q(election_date__gte=dt_from) |
        Q(election_date__isnull=True, election_year__gte=dt_from.year)
    )
    if country:
        ev_qs = ev_qs.filter(country_code__iexact=country)
    if election_type:
        ev_qs = ev_qs.filter(election_type=election_type)

    res_qs = (
        PartyResults.objects
        .select_related("party", "election", "region")
        .filter(election__in=ev_qs)
    )

    if nuts_level is not None:
        res_qs = res_qs.filter(region__nuts_level=nuts_level)
    if regions:
        res_qs = res_qs.filter(region__nuts_code__in=regions)
    if country:
        res_qs = res_qs.filter(
            Q(party__country_code__iexact=country) |
            Q(region__country_code__iexact=country)
        )

    res_qs = res_qs.order_by("election__election_date", "region__nuts_code", "party__id")
    results = list(res_qs)
    if not results:
        buff = StringIO()
        writer = csv.writer(buff)
        base_cols = [
            "election_id", "election_date", "election_type",
            "country_code", "region_code", "region_name",
            "party_id", "party_short_name", "party_canonical_name",
            "votes_pct", "turnout_pct", "seats",
        ]
        writer.writerow(base_cols + indicator_list)
        resp = HttpResponse(buff.getvalue(), content_type="text/csv")
        resp["Content-Disposition"] = 'attachment; filename="matrix_indicators_empty.csv"'
        return resp

    party_ids = {r.party_id for r in results}

    pos_qs = PartyPositioning.objects.filter(
        party_id__in=party_ids,
        dimension__in=indicator_list,
    )
    if positioning_source:
        pos_qs = pos_qs.filter(source_system=positioning_source)

    positions_by_key: Dict[tuple, List[tuple]] = {}
    for p in pos_qs:
        key = (p.party_id, p.dimension)
        year = p.valid_from.year
        positions_by_key.setdefault(key, []).append((year, float(p.value)))

    for key in positions_by_key:
        positions_by_key[key].sort(key=lambda x: x[0])

    # CSV in memoria
    buff = StringIO()
    writer = csv.writer(buff)

    base_cols = [
        "election_id",
        "election_date",
        "election_type",
        "country_code",
        "region_code",
        "region_name",
        "party_id",
        "party_short_name",
        "party_canonical_name",
        "votes_pct",
        "turnout_pct",
        "seats",
    ]
    header = base_cols + indicator_list
    writer.writerow(header)

    for r in results:
        election = r.election
        region = r.region
        party = r.party

        e_date = election.election_date
        e_year = e_date.year if e_date else (election.election_year or dt_from.year)

        row = [
            election.id,
            e_date.isoformat() if e_date else "",
            election.election_type,
            election.country_code,
            region.nuts_code if region else "",
            region.name_official if region else "",
            party.id,
            party.short_name or "",
            party.canonical_name,
            float(r.votes_pct) if r.votes_pct is not None else "",
            float(r.turnout_pct) if r.turnout_pct is not None else "",
            r.seats if r.seats is not None else "",
        ]

        for dim in indicator_list:
            val = pick_value(positions_by_key, party.id, dim, e_year, fill_down=fill_down)
            row.append(val if val is not None else "")

        writer.writerow(row)

    filename = "matrix_indicators"
    if country:
        filename += f"_{country}"
    filename += ".csv"

    resp = HttpResponse(buff.getvalue(), content_type="text/csv")
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp
