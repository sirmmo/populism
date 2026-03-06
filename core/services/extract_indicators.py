# core/services/extract_indicators.py

from django.db.models import Q
from datetime import datetime
from decimal import Decimal
from io import StringIO
import csv
from typing import Dict, List, Optional

from core.models import (
    PartyRegistry,
    PartyResults,
    PartyPositioning,
    ElectionEvent,
    Coalition,
    CoalitionMembership,
)
from core.services.positioning import pick_value


def _allocate_seats(total, shares):
    """Largest-remainder seat allocation (same logic as coalitions.py)."""
    if total is None:
        return [None for _ in shares]
    if total <= 0:
        return [0 for _ in shares]

    exact = [Decimal(total) * s for s in shares]
    floors = [int(e.to_integral_value(rounding="ROUND_FLOOR")) for e in exact]
    remainder = total - sum(floors)

    frac = [(i, exact[i] - Decimal(floors[i])) for i in range(len(shares))]
    frac.sort(key=lambda t: t[1], reverse=True)

    alloc = floors[:]
    for k in range(remainder):
        alloc[frac[k][0]] += 1
    return alloc


def extract_indicators_to_csv(
    date_from: str,
    indicators: Optional[List[str]] = None,
    country: Optional[str] = None,
    election_type: Optional[str] = None,
    nuts_level: Optional[int] = None,
    regions: Optional[List[str]] = None,
    positioning_source: Optional[str] = None,
    split_coalitions: bool = False,
    include_original_coalition: bool = False,
    fill_down: bool = False,
    group_by_macro_party: bool = False,
):
    # ---- parse date
    dt_from = datetime.fromisoformat(date_from).date()

    # ---- indicator list
    if not indicators:
        indicators = list(
            PartyPositioning.objects
            .values_list("dimension", flat=True)
            .distinct()
        )
    indicator_list = list(set(indicators))

    # ---- election filter
    ev_qs = ElectionEvent.objects.filter(
        Q(election_date__gte=dt_from) |
        Q(election_date__isnull=True, election_year__gte=dt_from.year)
    )
    if country:
        ev_qs = ev_qs.filter(country_code__iexact=country)
    if election_type:
        ev_qs = ev_qs.filter(election_type=election_type)

    # ---- results
    select_fields = ["party", "election", "region"]
    if group_by_macro_party:
        select_fields.append("party__macro_party")
    res_qs = (
        PartyResults.objects
        .select_related(*select_fields)
        .filter(election__in=ev_qs)
    )

    if nuts_level is not None:
        res_qs = res_qs.filter(region__nuts_level=nuts_level)
    if regions:
        res_qs = res_qs.filter(region__nuts_code__in=regions)

    results = list(res_qs)

    # ---- preload coalitions (before positioning so we can include member IDs)
    coalition_index: Dict[tuple, tuple] = {}
    member_party_ids: set = set()
    if split_coalitions:
        prefetch = ["memberships__member_party"]
        if group_by_macro_party:
            prefetch.append("memberships__member_party__macro_party")
        coalitions = (
            Coalition.objects
            .filter(election__in=ev_qs)
            .prefetch_related(*prefetch)
        )

        for c in coalitions:
            members = list(c.memberships.all())
            if not members:
                continue

            weights = []
            for m in members:
                if m.weight_share is not None:
                    weights.append(Decimal(m.weight_share))
                elif m.raw_weight is not None:
                    weights.append(Decimal(m.raw_weight))
                else:
                    weights.append(Decimal(1))

            total = sum(weights) or Decimal(len(weights))
            shares = [w / total for w in weights]

            coalition_index[(c.election_id, c.coalition_party_id)] = (
                members,
                shares,
            )
            member_party_ids.update(m.member_party_id for m in members)

    # ---- preload positioning (include both result parties and coalition members)
    party_ids = {r.party_id for r in results} | member_party_ids
    pos_qs = PartyPositioning.objects.filter(
        party_id__in=party_ids,
        dimension__in=indicator_list,
    )
    if positioning_source:
        pos_qs = pos_qs.filter(source_system=positioning_source)

    positions = {}
    for p in pos_qs:
        positions.setdefault((p.party_id, p.dimension), []).append(
            (p.valid_from.year, float(p.value))
        )

    for k in positions:
        positions[k].sort(key=lambda x: x[0])

    # ---- CSV
    buff = StringIO()
    writer = csv.writer(buff)

    base_cols = [
        "election_id", "election_date", "election_type",
        "country_code", "region_code", "region_name",
        "party_id", "party_short_name", "party_canonical_name",
    ]
    if group_by_macro_party:
        base_cols += ["macro_party_id", "macro_party_name"]
    base_cols += ["votes_pct", "turnout_pct", "seats"]
    writer.writerow(base_cols + indicator_list)

    for r in results:
        election = r.election
        region = r.region
        year = election.election_date.year if election.election_date else election.election_year

        key = (election.id, r.party_id)

        # ---- coalition split
        if split_coalitions and key in coalition_index:
            members, shares = coalition_index[key]
            seat_alloc = _allocate_seats(r.seats, shares)

            for idx, (m, share) in enumerate(zip(members, shares)):
                split_votes = (
                    float(r.votes_pct) * float(share)
                    if r.votes_pct is not None else ""
                )
                split_seats = seat_alloc[idx] if seat_alloc else ""

                row = [
                    election.id,
                    election.election_date,
                    election.election_type,
                    election.country_code,
                    region.nuts_code,
                    region.name_official,
                    m.member_party_id,
                    m.member_party.short_name or "",
                    m.member_party.canonical_name,
                ]
                if group_by_macro_party:
                    mp = m.member_party.macro_party
                    row += [mp.id if mp else "", mp.name if mp else ""]
                row += [
                    split_votes,
                    r.turnout_pct,
                    split_seats if split_seats is not None else "",
                ]
                for dim in indicator_list:
                    val = pick_value(positions, m.member_party_id, dim, year, fill_down=fill_down)
                    row.append(val if val is not None else "")
                writer.writerow(row)

            if not include_original_coalition:
                continue

        # ---- normal row
        row = [
            election.id,
            election.election_date,
            election.election_type,
            election.country_code,
            region.nuts_code,
            region.name_official,
            r.party.id,
            r.party.short_name or "",
            r.party.canonical_name,
        ]
        if group_by_macro_party:
            mp = r.party.macro_party
            row += [mp.id if mp else "", mp.name if mp else ""]
        row += [
            r.votes_pct,
            r.turnout_pct,
            r.seats,
        ]
        for dim in indicator_list:
            val = pick_value(positions, r.party.id, dim, year, fill_down=fill_down)
            row.append(val if val is not None else "")

        writer.writerow(row)

    return buff.getvalue()
