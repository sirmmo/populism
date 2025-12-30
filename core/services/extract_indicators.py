# core/services/extract_indicators.py

from django.db.models import Q
from datetime import datetime
from decimal import Decimal
from io import StringIO
import csv
from typing import List, Optional, Dict

from core.models import (
    PartyResults,
    PartyPositioning,
    ElectionEvent,
    Coalition,
    CoalitionMembership,
)


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
    res_qs = (
        PartyResults.objects
        .select_related("party", "election", "region")
        .filter(election__in=ev_qs)
    )

    if nuts_level is not None:
        res_qs = res_qs.filter(region__nuts_level=nuts_level)
    if regions:
        res_qs = res_qs.filter(region__nuts_code__in=regions)

    results = list(res_qs)

    # ---- preload positioning
    party_ids = {r.party_id for r in results}
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

    def pick_value(party_id, dim, year):
        lst = positions.get((party_id, dim))
        if not lst:
            return ""
        best = [v for y, v in lst if y <= year]
        return best[-1] if best else ""

    # ---- preload coalitions (solo se richiesto)
    coalition_index = {}
    if split_coalitions:
        coalitions = (
            Coalition.objects
            .filter(election__in=ev_qs)
            .prefetch_related("memberships")
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
                [m.member_party_id for m in members],
                shares,
            )

    # ---- CSV
    buff = StringIO()
    writer = csv.writer(buff)

    base_cols = [
        "election_id", "election_date", "election_type",
        "country_code", "region_code", "region_name",
        "party_id", "party_short_name", "party_canonical_name",
        "votes_pct", "turnout_pct", "seats",
    ]
    writer.writerow(base_cols + indicator_list)

    for r in results:
        election = r.election
        region = r.region
        year = election.election_date.year if election.election_date else election.election_year

        key = (election.id, r.party_id)

        # ---- coalizione?
        if split_coalitions and key in coalition_index:
            member_ids, shares = coalition_index[key]

            for pid, share in zip(member_ids, shares):
                row = [
                    election.id,
                    election.election_date,
                    election.election_type,
                    election.country_code,
                    region.nuts_code,
                    region.name_official,
                    pid,
                    "",  # short_name lo puoi joinare dopo se vuoi
                    "",  # canonical_name idem
                    float(r.votes_pct) * float(share) if r.votes_pct else "",
                    r.turnout_pct,
                    "",
                ]
                for dim in indicator_list:
                    row.append(pick_value(pid, dim, year))
                writer.writerow(row)

            if not include_original_coalition:
                continue

        # ---- riga normale
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
            r.votes_pct,
            r.turnout_pct,
            r.seats,
        ]
        for dim in indicator_list:
            row.append(pick_value(r.party.id, dim, year))

        writer.writerow(row)

    return buff.getvalue()
