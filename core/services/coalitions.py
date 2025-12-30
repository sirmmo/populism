# politics/services/coalitions.py
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Tuple
import csv
from io import StringIO

from core.models import Coalition, CoalitionMembership, PartyResults


def _d(x) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def _normalize(weights: List[Decimal]) -> List[Decimal]:
    s = sum(weights)
    if s <= 0:
        n = len(weights)
        return [Decimal(1) / Decimal(n)] * n if n else []
    return [w / s for w in weights]


def _allocate_seats(total: Optional[int], shares: List[Decimal]) -> List[Optional[int]]:
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


def coalition_split_csv(coalition: Coalition, include_original: bool = False) -> str:
    """
    Ritorna il CSV dello split per UNA coalition (Coalition model).
    Non scrive su DB. Non crea partiti.
    """
    memberships = list(
        CoalitionMembership.objects.select_related("member_party")
        .filter(coalition=coalition)
        .order_by("id")
    )
    if not memberships:
        raise ValueError(f"Coalition {coalition.id} has no memberships.")

    has_share = any(m.weight_share is not None for m in memberships)
    has_raw = any(m.raw_weight is not None for m in memberships)

    weights: List[Decimal] = []
    for m in memberships:
        if has_share:
            weights.append(_d(m.weight_share) or Decimal(0))
        elif has_raw:
            weights.append(_d(m.raw_weight) or Decimal(0))
        else:
            weights.append(Decimal(1))

    shares = _normalize(weights)

    base_results = list(
        PartyResults.objects.select_related("party", "election", "region")
        .filter(election=coalition.election, party=coalition.coalition_party)
    )

    buff = StringIO()
    writer = csv.writer(buff)

    header = [
        "coalition_id",
        "election_id",
        "region_id",
        "region_code",
        "country_code",
        "is_coalition",
        "party_id",
        "party_short_name",
        "party_canonical_name",
        "base_result_id",
        "base_votes_pct",
        "split_votes_pct",
        "base_seats",
        "split_seats",
        "turnout_pct",
        "weight_share",
    ]
    writer.writerow(header)

    for base in base_results:
        base_votes = _d(base.votes_pct)
        turnout = _d(base.turnout_pct)
        seat_alloc = _allocate_seats(base.seats, shares)

        if include_original:
            writer.writerow([
                coalition.id,
                coalition.election_id,
                base.region_id,
                base.region.nuts_code,
                coalition.election.country_code,
                1,
                base.party_id,
                base.party.short_name or "",
                base.party.canonical_name,
                base.id,
                str(base_votes) if base_votes is not None else "",
                "",
                base.seats if base.seats is not None else "",
                "",
                str(turnout) if turnout is not None else "",
                "",
            ])

        for idx, (m, share) in enumerate(zip(memberships, shares)):
            split_votes = (base_votes * share) if base_votes is not None else None
            split_seats = seat_alloc[idx] if seat_alloc else None

            writer.writerow([
                coalition.id,
                coalition.election_id,
                base.region_id,
                base.region.nuts_code,
                coalition.election.country_code,
                0,
                m.member_party_id,
                m.member_party.short_name or "",
                m.member_party.canonical_name,
                base.id,
                str(base_votes) if base_votes is not None else "",
                str(split_votes) if split_votes is not None else "",
                base.seats if base.seats is not None else "",
                "" if split_seats is None else split_seats,
                str(turnout) if turnout is not None else "",
                str(share),
            ])

    return buff.getvalue()
