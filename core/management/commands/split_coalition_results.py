from django.core.management.base import BaseCommand
from django.db import transaction
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple
import csv
import sys

from core.models import (
    Coalition, CoalitionMembership,
    PartyResults
)


def _d(x) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def _normalize_weights(weights: List[Decimal]) -> List[Decimal]:
    s = sum(weights)
    if s <= 0:
        # fallback equal
        n = len(weights)
        if n == 0:
            return []
        return [Decimal(1) / Decimal(n)] * n
    return [w / s for w in weights]


def _allocate_seats(total: int, shares: List[Decimal]) -> List[int]:
    """
    Largest remainder method to preserve total seats exactly.
    """
    if total is None:
        return [None for _ in shares]  # type: ignore
    if total <= 0:
        return [0 for _ in shares]

    exact = [Decimal(total) * s for s in shares]
    floors = [int(e.to_integral_value(rounding="ROUND_FLOOR")) for e in exact]
    remainder = total - sum(floors)
    # sort indices by fractional part desc
    frac = [(i, exact[i] - Decimal(floors[i])) for i in range(len(shares))]
    frac.sort(key=lambda t: t[1], reverse=True)

    alloc = floors[:]
    for k in range(remainder):
        alloc[frac[k][0]] += 1
    return alloc


class Command(BaseCommand):
    help = (
        "Split coalition PartyResults into member parties for a given election.\n"
        "Coalition is a PartyRegistry already present in DB.\n"
        "Outputs CSV by default; does not mutate PartyResults."
    )

    def add_arguments(self, parser):
        grp = parser.add_mutually_exclusive_group(required=True)
        grp.add_argument("--coalition-id", type=int, help="Coalition.id")
        grp.add_argument("--coalition-party-id", type=int, help="PartyRegistry.id that represents the coalition")

        parser.add_argument("--election-id", type=int, help="Required if using --coalition-party-id")

        parser.add_argument("--mode", choices=["csv", "preview"], default="csv",
                            help="csv: output CSV to stdout or --out; preview: prints a short summary")
        parser.add_argument("--out", type=str, default=None,
                            help="CSV output path. If omitted, writes to stdout.")
        parser.add_argument("--include-original", action="store_true",
                            help="Include the original coalition rows in the CSV too (marked as is_coalition=1).")

    @transaction.atomic
    def handle(self, *args, **opts):
        coalition_id = opts.get("coalition_id")
        coalition_party_id = opts.get("coalition_party_id")
        election_id = opts.get("election_id")
        mode = opts["mode"]
        out_path = opts.get("out")
        include_original = bool(opts.get("include_original"))

        if coalition_id:
            coal = Coalition.objects.select_related("coalition_party", "election").get(id=coalition_id)
        else:
            if not election_id:
                raise ValueError("--election-id is required when using --coalition-party-id")
            coal = Coalition.objects.select_related("coalition_party", "election").get(
                coalition_party_id=coalition_party_id,
                election_id=election_id,
            )

        memberships = list(
            CoalitionMembership.objects.select_related("member_party")
            .filter(coalition=coal)
            .order_by("id")
        )
        if not memberships:
            raise ValueError(f"Coalition {coal.id} has no memberships. Cannot split.")

        # Determine effective weights
        # Strategy:
        # - if ANY member has weight_share set -> use weight_share where present else 0, then normalize
        # - else if ANY raw_weight set -> use raw_weight else 1, then normalize
        # - else equal
        has_share = any(m.weight_share is not None for m in memberships)
        has_raw = any(m.raw_weight is not None for m in memberships)

        eff_weights: List[Decimal] = []
        for m in memberships:
            if has_share:
                w = _d(m.weight_share) or Decimal(0)
            elif has_raw:
                w = _d(m.raw_weight) or Decimal(0)
            else:
                w = Decimal(1)
            eff_weights.append(w)

        shares = _normalize_weights(eff_weights)

        # Fetch coalition results to split (same election, coalition party)
        base_qs = PartyResults.objects.select_related("party", "election", "region").filter(
            election=coal.election,
            party=coal.coalition_party,
        )

        base_results = list(base_qs)
        if not base_results:
            self.stdout.write(self.style.WARNING(
                f"No PartyResults found for coalition_party={coal.coalition_party_id} on election={coal.election_id}"
            ))
            return

        if mode == "preview":
            self.stdout.write(self.style.SUCCESS(
                f"Coalition {coal.id}: party={coal.coalition_party_id} election={coal.election_id} "
                f"rows={len(base_results)} members={len(memberships)}"
            ))
            for m, s in zip(memberships, shares):
                self.stdout.write(f" - member_party={m.member_party_id} share={s:.6f}")
            # show a couple of regions
            sample = base_results[:5]
            for r in sample:
                self.stdout.write(
                    f" * region={r.region.nuts_code} votes_pct={r.votes_pct} seats={r.seats}"
                )
            return

        # CSV output
        out_f = open(out_path, "w", newline="", encoding="utf-8") if out_path else sys.stdout
        writer = csv.writer(out_f)

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
            base_seats = base.seats
            turnout = _d(base.turnout_pct)

            # seats allocation preserves totals
            seat_alloc = None
            if base_seats is not None:
                seat_alloc = _allocate_seats(int(base_seats), shares)

            if include_original:
                writer.writerow([
                    coal.id,
                    coal.election_id,
                    base.region_id,
                    base.region.nuts_code,
                    coal.election.country_code,
                    1,
                    base.party_id,
                    base.party.short_name or "",
                    base.party.canonical_name,
                    base.id,
                    str(base_votes) if base_votes is not None else "",
                    "",
                    base_seats if base_seats is not None else "",
                    "",
                    str(turnout) if turnout is not None else "",
                    "",
                ])

            for idx, (m, share) in enumerate(zip(memberships, shares)):
                split_votes = None
                if base_votes is not None:
                    split_votes = (base_votes * share)

                split_seats = ""
                if seat_alloc is not None:
                    split_seats = seat_alloc[idx]

                writer.writerow([
                    coal.id,
                    coal.election_id,
                    base.region_id,
                    base.region.nuts_code,
                    coal.election.country_code,
                    0,
                    m.member_party_id,
                    m.member_party.short_name or "",
                    m.member_party.canonical_name,
                    base.id,
                    str(base_votes) if base_votes is not None else "",
                    str(split_votes) if split_votes is not None else "",
                    base_seats if base_seats is not None else "",
                    split_seats,
                    str(turnout) if turnout is not None else "",
                    str(share),
                ])

        if out_path:
            out_f.close()
            self.stdout.write(self.style.SUCCESS(f"✅ Wrote CSV to {out_path}"))
        else:
            self.stdout.write(self.style.SUCCESS("✅ CSV written to stdout"))
