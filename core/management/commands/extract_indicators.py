
from django.core.management.base import BaseCommand
from django.db.models import Q
from core.models import (
    PartyPositioning,
    PartyResults,
    ElectionEvent,
)
from datetime import datetime
from io import StringIO
import csv
from typing import List, Dict

from core.services.positioning import pick_value


class Command(BaseCommand):
    help = "Export the indicators matrix to CSV"

    def handle(self, *args, **opts):

        date_from = "1995-01-01"
        indicators = ""
        country = None
        election_type = None
        nuts_level = None
        regions = None
        positioning_source = None

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
        except Exception:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()

        indicator_list = list(set(indicators))

        # filtra eventi elettorali
        ev_qs = ElectionEvent.objects.filter(
            Q(election_date__gte=dt_from)
            | Q(election_date__isnull=True, election_year__gte=dt_from.year)
        )
        if country:
            ev_qs = ev_qs.filter(country_code__iexact=country)
        if election_type:
            ev_qs = ev_qs.filter(election_type=election_type)

        # risultati (PartyResults) su quegli eventi/regioni
        res_qs = (
            PartyResults.objects.select_related("party", "election", "region").filter(
                election__in=ev_qs
            )
        )

        if nuts_level is not None:
            res_qs = res_qs.filter(region__nuts_level=nuts_level)
        if regions:
            res_qs = res_qs.filter(region__nuts_code__in=regions)
        if country:
            res_qs = res_qs.filter(
                Q(party__country_code__iexact=country)
                | Q(region__country_code__iexact=country)
            )

        res_qs = res_qs.order_by(
            "election__election_date", "region__nuts_code", "party__id"
        )
        results = list(res_qs)
        if not results:
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
            writer.writerow(base_cols + indicator_list)
            self.stdout.write("No results found, writing empty CSV.")
            return

        party_ids = {r.party_id for r in results}

        # carica le posizioni per (party, dimension)
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
                val = pick_value(positions_by_key, party.id, dim, e_year)
                row.append(val if val is not None else "")

            writer.writerow(row)

        filename = "matrix_indicators"
        if country:
            filename += f"_{country}"
        filename += ".csv"

        with open(filename, "w+") as fout:
            fout.write(buff.getvalue())

        self.stdout.write(self.style.SUCCESS(f"Exported to {filename}"))
