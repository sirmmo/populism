
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Avg, F, Q
from django.db.models.functions import ExtractYear
from django.http import HttpResponse
from core.models import *
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from io import StringIO, BytesIO
import csv
import pandas as pd  # opzionale per Parquet
from typing import List, Optional, Dict
import pandas as pd
import math

import json
import requests



class Command(BaseCommand):
    help = (
        "Esporta il file con gli indicatori"
    )

   
    def handle(self, *args, **opts):

        date_from="1995-01-01"                         # es. "2000-01-01"
        indicators=""                  # ?indicators=rile&indicators=people_vs_elite
        country = None         # ISO3
        election_type = None   # 'national_parliament', ...
        nuts_level = None     # 0,1,2,3
        regions = None  # lista NUTS (ITC1, ITC11, ...)
        positioning_source  = None # "CHES", "Manifesto", ecc.

        """
        Versione CSV di /matrix/indicators:
        una riga per (election, region, party) con risultati + indicatori scelti.
        """

        ppd = list(PartyPositioning.objects.all().distinct('dimension').values_list('dimension', flat=True))
        print(ppd)
        print(indicators)
        if len(indicators) == 0:
            print(indicators)
            indicators = ppd
        else:
            indicators = indicators.split('|')
        if regions:
            regions = regions.split('|')

        # 1) parse date_from
        try:
            dt_from = datetime.fromisoformat(date_from).date()
        except Exception:
            try:
                dt_from = datetime.strptime(date_from, "%Y-%m-%d").date()
            except Exception:
                raise ValueError("date_from deve essere in formato YYYY-MM-DD")

        indicator_list = list(set(indicators))  # dedup, ordine non garantito

        # 2) filtra eventi elettorali
        ev_qs = ElectionEvent.objects.filter(
            Q(election_date__gte=dt_from) |
            Q(election_date__isnull=True, election_year__gte=dt_from.year)
        )
        if country:
            ev_qs = ev_qs.filter(country_code__iexact=country)
        if election_type:
            ev_qs = ev_qs.filter(election_type=election_type)

        # 3) risultati (PartyResults) su quegli eventi/regioni
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
            # CSV vuoto ma con header minimo
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

        # 4) carica le posizioni per (party, dimension)
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

        def pick_value(party_id: int, dim: str, e_year: int):
            lst = positions_by_key.get((party_id, dim))
            if not lst:
                return None
            best_val = None
            best_year = None
            for y, v in lst:
                if y <= e_year and (best_year is None or y > best_year):
                    best_year = y
                    best_val = v
            return best_val

        # 5) CSV in memoria
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

            # appende gli indicatori scelti
            for dim in indicator_list:
                val = pick_value(party.id, dim, e_year)
                row.append(val if val is not None else "")

            writer.writerow(row)

        filename = "matrix_indicators"
        if country:
            filename += f"_{country}"
        filename += ".csv"

        with open(filename, 'w+') as fout:
            fout.write(buff.getvalue())


        #with open(filename, 'r+') as fin: 
        #    response = requests.post(
        #        "https://api-public.filemail.com/transfer/initialize",
        #        headers={"Content-Type":"application/json-patch+json"},
        #        data=json.dumps({
        #        "to": [
        #            "andreagentiliuni@gmail.com"
        #        ],
        #        "subject": filename,
        #        "message": f"in allegato {filename}",
        #        "days": 7,
        #        })
        #    )
#
        #    data = response.json()
#
        #    uploads = requests.post(url, files={filename: fin})




