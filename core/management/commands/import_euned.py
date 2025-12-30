from django.core.management.base import BaseCommand
from django.db import transaction
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import pandas as pd
import math

from core.models import (
    PartySourceMap,
    PartyAlias,
    PartyResults,
    ElectionEvent,
    ElectionEventRegion,
    GeoRegion,
    DataQualityIssues,
)


def _dec(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None


def _pfid(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        return str(int(float(x)))
    except Exception:
        return str(x).strip() or None


class Command(BaseCommand):
    help = (
        "EU-NED strict importer:\n"
        "- NON crea nuovi partiti\n"
        "- partyfacts_id → PartyRegistry via PartySourceMap\n"
        "- risultati (votes_pct, turnout_pct)\n"
        "- ElectionEvent + ElectionEventRegion\n"
        "- regioni NUTS: usa quelle già importate, crea STUB marcati se mancano"
    )

    def add_arguments(self, parser):
        parser.add_argument("file", type=str, help="EU-NED joint CSV")
        parser.add_argument("--source-system", type=str, default="EU_NED")
        parser.add_argument("--observed-at", type=str, default=None)
        parser.add_argument("--add-aliases", action="store_true")

    @transaction.atomic
    def handle(self, *args, **opt):
        path = opt["file"]
        source_system = opt["source_system"]
        add_aliases = bool(opt["add_aliases"])

        observed_at = opt["observed_at"]
        if observed_at:
            try:
                observed_at = datetime.fromisoformat(observed_at)
            except Exception:
                observed_at = datetime.utcnow()
        else:
            observed_at = datetime.utcnow()

        df = pd.read_csv(path, low_memory=False)

        required = {
            "country",
            "country_code",
            "nutslevel",
            "nuts2016",
            "year",
            "type",
            "partyfacts_id",
            "partyvote",
            "validvote",
            "electorate",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Mancano colonne fondamentali: {sorted(missing)}")

        df["year"] = df["year"].astype(int)
        df["partyfacts_id"] = df["partyfacts_id"].apply(_pfid)

        # mappa tipo elezione → modello
        MAP_TYPE = {
            "Parliament": ElectionEvent.ElectionType.NATIONAL_PARLIAMENT,
            "EP": ElectionEvent.ElectionType.EUROPEAN_PARLIAMENT,
            "Regional": ElectionEvent.ElectionType.REGIONAL,
            "Provincial": ElectionEvent.ElectionType.PROVINCIAL,
            "Local": ElectionEvent.ElectionType.LOCAL,
        }

        results_created = 0
        results_updated = 0
        eer_created = 0
        issues = 0
        alias_created = 0

        # cache eventi
        election_cache = {}

        # Gruppo: un evento per (country_code, type, year)
        for (_, etype, yr), chunk in df.groupby(["country_code", "type", "year"], dropna=False):
            e_type = MAP_TYPE.get(str(etype).strip(), ElectionEvent.ElectionType.OTHER)
            e_date = date(int(yr), 1, 1)

            for _, row in chunk.iterrows():
                pfid = row.get("partyfacts_id")
                if not pfid:
                    DataQualityIssues.objects.create(
                        severity=DataQualityIssues.Severity.WARNING,
                        issue_type="MISSING_PFID",
                        details=f"EU-NED: pfid mancante → {row.to_dict()}",
                        source_system=source_system,
                    )
                    issues += 1
                    continue

                # 1) mapping PartyFacts → PartyRegistry
                psm = PartySourceMap.objects.filter(
                    source_system="PartyFacts",
                    source_party_id=pfid,
                ).select_related("party").first()

                if not psm:
                    DataQualityIssues.objects.create(
                        severity=DataQualityIssues.Severity.WARNING,
                        issue_type="UNMAPPED_PFID",
                        details=f"EU-NED: partyfacts_id={pfid} non mappato",
                        source_system=source_system,
                    )
                    issues += 1
                    continue

                party = psm.party

                # 2) ElectionEvent (chiave nel modello: ISO3 dal party)
                e_key = (party.country_code, e_type, yr)
                if e_key in election_cache:
                    election = election_cache[e_key]
                else:
                    election, _ = ElectionEvent.objects.get_or_create(
                        country_code=party.country_code,
                        election_type=e_type,
                        election_date=e_date,
                        defaults={
                            "election_year": yr,
                            "notes": "EU-NED strict import",
                        },
                    )
                    election_cache[e_key] = election

                # 3) Regione NUTS
                nuts_code = None
                if not pd.isna(row.get("nuts2016")):
                    nuts_code = str(row["nuts2016"]).strip()

                nuts_level = 0
                try:
                    nl = row.get("nutslevel")
                    if not pd.isna(nl):
                        nuts_level = int(nl)
                except Exception:
                    pass

                region = None
                if nuts_code:
                    region = GeoRegion.objects.filter(
                        nuts_code=nuts_code,
                        nuts_level=nuts_level,
                    ).order_by("-is_current").first()

                # se non esiste → STUB
                if not region:
                    region = GeoRegion.objects.create(
                        nuts_code=nuts_code or f"{party.country_code}-NUTS{nuts_level}",
                        nuts_level=nuts_level,
                        name_official=row.get("regionname", "") or nuts_code or f"{party.country_code}-NUTS{nuts_level}",
                        country_code=party.country_code,
                        valid_from=date(1900, 1, 1),
                        is_current=True,
                        notes="Stub created by EU-NED strict importer; missing official NUTS pre-load.",
                    )

                # 4) ElectionEventRegion (CharField = nuts_code)
                _, new_eer = ElectionEventRegion.objects.get_or_create(
                    election=election,
                    region=region.nuts_code,
                    defaults={
                        "is_reporting_unit": True,
                        "notes": "EU-NED strict",
                    },
                )
                if new_eer:
                    eer_created += 1

                # 5) Risultati
                pv = _dec(row.get("partyvote"))
                vv = _dec(row.get("validvote"))
                el = _dec(row.get("electorate"))

                votes_pct = None
                turnout_pct = None

                if pv is not None and vv is not None and vv != 0:
                    votes_pct = (pv / vv) * Decimal(100)
                else:
                    DataQualityIssues.objects.create(
                        severity=DataQualityIssues.Severity.WARNING,
                        issue_type="INVALID_PARTYVOTE",
                        details=f"EU-NED: pfid={pfid}, partyvote={pv}, validvote={vv}",
                        source_system=source_system,
                    )
                    issues += 1

                if vv is not None and el is not None and el != 0:
                    turnout_pct = (vv / el) * Decimal(100)

                res, created = PartyResults.objects.update_or_create(
                    party=party,
                    election=election,
                    region=region,
                    defaults={
                        "votes_pct": votes_pct,
                        "turnout_pct": turnout_pct,
                        "seats": None,
                        "source_system": source_system,
                        "observed_at": observed_at,
                        "quality_flag": PartyResults.QualityFlag.OK,
                        "notes": f"EU-NED strict ({etype})",
                    },
                )
                if created:
                    results_created += 1
                else:
                    results_updated += 1

                # 6) Alias opzionale
                if add_aliases:
                    pname = row.get("partyname") if "partyname" in row else None
                    if pname and not pd.isna(pname):
                        pname = str(pname).strip()
                        _, new_alias = PartyAlias.objects.get_or_create(
                            party=party,
                            alias_name=pname,
                            alias_type=PartyAlias.AliasType.OTHER,
                            source_system=source_system,
                            valid_from=e_date,
                            defaults={
                                "valid_to": None,
                                "confidence": Decimal("0.75"),
                                "added_by": "import_euned_joint",
                                "notes": f"Alias EU-NED (pfid={pfid}, year={yr})",
                            },
                        )
                        if new_alias:
                            alias_created += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"EU-NED strict import → results: created={results_created}, updated={results_updated}, "
                f"EER={eer_created}, issues={issues}, aliases={alias_created}"
            )
        )
