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
    PartyPositioning,
    ElectionEvent,
    GeoRegion,
    ElectionEventRegion,
    DataQualityIssues,
)


def _to_decimal(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def _parse_edate(edate_str, date_int):
    """
    Manifesto MPDS2024a:
    - edate: 'dd/mm/YYYY' (es. '17/09/1944')
    - date: int YYYYMM (es. 194409)
    """
    # 1) edate 'dd/mm/YYYY'
    if isinstance(edate_str, str) and edate_str.strip():
        try:
            # dayfirst=True perché è dd/mm/YYYY
            dt = pd.to_datetime(edate_str, dayfirst=True)
            return dt.date()
        except Exception:
            pass

    # 2) date numerico YYYYMM
    if not pd.isna(date_int):
        try:
            di = int(date_int)
            y = di // 100
            m = di % 100 or 1
            return date(y, m, 1)
        except Exception:
            pass

    # 3) fallback
    return date(1900, 1, 1)


class Command(BaseCommand):
    help = (
        "Importa Manifesto MPDS2024a in modo strict:\n"
        "- NON crea PartyRegistry\n"
        "- usa PartySourceMap(source_system='Manifesto', source_party_id=party)\n"
        "- scrive PartyResults (NUTS0) e PartyPositioning('rile')\n"
        "- opzionalmente crea PartyAlias con notes che includono l'ID Manifesto e partyfacts_id"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--mpdataset",
            required=True,
            type=str,
            help="Path a MPDataset_MPDS2024a (CSV)",
        )
        parser.add_argument(
            "--source-system",
            default="Manifesto",
            type=str,
            help="Nome del source_system per mapping/positioning (default: Manifesto)",
        )
        parser.add_argument(
            "--observed-at",
            default=None,
            type=str,
            help="Timestamp ISO da usare per PartyResults.observed_at (default: now)",
        )
        parser.add_argument(
            "--add-aliases",
            action="store_true",
            help="Se presente, crea alias da partyname/partyabbrev con ID Manifesto nei notes",
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        mp_path = opts["mpdataset"]
        source_system = opts["source_system"]
        observed_at = opts["observed_at"]
        add_aliases = bool(opts["add_aliases"])

        if observed_at:
            try:
                observed_at = datetime.fromisoformat(observed_at)
            except Exception:
                observed_at = datetime.utcnow()
        else:
            observed_at = datetime.utcnow()

        df = pd.read_csv(mp_path, low_memory=False)

        required = {"party", "partyname", "partyabbrev", "edate", "date"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Mancano colonne richieste in MPDataset: {sorted(missing)}")

        has_pervote = "pervote" in df.columns
        has_pervote_n = "pervote_n" in df.columns
        has_vote = "vote" in df.columns
        has_absseat = "absseat" in df.columns

        results_created = 0
        results_updated = 0
        pos_created = 0
        pos_updated = 0
        unmapped = 0
        created_eer = 0
        aliases_created = 0

        nuts0_cache = {}

        for _, row in df.iterrows():
            party_code_raw = row.get("party")
            if pd.isna(party_code_raw):
                continue

            # party nel dataset Manifesto: int -> string
            try:
                party_code = str(int(party_code_raw))
            except Exception:
                party_code = str(party_code_raw).strip()

            # 1) Trova il partito via PartySourceMap (Manifesto)
            psm = (
                PartySourceMap.objects.filter(
                    source_system=source_system,
                    source_party_id=party_code,
                )
                .select_related("party")
                .first()
            )

            if not psm:
                DataQualityIssues.objects.create(
                    severity=DataQualityIssues.Severity.WARNING,
                    issue_type="UNMAPPED_MANIFESTO_PARTY",
                    details=f"Manifesto party={party_code} name={row.get('partyname')}",
                    source_system=source_system,
                )
                unmapped += 1
                continue

            party = psm.party

            # 2) Data elezione
            e_date = _parse_edate(row.get("edate"), row.get("date"))

            election, _ = ElectionEvent.objects.get_or_create(
                country_code=party.country_code,  # ISO3 nel tuo modello
                election_type=ElectionEvent.ElectionType.NATIONAL_PARLIAMENT,
                election_date=e_date,
                defaults={
                    "election_year": e_date.year,
                    "notes": "Manifesto MPDS2024a strict import",
                },
            )

            # 3) Regione NUTS0: stub se manca
            if party.country_code not in nuts0_cache:
                region = GeoRegion.objects.filter(
                    country_code=party.country_code,
                    nuts_level=0,
                ).first()
                if not region:
                    region = GeoRegion.objects.create(
                        nuts_code=f"{party.country_code}-NUTS0",
                        nuts_level=0,
                        name_official=party.country_code,
                        country_code=party.country_code,
                        valid_from=date(1900, 1, 1),
                        is_current=True,
                        notes="Stub NUTS0 from Manifesto MPDS2024a import",
                    )
                nuts0_cache[party.country_code] = region
            region = nuts0_cache[party.country_code]

            # ElectionEventRegion: nel tuo modello region è CharField
            _, eer_created = ElectionEventRegion.objects.get_or_create(
                election=election,
                region=region.nuts_code or region.name_official or party.country_code,
                defaults={
                    "is_reporting_unit": True,
                    "notes": "Manifesto MPDS strict",
                },
            )
            if eer_created:
                created_eer += 1

            # 4) PartyResults (NUTS0)
            votes_pct = None
            if has_pervote and not pd.isna(row.get("pervote")):
                votes_pct = _to_decimal(row.get("pervote"))
            elif has_pervote_n and not pd.isna(row.get("pervote_n")):
                votes_pct = _to_decimal(row.get("pervote_n"))
            elif has_vote and not pd.isna(row.get("vote")):
                # se vote è già percentuale, ok; se è assoluto, hai bisogno di totali (qui non li usiamo)
                votes_pct = _to_decimal(row.get("vote"))

            seats_val = None
            if has_absseat and not pd.isna(row.get("absseat")):
                try:
                    seats_val = int(row.get("absseat"))
                except Exception:
                    seats_val = None

            res, created = PartyResults.objects.update_or_create(
                party=party,
                election=election,
                region=region,
                defaults={
                    "votes_pct": votes_pct,
                    "turnout_pct": None,
                    "seats": seats_val,
                    "source_system": source_system,
                    "observed_at": observed_at,
                    "quality_flag": PartyResults.QualityFlag.OK,
                    "notes": "Manifesto MPDS2024a strict",
                },
            )
            if created:
                results_created += 1
            else:
                results_updated += 1

            # 5) PartyPositioning: 'rile'
            if "rile" in df.columns and not pd.isna(row.get("rile")):
                dec = _to_decimal(row.get("rile"))
                if dec is not None:
                    base_year = election.election_date.year if election.election_date else (election.election_year or 1900)
                    ppos, p_created = PartyPositioning.objects.update_or_create(
                        party=party,
                        dimension="rile",
                        source_system=source_system,
                        valid_from=date(base_year, 1, 1),
                        defaults={
                            "value": dec,
                            "valid_to": None,
                            "confidence": Decimal("0.8"),
                            "notes": f"MPDS {base_year}",
                        },
                    )
                    if p_created:
                        pos_created += 1
                    else:
                        pos_updated += 1

            # 6) Alias opzionali: partyname / partyabbrev, con ID Manifesto nei notes
            if add_aliases:
                # prova a recuperare partyfacts_id (se c'è mapping PartyFacts)
                pf_map = (
                    PartySourceMap.objects.filter(party=party, source_system="PartyFacts")
                    .values("source_party_id")
                    .first()
                )
                partyfacts_id = pf_map["source_party_id"] if pf_map else None

                alias_names = []

                partyname = row.get("partyname")
                if not pd.isna(partyname):
                    alias_names.append(
                        (str(partyname).strip(), PartyAlias.AliasType.OTHER)
                    )

                partyabbrev = row.get("partyabbrev")
                if not pd.isna(partyabbrev):
                    alias_names.append(
                        (str(partyabbrev).strip(), PartyAlias.AliasType.SHORT_NAME)
                    )

                for alias_name, alias_type in alias_names:
                    if not alias_name:
                        continue

                    _, a_created = PartyAlias.objects.get_or_create(
                        party=party,
                        alias_name=alias_name,
                        alias_type=alias_type,
                        source_system=source_system,
                        valid_from=election.election_date or date(election.election_year or 1900, 1, 1),
                        defaults={
                            "valid_to": None,
                            "confidence": Decimal("0.85"),
                            "added_by": "import_manifesto_mpds",
                            "notes": (
                                f"Alias from {source_system} "
                                f"(source_id={party_code}"
                                + (f', partyfacts_id={partyfacts_id}' if partyfacts_id else "")
                                + f", year={election.election_year or e_date.year})"
                            ),
                        },
                    )
                    if a_created:
                        aliases_created += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Manifesto MPDS2024a strict: results created={results_created}, "
                f"updated={results_updated}, positioning created={pos_created}, "
                f"updated={pos_updated}, unmapped_parties={unmapped}, "
                f"EER created={created_eer}, aliases_created={aliases_created}"
            )
        )
