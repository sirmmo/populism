from django.core.management.base import BaseCommand
from django.db import transaction
from datetime import date
from decimal import Decimal, InvalidOperation
import pandas as pd
import math

from core.models import (
    PartySourceMap,
    PartyPositioning,
    DataQualityIssues,
)


def _to_decimal(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


def _parse_year(edate_str, date_int):
    """
    Manifesto MPDS2024a:
    - edate: 'dd/mm/YYYY'
    - date: int YYYYMM
    Per le perXXX ci basta l'anno.
    """
    # prova a pescare da edate
    if isinstance(edate_str, str) and edate_str.strip():
        try:
            dt = pd.to_datetime(edate_str, dayfirst=True)
            return int(dt.year)
        except Exception:
            pass

    # fallback su "date" tipo YYYYMM
    if not pd.isna(date_int):
        try:
            di = int(date_int)
            y = di // 100  # YYYYMM -> YYYY
            return y
        except Exception:
            pass

    return 1900  # fallback ultra-conservativo


class Command(BaseCommand):
    help = (
        "Importa SOLO le variabili perXXX del Manifesto MPDS (MPDataset_MPDS2024a) "
        "come PartyPositioning.\n"
        "- NON crea PartyRegistry\n"
        "- NON tocca PartyResults / ElectionEvent\n"
        "- Usa PartySourceMap(source_system='Manifesto', source_party_id=party)\n"
        "- Ogni perNNN -> PartyPositioning.dimension='perNNN'"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--mpdataset",
            required=True,
            type=str,
            help="Path a MPDataset_MPDS2024a CSV",
        )
        parser.add_argument(
            "--dimensions",
            nargs="*",
            help=(
                "Lista opzionale di perXXX da importare. "
                "Se omessa, importa tutte le colonne che iniziano con 'per' seguite da numeri."
            ),
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        mp_path = opts["mpdataset"]
        source_system = "Manifesto"
        dim_filter = opts.get("dimensions")

        df = pd.read_csv(mp_path, low_memory=False)

        required = {"party", "edate", "date"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Mancano colonne richieste in MPDataset: {sorted(missing)}")

        # 1) individua le colonne perXXX
        if dim_filter:
            # usa solo quelle specificate, se presenti
            per_cols = [c for c in dim_filter if c in df.columns]
            missing_req = set(dim_filter) - set(per_cols)
            if missing_req:
                self.stdout.write(
                    self.style.WARNING(
                        f"⚠ Alcune dimensioni richieste non esistono nel CSV: {sorted(missing_req)}"
                    )
                )
        else:
            # autodetect: per + numeri
            per_cols = [
                c for c in df.columns
                if c.startswith("per") and c[3:].isdigit()
            ]

        if not per_cols:
            self.stdout.write(self.style.WARNING("Nessuna colonna perXXX trovata da importare."))
            return

        self.stdout.write(f"Userò {len(per_cols)} dimensioni perXXX: {', '.join(sorted(per_cols))[:200]}...")

        created = 0
        updated = 0
        unmapped = 0

        # 2) loop sulle righe
        for _, row in df.iterrows():
            party_code_raw = row.get("party")
            if pd.isna(party_code_raw):
                continue

            try:
                party_code = str(int(party_code_raw))
            except Exception:
                party_code = str(party_code_raw).strip()

            # risolvi partito via PartySourceMap
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
                    issue_type="UNMAPPED_MANIFESTO_PARTY_PER",
                    details=f"Manifesto perXXX: party={party_code}, edate={row.get('edate')}",
                    source_system=source_system,
                )
                unmapped += 1
                continue

            party = psm.party
            year = _parse_year(row.get("edate"), row.get("date"))
            valid_from = date(year, 1, 1)

            # 3) perNNN -> PartyPositioning
            for col in per_cols:
                val = row.get(col)
                if pd.isna(val):
                    continue
                dec = _to_decimal(val)
                if dec is None:
                    continue
                print(party, col, source_system, valid_from, dec)
                obj, was_created = PartyPositioning.objects.update_or_create(
                    party=party,
                    dimension=col,
                    source_system=source_system,
                    valid_from=valid_from,
                    defaults={
                        "value": dec,
                        "valid_to": None,
                        "confidence": Decimal("0.75"),
                        "notes": f"{source_system} MPDS per-category ({col}, year={year})",
                    },
                )
                if was_created:
                    created += 1
                else:
                    updated += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Manifesto perXXX import: positioning created={created}, updated={updated}, "
                f"unmapped_parties={unmapped}, dims={len(per_cols)}"
            )
        )
