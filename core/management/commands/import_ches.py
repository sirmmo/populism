from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import *
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import pandas as pd
import math


STRUCT_COLS = {
    'country', 'party_id', 'party', 'family', 'electionyear', 'vote',
       'seat', 'seatperc', 'epvote', 'eu_position', 'eu_salience',
       'eu_dissent', 'eu_blur', 'lrecon', 'lrecon_blur', 'lrecon_dissent',
       'lrecon_salience', 'galtan', 'galtan_blur', 'galtan_dissent',
       'galtan_salience', 'lrgen', 'immigrate_policy', 'immigrate_salience',
       'immigrate_dissent', 'multiculturalism', 'multicult_salience',
       'redistribution', 'redist_salience', 'climate_change',
       'climate_change_salience', 'environment', 'environment_salience',
       'spendvtax', 'deregulation', 'civlib_laworder', 'womens_rights',
       'lgbtq_rights', 'samesex_marriage', 'religious_principles',
       'ethnic_minorities', 'nationalism', 'urban_rural', 'protectionism',
       'regions', 'executive_power', 'judicial_independence',
       'corrupt_salience', 'anti_islam', 'people_v_elite',
       'anti_elite_salience', 'eu_foreign', 'eu_intmark', 'eu_russia'
}

dim_cols =  {'eu_position', 'eu_salience',
                    'eu_dissent', 'eu_blur', 'lrecon', 'lrecon_blur', 'lrecon_dissent',
                    'lrecon_salience', 'galtan', 'galtan_blur', 'galtan_dissent',
                    'galtan_salience', 'lrgen', 'immigrate_policy', 'immigrate_salience',
                    'immigrate_dissent', 'multiculturalism', 'multicult_salience',
                    'redistribution', 'redist_salience', 'climate_change',
                    'climate_change_salience', 'environment', 'environment_salience',
                    'spendvtax', 'deregulation', 'civlib_laworder', 'womens_rights',
                    'lgbtq_rights', 'samesex_marriage', 'religious_principles',
                    'ethnic_minorities', 'nationalism', 'urban_rural', 'protectionism',
                    'regions', 'executive_power', 'judicial_independence',
                    'corrupt_salience', 'anti_islam', 'people_v_elite',
                    'anti_elite_salience', 'eu_foreign', 'eu_intmark', 'eu_russia'}


# Se vuoi forzare ISO2 -> ISO3, popola questa mappa (altrimenti tengo ISO2 upper-case)
ISO2_TO_ISO3 = {
    # "BE": "BEL",
    # "NL": "NLD",
    # "IT": "ITA",
}


def _to_decimal(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


class Command(BaseCommand):
    help = (
        "Importa CHES in modo *strict*:\n"
        "- NON crea mai nuovi PartyRegistry\n"
        "- usa PartySourceMap(source_system='CHES', source_party_id=party_id)\n"
        "- scrive PartyPositioning\n"
        "- crea eventuali PartyAlias con notes che includono l'ID nel source system"
    )

    def add_arguments(self, parser):
        parser.add_argument("file", type=str, help="Path al CSV CHES")
        parser.add_argument(
            "--source-system",
            type=str,
            default="CHES",
            help="Nome del source_system per PartySourceMap/Positioning (default: CHES)",
        )
        parser.add_argument(
            "--add-aliases",
            action="store_true",
            help="Se presente, crea alias dai nomi nel file CHES (se ci sono colonne di label)",
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        path = opts["file"]
        source_system = opts["source_system"]
        add_aliases = bool(opts["add_aliases"])

        df = pd.read_csv(path, low_memory=False)

        required = {"country", "party_id", "electionyear"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Mancano colonne obbligatorie nel CSV: {sorted(missing)}")

        # normalizza tipi base
        df["electionyear"] = df["electionyear"].astype(int)
        df["party_id"] = df["party_id"].apply(
            lambda x: str(int(x)) if not pd.isna(x) and str(x).strip() != "" else None
        )

        # colonne candidate per i nomi (se presenti)
        name_cols = [
            c
            for c in [
                "party",          # es. "FN", "PVV", ecc. (nel tuo file 1999-2019)
                "party_name",
                "partyname",
                "party_english",
                "party_native",
                "name",
                "name_short",
            ]
            if c in df.columns
        ]

        # dimensioni numeric (tutte le float/int, esclusi i campi non-dimension)
        numeric_cols = [c for c, dt in df.dtypes.items() if dt.kind in ("f", "i")]
        dim_cols = [
            c
            for c in numeric_cols
            if c not in {"vote", "epvote", "seat", "seatperc", "electionyear"}
        ]

        created_pos = 0
        updated_pos = 0
        unmapped = 0
        alias_created = 0

        for _, row in df.iterrows():
            party_id = row.get("party_id")
            if not party_id:
                continue

            # 1) Trova il partito via mapping CHES -> PartyRegistry
            psm = (
                PartySourceMap.objects.filter(
                    source_system=source_system,
                    source_party_id=str(party_id),
                )
                .select_related("party")
                .first()
            )

            if not psm:
                DataQualityIssues.objects.create(
                    severity=DataQualityIssues.Severity.WARNING,
                    issue_type="UNMAPPED_CHES_PARTY",
                    details=f"CHES party_id={party_id} country={row.get('country')}",
                    source_system=source_system,
                )
                unmapped += 1
                continue

            party = psm.party
            year = int(row["electionyear"])
            valid_from = date(year, 1, 1)

            # 2) Positioning per tutte le dimensioni numeric-dimension
            for dim in dim_cols:
                val = row.get(dim)
                if pd.isna(val):
                    continue
                dec = _to_decimal(val)
                if dec is None:
                    continue

                obj, created = PartyPositioning.objects.update_or_create(
                    party=party,
                    dimension=dim,
                    source_system=source_system,
                    valid_from=valid_from,
                    defaults={
                        "value": dec,
                        "valid_to": None,
                        "confidence": Decimal("0.90"),
                        "notes": f"{source_system} {year}",
                    },
                )
                if created:
                    created_pos += 1
                else:
                    updated_pos += 1

            # 3) Alias (opzionale) – registra anche l'ID della fonte nei notes
            if add_aliases and name_cols:
                # prova a recuperare anche il PartyFacts id (se c'è) per arricchire i notes
                pf_map = (
                    PartySourceMap.objects.filter(party=party, source_system="PartyFacts")
                    .values("source_party_id")
                    .first()
                )
                partyfacts_id = pf_map["source_party_id"] if pf_map else None

                aliases = set()
                for col in name_cols:
                    val = row.get(col)
                    if pd.isna(val):
                        continue
                    name = str(val).strip()
                    if not name:
                        continue
                    aliases.add(name)

                for alias_name in aliases:
                    _, a_created = PartyAlias.objects.get_or_create(
                        party=party,
                        alias_name=alias_name,
                        alias_type=PartyAlias.AliasType.OTHER,
                        source_system=source_system,
                        valid_from=valid_from,
                        defaults={
                            "valid_to": None,
                            "confidence": Decimal("0.80"),
                            "added_by": "import_ches",
                            "notes": (
                                f"Alias from {source_system} "
                                f"(source_id={party_id}"
                                + (f', partyfacts_id={partyfacts_id}' if partyfacts_id else "")
                                + f", year={year})"
                            ),
                        },
                    )
                    if a_created:
                        alias_created += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ CHES strict import completato: positioning created={created_pos}, "
                f"updated={updated_pos}, unmapped_parties={unmapped}, aliases_created={alias_created}"
            )
        )