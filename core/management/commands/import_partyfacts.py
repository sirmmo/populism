from django.core.management.base import BaseCommand
from django.db import transaction
from datetime import date
from decimal import Decimal
import pandas as pd
import math

from core.models import (
    PartyRegistry, PartyAlias, PartySourceMap, DataQualityIssues
)


def _pf_id(x):
    """Normalizza partyfacts_id: float/int -> string, NaN -> None."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    try:
        return str(int(float(x)))
    except Exception:
        return str(x).strip() or None


class Command(BaseCommand):
    help = (
        "Importa PartyFacts come registro master:\n"
        "- partyfacts-core-parties.csv per creare/aggiornare PartyRegistry + PartySourceMap(PartyFacts)\n"
        "- partyfacts-external-parties.csv per creare/aggiornare mapping verso CHES/Manifesto/ParlGov ecc."
    )

    def add_arguments(self, parser):
        parser.add_argument("--core", required=True, type=str,
                            help="Path a partyfacts-core-parties.csv")
        parser.add_argument("--external", required=True, type=str,
                            help="Path a partyfacts-external-parties.csv")
        parser.add_argument("--valid-from", default="1945-01-01", type=str,
                            help="Data di inizio validità predefinita per i mapping (YYYY-MM-DD)")

    @transaction.atomic
    def handle(self, *args, **opts):
        core_path = opts["core"]
        ext_path = opts["external"]
        valid_from = pd.to_datetime(opts["valid_from"]).date()

        self.stdout.write(f"📥 Import PartyFacts core da {core_path}")
        core = pd.read_csv(core_path, low_memory=False)

        required_core = {"country", "partyfacts_id", "name_short", "name", "name_english"}
        missing = required_core - set(core.columns)
        if missing:
            raise ValueError(f"Colonne mancanti in core CSV: {sorted(missing)}")

        cores_created = 0
        cores_updated = 0
        maps_created = 0
        alias_created = 0
        issues = 0

        # 1) CORE PARTIES → PartyRegistry + PartySourceMap(PartyFacts) + alias
        for _, row in core.iterrows():
            pfid = _pf_id(row["partyfacts_id"])
            if not pfid:
                continue

            country = str(row["country"]).strip().upper() if not pd.isna(row["country"]) else None
            name_short = None if pd.isna(row.get("name_short")) else str(row["name_short"]).strip()
            name = None if pd.isna(row.get("name")) else str(row["name"]).strip()
            name_eng = None if pd.isna(row.get("name_english")) else str(row["name_english"]).strip()
            name_other = None if pd.isna(row.get("name_other")) else str(row["name_other"]).strip()
            year_first = int(row["year_first"]) if not pd.isna(row.get("year_first")) else 1945

            canonical = name_eng or name or name_short or f"PF:{country}:{pfid}"

            # se esiste già un PartySourceMap PartyFacts, usiamo quello come party principale
            psm = PartySourceMap.objects.filter(
                source_system="PartyFacts",
                source_party_id=pfid
            ).select_related("party").first()

            if psm:
                party = psm.party
                # aggiorna campi base se più ricchi
                updated = False
                if not party.short_name and name_short:
                    party.short_name = name_short
                    updated = True
                if party.canonical_name.startswith("PF:") and canonical:
                    party.canonical_name = canonical
                    updated = True
                if updated:
                    party.save()
                    cores_updated += 1
            else:
                # crea nuovo PartyRegistry principale
                party = PartyRegistry.objects.create(
                    canonical_name=canonical,
                    short_name=name_short or None,
                    country_code=country or "UNK",
                    valid_from=date(year_first, 1, 1),
                    status=PartyRegistry.Status.ACTIVE,
                    notes="Created from PartyFacts core",
                )
                cores_created += 1

                PartySourceMap.objects.create(
                    party=party,
                    source_system="PartyFacts",
                    source_party_id=pfid,
                    valid_from=valid_from,
                    confidence=Decimal("1.0"),
                    notes="Master mapping PartyFacts core"
                )
                maps_created += 1

            # alias da nomi diversi
            alias_names = set()
            for alias_name in [name, name_eng, name_other]:
                if alias_name and alias_name != canonical:
                    alias_names.add(alias_name)

            for an in alias_names:
                PartyAlias.objects.get_or_create(
                    party=party,
                    alias_name=an,
                    alias_type=PartyAlias.AliasType.OTHER,
                    source_system="PartyFacts",
                    source_system_id = pfid,
                    valid_from=valid_from,
                    defaults={
                        "valid_to": None,
                        "confidence": Decimal("0.9"),
                        "added_by": "import_partyfacts",
                        "notes": "Alias from PartyFacts core"
                    }
                )
                alias_created += 1

        self.stdout.write(self.style.SUCCESS(
            f"✅ PartyFacts core: parties created={cores_created}, "
            f"updated={cores_updated}, PF-maps created={maps_created}, aliases={alias_created}"
        ))

        # 2) EXTERNAL PARTIES → mapping dataset_key → partyfacts (→ PartyRegistry)
        self.stdout.write(f"📥 Import PartyFacts external da {ext_path}")
        ext = pd.read_csv(ext_path, low_memory=False)

        required_ext = {"dataset_key", "dataset_party_id", "partyfacts_id"}
        missing_ext = required_ext - set(ext.columns)
        if missing_ext:
            raise ValueError(f"Colonne mancanti in external CSV: {sorted(missing_ext)}")

        ext_maps_created = 0
        ext_alias_created = 0

        for _, row in ext.iterrows():
            pfid = _pf_id(row.get("partyfacts_id"))
            if not pfid:
                # esterno non ancora linkato a PartyFacts
                continue

            dataset_key = str(row["dataset_key"]).strip().lower()
            dataset_party_id_raw = row.get("dataset_party_id")
            if pd.isna(dataset_party_id_raw):
                continue
            dataset_party_id = str(dataset_party_id_raw).strip()

            # normalizza nome sistema per coerenza (puoi adattare mapping se vuoi)
            system_map = {
                "manifesto": "Manifesto",
                "ches": "CHES",
                "parlgov": "ParlGov",
            }
            source_system = system_map.get(dataset_key, dataset_key)

            # trova il party principale via PartyFacts
            psm_pf = PartySourceMap.objects.filter(
                source_system="PartyFacts",
                source_party_id=pfid
            ).select_related("party").first()

            if not psm_pf:
                DataQualityIssues.objects.create(
                    severity=DataQualityIssues.Severity.WARNING,
                    issue_type="PF_EXTERNAL_WITHOUT_CORE",
                    details=f"External {dataset_key}:{dataset_party_id} ha PF={pfid} ma no core mapping",
                    source_system="PartyFacts",
                )
                issues += 1
                continue

            party = psm_pf.party

            # crea mapping verso dataset esterno
            PartySourceMap.objects.get_or_create(
                party=party,
                source_system=source_system,
                source_party_id=dataset_party_id,
                valid_from=valid_from,
                defaults={
                    "valid_to": None,
                    "confidence": Decimal("0.95"),
                    "notes": f"Linked via PartyFacts external ({dataset_key})"
                }
            )
            ext_maps_created += 1

            # alias da nome_short / name / name_english se presenti
            name_short = None if pd.isna(row.get("name_short")) else str(row["name_short"]).strip()
            name = None if pd.isna(row.get("name")) else str(row["name"]).strip()
            name_eng = None if pd.isna(row.get("name_english")) else str(row["name_english"]).strip()

            alias_names = set()
            for an in [name_short, name, name_eng]:
                if an and an not in {party.canonical_name, party.short_name}:
                    alias_names.add(an)

            for an in alias_names:
                PartyAlias.objects.get_or_create(
                    party=party,
                    alias_name=an,
                    alias_type=PartyAlias.AliasType.SHORT_NAME if an == name_short else PartyAlias.AliasType.OTHER,
                    source_system=source_system,
                    source_system_id = dataset_party_id,
                    valid_from=valid_from,
                    defaults={
                        "valid_to": None,
                        "confidence": Decimal("0.8"),
                        "added_by": "import_partyfacts",
                        "notes": f"Alias from PartyFacts external ({dataset_key})"
                    }
                )
                ext_alias_created += 1

        self.stdout.write(self.style.SUCCESS(
            f"✅ PartyFacts external: ext-maps created={ext_maps_created}, ext-aliases={ext_alias_created}, issues={issues}"
        ))
        self.stdout.write(self.style.SUCCESS("🎉 PartyFacts import completato (master registry pronto)."))
