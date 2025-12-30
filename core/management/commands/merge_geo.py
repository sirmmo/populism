from django.core.management.base import BaseCommand
from django.db import transaction, IntegrityError
from datetime import date

from core.models import (
    GeoRegion,
    GeoRegionHierarchy,
    PartyResults,
    DataQualityIssues,
    ElectionEventRegion,
)


class Command(BaseCommand):
    help = (
        "Unisce due GeoRegion:\n"
        " - sposta tutte le FK dal 'from' al 'to'\n"
        " - aggiorna i codici in ElectionEventRegion.region\n"
        " - poi disattiva o elimina il GeoRegion sorgente.\n\n"
        "Esempio tipico: ITA-NUTS0 -> IT"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--fromcode",
            required=True,
            help="nuts_code del GeoRegion sorgente (es. 'ITA-NUTS0')",
        )
        parser.add_argument(
            "--tocode",
            required=True,
            help="nuts_code del GeoRegion target (es. 'IT')",
        )
        parser.add_argument(
            "--country",
            help="ISO3 (opzionale, per disambiguare es. ITA, NLD)",
        )
        parser.add_argument(
            "--level",
            type=int,
            help="nuts_level (0,1,2,3) opzionale, per disambiguare",
        )
        parser.add_argument(
            "--soft-delete",
            action="store_true",
            help="Invece di cancellare il GeoRegion sorgente, imposta is_current=False e valid_to=oggi",
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        from_code = opts["fromcode"]
        to_code = opts["tocode"]
        country = opts.get("country")
        level = opts.get("level")
        soft_delete = bool(opts.get("soft_delete"))

        from_qs = GeoRegion.objects.filter(nuts_code=from_code)
        to_qs = GeoRegion.objects.filter(nuts_code=to_code)

        if country:
            from_qs = from_qs.filter(country_code=country)
            to_qs = to_qs.filter(country_code=country)
        if level is not None:
            from_qs = from_qs.filter(nuts_level=level)
            to_qs = to_qs.filter(nuts_level=level)

        from_region = from_qs.order_by("-valid_from").first()
        to_region = to_qs.order_by("-valid_from").first()

        if not from_region:
            raise ValueError(f"GeoRegion sorgente non trovato per nuts_code={from_code}")
        if not to_region:
            raise ValueError(f"GeoRegion target non trovato per nuts_code={to_code}")

        if from_region.id == to_region.id:
            self.stdout.write(self.style.WARNING("from_region e to_region coincidono: niente da fare."))
            return

        self.stdout.write(
            f"Unisco GeoRegion {from_region.id} ({from_region.nuts_code}) "
            f"→ {to_region.id} ({to_region.nuts_code})"
        )

        # 1) PartyResults.region
        pr_count = PartyResults.objects.filter(region=from_region).update(region=to_region)
        self.stdout.write(f" - PartyResults aggiornati: {pr_count}")

        # 2) DataQualityIssues.region
        dqi_count = DataQualityIssues.objects.filter(region=from_region).update(region=to_region)
        self.stdout.write(f" - DataQualityIssues aggiornati: {dqi_count}")

        # 3) GeoRegionHierarchy.parent_region / child_region
        # attenzione ai possibili duplicati: gestiamo con try/except
        gh_parent = GeoRegionHierarchy.objects.filter(parent_region=from_region)
        gh_child = GeoRegionHierarchy.objects.filter(child_region=from_region)

        parent_moved = 0
        child_moved = 0

        for link in gh_parent:
            try:
                link.parent_region = to_region
                link.save()
                parent_moved += 1
            except IntegrityError:
                # esiste già un link identico con to_region -> child_region -> valid_from
                link.delete()

        for link in gh_child:
            try:
                link.child_region = to_region
                link.save()
                child_moved += 1
            except IntegrityError:
                link.delete()

        self.stdout.write(f" - GeoRegionHierarchy parent aggiornati: {parent_moved}")
        self.stdout.write(f" - GeoRegionHierarchy child aggiornati: {child_moved}")

        # 4) ElectionEventRegion.region (CharField) – aggiorna il codice
        eer_count = ElectionEventRegion.objects.filter(region=from_region.nuts_code).update(
            region=to_region.nuts_code
        )
        self.stdout.write(f" - ElectionEventRegion.region aggiornati: {eer_count}")

        # 5) disattiva o elimina il GeoRegion sorgente
        if soft_delete:
            from_region.is_current = False
            from_region.valid_to = date.today()
            from_region.notes = (from_region.notes or "") + f"\nMerged into {to_region.id} on {date.today().isoformat()}"
            from_region.save()
            self.stdout.write(self.style.SUCCESS(f"GeoRegion sorgente marcato come non corrente (id={from_region.id})."))
        else:
            from_region.delete()
            self.stdout.write(self.style.SUCCESS(f"GeoRegion sorgente eliminato (id={from_region.id})."))

        self.stdout.write(self.style.SUCCESS("Merge GeoRegion completato."))
