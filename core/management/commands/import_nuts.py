from django.core.management.base import BaseCommand
from django.db import transaction
from core.models import GeoRegion, GeoRegionHierarchy
from datetime import date
import pandas as pd
import geopandas as gpd

# Mappa lunghezze NUTS per livello
NUTS_LEN = {0: 2, 1: 3, 2: 4, 3: 5}

class Command(BaseCommand):
    help = (
        "Importa NUTS RG (poligoni) dai file Eurostat RG_60M_2024_4326_LEVL_*.shp.zip. "
        "Popola geo_region e geo_region_hierarchy. "
        "Usa le colonne reali: NUTS_ID, LEVL_CODE, CNTR_CODE, NAME_LATN, NUTS_NAME."
    )

    def add_arguments(self, parser):
        parser.add_argument("paths", nargs="+", type=str,
                            help="Uno o più .shp.zip RG (qualsiasi livello 0..3)")
        parser.add_argument("--valid-from", type=str, default="2024-01-01")
        parser.add_argument("--ver", type=str, default="NUTS2024")
        parser.add_argument("--with-geom", action="store_true",
                            help="Importa geometrie (richiede campo geometry in GeoRegion)")
        parser.add_argument("--deprecate-previous", action="store_true",
                            help="Chiude versioni precedenti di stessi NUTS_ID (valid_to, is_current=False)")

    @transaction.atomic
    def handle(self, *args, **opts):
        paths = opts["paths"]
        valid_from = pd.to_datetime(opts["valid_from"]).date()
        version = opts["ver"]
        with_geom = bool(opts["with_geom"])
        deprecate_prev = bool(opts["deprecate_previous"])

        self.stdout.write(f"📥 Import NUTS RG: {len(paths)} file, geom={with_geom}, deprecate_prev={deprecate_prev}")
        frames = []
        for p in paths:
            # GeoPandas può leggere .zip direttamente
            df = gpd.read_file(p, ignore_geometry=(not with_geom))
            required = {"NUTS_ID", "LEVL_CODE", "CNTR_CODE", "NAME_LATN", "NUTS_NAME"}
            missing = required - set(df.columns)
            if missing:
                raise ValueError(f"{p}: colonne mancanti {sorted(missing)}")
            # normalizza tipi
            df["NUTS_ID"] = df["NUTS_ID"].astype(str)
            df["LEVL_CODE"] = df["LEVL_CODE"].astype(int)
            df["CNTR_CODE"] = df["CNTR_CODE"].astype(str)
            frames.append(df)

        gdf = pd.concat(frames, ignore_index=True)
        # tieni solo livelli 0..3 standard
        gdf = gdf[gdf["LEVL_CODE"].isin([0,1,2,3])].copy()

        # drop duplicati (stesso NUTS_ID/LEVL_CODE/CNTR_CODE)
        gdf = gdf.drop_duplicates(subset=["NUTS_ID", "LEVL_CODE", "CNTR_CODE"])

        created = 0
        updated = 0

        # 1) upsert geo_region
        for _, r in gdf.iterrows():
            nuts_id = r["NUTS_ID"]
            level = int(r["LEVL_CODE"])
            country = r["CNTR_CODE"]
            # coerenza lunghezza
            expect_len = NUTS_LEN[level]
            if len(nuts_id) != expect_len:
                # a volte Eurostat include entry speciali (es. EL=2 ok; ma se mismatch, forziamo truncate)
                nuts_id = nuts_id[:expect_len]

            name_official = (r.get("NAME_LATN") or r.get("NUTS_NAME") or nuts_id)
            defaults = dict(
                nuts_level=level,
                name_official=name_official,
                country_code=country,
                valid_from=valid_from,
                is_current=True,
                notes=version,
            )

            if with_geom and "geometry" in gdf.columns and hasattr(GeoRegion, "geometry"):
                defaults["geometry"] = r["geometry"]

            if deprecate_prev:
                # chiudi eventuali versioni precedenti dello stesso codice
                GeoRegion.objects.filter(nuts_code=nuts_id, nuts_level=level)\
                    .exclude(valid_from=valid_from)\
                    .update(is_current=False, valid_to=valid_from)

            obj, was_created = GeoRegion.objects.update_or_create(
                nuts_code=nuts_id,
                nuts_level=level,
                country_code=country,
                valid_from=valid_from,
                defaults=defaults
            )
            created += int(was_created)
            updated += int(not was_created)

        self.stdout.write(self.style.SUCCESS(f"✅ geo_region: created={created}, updated={updated}"))

        # 2) gerarchie: parent = code senza l’ultimo carattere
        self.stdout.write("🔗 Creo/aggiorno geo_region_hierarchy...")
        links = 0
        for level in [1,2,3]:
            sub = gdf[gdf["LEVL_CODE"] == level]
            for _, r in sub.iterrows():
                child_code = str(r["NUTS_ID"])[:NUTS_LEN[level]]  # sicurezza
                parent_code = child_code[:-1]
                parent_level = level - 1
                # lookup parent/child
                parent = GeoRegion.objects.filter(nuts_code=parent_code, nuts_level=parent_level, valid_from=valid_from).first()
                child = GeoRegion.objects.filter(nuts_code=child_code, nuts_level=level, valid_from=valid_from).first()
                if not parent or not child:
                    continue
                GeoRegionHierarchy.objects.get_or_create(
                    parent_region=parent,
                    child_region=child,
                    valid_from=valid_from,
                    defaults={"valid_to": None}
                )
                links += 1

        self.stdout.write(self.style.SUCCESS(f"✅ geo_region_hierarchy: links={links}"))
        self.stdout.write(self.style.SUCCESS("🎉 Import NUTS RG completato."))
