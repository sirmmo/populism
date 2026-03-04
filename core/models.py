from django.db import models
from datetime import date

class PartyFamily(models.Model):
    id=models.AutoField(primary_key=True)
    name = models.CharField(max_length=200)
    

class PartyRegistry(models.Model):
    class Status(models.TextChoices):
        ACTIVE = "active", "Active"
        MERGED = "merged", "Merged"
        SPLIT = "split", "Split"
        RENAMED = "renamed", "Renamed"
        DISSOLVED = "dissolved", "Dissolved"

    id = models.AutoField(primary_key=True)  # party_uid
    canonical_name = models.TextField()
    short_name = models.TextField(blank=True, null=True)
    country_code = models.CharField(max_length=3)  # ISO3, e.g. NLD, ITA, EU
    valid_from = models.DateField()
    valid_to = models.DateField(blank=True, null=True)  # null = still valid
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.ACTIVE,
    )
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "party_registry"

    def __str__(self):
        return f"{self.short_name or self.canonical_name} ({self.country_code})"


class PartyAlias(models.Model):
    class AliasType(models.TextChoices):
        SHORT_NAME = "short_name", "Short Name"
        TRANSLATION = "translation", "Translation"
        HISTORICAL = "historical_name", "Historical Name"
        TYPO = "typo", "Typo / Misspelling"
        COALITION = "coalition_label", "Coalition Label"
        OTHER = "other", "Other"

    id = models.AutoField(primary_key=True)
    party = models.ForeignKey(
        PartyRegistry,
        on_delete=models.CASCADE,
        related_name="aliases",
    )
    alias_name = models.TextField()
    alias_type = models.CharField(
        max_length=32,
        choices=AliasType.choices,
        default=AliasType.OTHER,
    )
    source_system = models.CharField(max_length=64)  # e.g. "CHES", "Manifesto", "EU_NED"
    source_system_id = models.CharField(max_length=200, null=True, blank=True)
    valid_from = models.DateField()
    valid_to = models.DateField(blank=True, null=True)
    confidence = models.FloatField(
        
    )  # 0.00 - 1.00
    added_by = models.CharField(
        max_length=128, blank=True, null=True
    )  # "marco", "auto_matcher", etc.
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "party_alias"
        indexes = [
            models.Index(fields=["alias_name", "source_system", "valid_from"]),
        ]

    def __str__(self):
        return f"{self.alias_name} -> {self.party_id}"


class PartySourceMap(models.Model):
    id = models.AutoField(primary_key=True)
    party = models.ForeignKey(
        PartyRegistry,
        on_delete=models.CASCADE,
        related_name="source_maps",
    )
    source_system = models.CharField(
        max_length=64
    )  # "CHES", "Manifesto", "ParlGov", "PartyFacts", etc.
    source_party_id = models.CharField(
        max_length=128
    )  # the ID or code used in that source
    valid_from = models.DateField()
    valid_to = models.DateField(blank=True, null=True)
    confidence = models.FloatField()
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "party_source_map"
        constraints = [
            models.UniqueConstraint(
                fields=["source_system", "source_party_id", "valid_from"],
                name="uniq_party_source_period",
            )
        ]

    def __str__(self):
        return f"{self.source_system}:{self.source_party_id} -> {self.party_id}"


class GeoRegion(models.Model):
    id = models.AutoField(primary_key=True)  # region_uid
    nuts_code = models.CharField(max_length=32, null=True, blank=True)  # e.g. "NL11", "NL111", "NLD-NUTS0"
    nuts_level = models.SmallIntegerField()  # 0,1,2,3
    name_official = models.TextField(null=True, blank=True)
    country_code = models.CharField(max_length=3, null=True, blank=True)  # ISO3 like NLD, ITA
    valid_from = models.DateField()
    valid_to = models.DateField(blank=True, null=True)
    is_current = models.BooleanField(default=True)
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "geo_region"
        indexes = [
            models.Index(fields=["nuts_code", "valid_from"]),
            models.Index(fields=["country_code", "nuts_level", "is_current"]),
        ]
        # Nota: potremmo voler evitare due region_uid diversi con stesso nuts_code + stesso periodo.
        # Questo si può approssimare con UniqueConstraint parziale in Postgres, ma Django non supporta
        # nativamente partial unique senza RawSQL. Lo lasciamo documentato.


    def __str__(self):
        return f"{self.nuts_code} ({self.name_official})"


class GeoRegionHierarchy(models.Model):
    parent_region = models.ForeignKey(
        GeoRegion,
        on_delete=models.CASCADE,
        related_name="children_links",
    )
    child_region = models.ForeignKey(
        GeoRegion,
        on_delete=models.CASCADE,
        related_name="parent_links",
    )
    valid_from = models.DateField()
    valid_to = models.DateField(blank=True, null=True)

    class Meta:
        db_table = "geo_region_hierarchy"
        unique_together = (
            "parent_region",
            "child_region",
            "valid_from",
        )

    def __str__(self):
        return f"{self.parent_region_id} -> {self.child_region_id}"


class ElectionEvent(models.Model):
    class ElectionType(models.TextChoices):
        EUROPEAN_PARLIAMENT = "european_parliament", "European Parliament"
        NATIONAL_PARLIAMENT = "national_parliament", "National Parliament"
        REGIONAL = "regional", "Regional"
        PROVINCIAL = "provincial", "Provincial"
        LOCAL = "local", "Local"
        OTHER = "other", "Other"

    id = models.AutoField(primary_key=True)  # election_id
    country_code = models.CharField(max_length=3)  # ISO3
    election_type = models.CharField(
        max_length=32, choices=ElectionType.choices
    )
    election_year = models.IntegerField(null=True, blank=True)
    election_date = models.DateField(null=True, blank=True)
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "election_event"
        indexes = [
            models.Index(fields=["election_date"]),
            models.Index(fields=["country_code", "election_type"]),
        ]

    def __str__(self):
        return f"{self.country_code} {self.election_type} {self.election_date}"


class ElectionEventRegion(models.Model):
    election = models.ForeignKey(
        ElectionEvent,
        on_delete=models.CASCADE,
        related_name="reporting_regions",
    )
    region = models.CharField(max_length=200)
    is_reporting_unit = models.BooleanField(default=True)
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "election_event_region"
        unique_together = ("election", "region")

    def __str__(self):
        return f"Election {self.election} / Region {self.region}"


class PartyResults(models.Model):
    class QualityFlag(models.TextChoices):
        OK = "ok", "OK"
        SUSPECT = "suspect", "Suspect"
        MANUAL_OVERRIDE = "manual_override", "Manual Override"

    id = models.AutoField(primary_key=True)  # result_id
    party = models.ForeignKey(
        PartyRegistry,
        on_delete=models.CASCADE,
        related_name="results",
    )
    election = models.ForeignKey(
        ElectionEvent,
        on_delete=models.CASCADE,
        related_name="results",
    )
    region = models.ForeignKey(
        GeoRegion,
        on_delete=models.CASCADE,
        related_name="results",
    )

    votes_pct = models.FloatField(
        blank=True, null=True
    )  # e.g. 17.532
    seats = models.IntegerField(blank=True, null=True)
    turnout_pct = models.FloatField(
        blank=True, null=True
    )

    source_system = models.CharField(
        max_length=64
    )  # "EU_NED", "ministero_interno", etc.
    observed_at = models.DateTimeField()  # quando abbiamo registrato questo valore
    quality_flag = models.CharField(
        max_length=16,
        choices=QualityFlag.choices,
        default=QualityFlag.OK,
    )
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "party_results"
        indexes = [
            models.Index(fields=["election", "region", "party"]),
            models.Index(fields=["source_system", "observed_at"]),
        ]

    def __str__(self):
        return (
            f"Result p:{self.party_id} e:{self.election_id} r:{self.region_id}"
        )

class PartyPositioning(models.Model):
    id = models.AutoField(primary_key=True)  # positioning_id
    party = models.ForeignKey(
        PartyRegistry,
        on_delete=models.CASCADE,
        related_name="positioning",
    )
    dimension = models.CharField(
        max_length=64
    )  # e.g. "lr_general", "eu_integration", "immigration"
    value = models.FloatField()
    source_system = models.CharField(
        max_length=64
    )  # "CHES", "ManifestoScaling", etc.
    valid_from = models.DateField()
    valid_to = models.DateField(blank=True, null=True)
    confidence = models.FloatField()
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "party_positioning"
        indexes = [
            models.Index(fields=["party", "dimension", "valid_from"]),
        ]
        verbose_name_plural = "Party Positioning"

    def __str__(self):
        return f"{self.party_id} {self.dimension}={self.value}"


class DataQualityIssues(models.Model):
    class Severity(models.TextChoices):
        ERROR = "error", "Error"
        WARNING = "warning", "Warning"
        INFO = "info", "Info"

    id = models.AutoField(primary_key=True)  # issue_id
    detected_at = models.DateTimeField(auto_now_add=True)
    severity = models.CharField(
        max_length=16,
        choices=Severity.choices,
    )
    issue_type = models.CharField(
        max_length=64
    )  # e.g. "INCONSISTENT_AGG", "AMBIGUOUS_MAPPING", etc.

    # Context: not all are required at the same time
    party = models.ForeignKey(
        PartyRegistry,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="quality_issues",
    )
    election = models.ForeignKey(
        ElectionEvent,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="quality_issues",
    )
    region = models.ForeignKey(
        GeoRegion,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="quality_issues",
    )
    source_system = models.CharField(
        max_length=64, blank=True, null=True
    )

    details = models.TextField()  # human-readable description of the issue

    class Meta:
        db_table = "data_quality_issues"
        indexes = [
            models.Index(fields=["severity", "issue_type"]),
            models.Index(fields=["party"]),
            models.Index(fields=["election"]),
            models.Index(fields=["region"]),
        ]

    def __str__(self):
        return f"[{self.severity}] {self.issue_type}"

class DimensionDescriptor(models.Model):
    source_system = models.CharField(
        max_length=64, blank=True, null=True
    )
    macro = models.CharField(
        max_length=64, blank=True, null=True
    )
    macro_label = models.CharField(
        max_length=64, blank=True, null=True
    )
    variable = models.CharField(
        max_length=64, blank=True, null=True
    )
    title = models.CharField(
        max_length=200, blank=True, null=True
    )
    description = models.TextField( blank=True, null=True
    )
    label = models.CharField(max_length=300, null=True, blank=True)

    def __str__(self):
        return f"{self.variable}"

class Coalition(models.Model):
    """
    Rappresenta una coalizione in un contesto elettorale specifico.
    Il 'contenitore' È un party già presente in PartyRegistry,
    che nei dati di origine appare come lista/coalizione.
    """

    id = models.AutoField(primary_key=True)

    # Il partito che rappresenta la coalizione nei dati (già esistente in PartyRegistry)
    coalition_party = models.ForeignKey(
        PartyRegistry,
        on_delete=models.CASCADE,
        related_name="as_coalition",
        help_text="PartyRegistry che rappresenta la coalizione (lista coalizionale).",
    )

    # Contesto elettorale in cui questa coalizione è definita
    election = models.ForeignKey(
        ElectionEvent,
        on_delete=models.CASCADE,
        related_name="coalitions",
        help_text="Elezione in cui questa coalizione è rilevante.",
    )

    # opzionale, ma spesso utile per filtri veloci
    country_code = models.CharField(
        max_length=3,
        blank=True,
        null=True,
        help_text="Ridondante (di solito = coalition_party.country_code), ma utile per filtri.",
    )

    valid_from = models.DateField(default=date(1900, 1, 1))
    valid_to = models.DateField(blank=True, null=True)

    source_system = models.CharField(max_length=64, blank=True, null=True)  # "EU_NED", "ministero_interno", etc.
    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "coalition"
        indexes = [
            models.Index(fields=["country_code", "election"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["coalition_party", "election"],
                name="uniq_coalition_party_election",
            )
        ]

    def __str__(self):
        return f"Coalition {self.coalition_party} @ {self.election}"

class CoalitionMembership(models.Model):
    class WeightType(models.TextChoices):
        EQUAL = "equal", "Equal Share"
        VOTES = "votes", "Votes-based"
        SEATS = "seats", "Seats-based"
        CUSTOM = "custom", "Custom Weight"

    id = models.AutoField(primary_key=True)

    coalition = models.ForeignKey(
        Coalition,
        on_delete=models.CASCADE,
        related_name="memberships",
    )

    # Il partito membro (anch'esso in PartyRegistry)
    member_party = models.ForeignKey(
        PartyRegistry,
        on_delete=models.CASCADE,
        related_name="coalition_memberships",
    )

    weight_type = models.CharField(
        max_length=16,
        choices=WeightType.choices,
        default=WeightType.EQUAL,
        help_text="Modo con cui ripartire i voti della coalizione sui membri.",
    )

    # Se CUSTOM, questa è la quota normalizzata 0–1.
    weight_share = models.DecimalField(
        max_digits=6,
        decimal_places=5,
        blank=True,
        null=True,
        help_text="Quota 0–1 (se CUSTOM) oppure quota normalizzata calcolata da VOTES/SEATS.",
    )

    # Valore grezzo usato per derivare la share (es. voti nazionali, seggi…)
    raw_weight = models.DecimalField(
        max_digits=12,
        decimal_places=3,
        blank=True,
        null=True,
        help_text="Valore grezzo (es. voti, seggi) usato per calcolare la share.",
    )

    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "coalition_membership"
        unique_together = ("coalition", "member_party")

    def __str__(self):
        return f"{self.coalition} -> {self.member_party}"

from django.contrib.auth import get_user_model

User = get_user_model()


class DataExportJob(models.Model):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        RUNNING = "running", "Running"
        DONE = "done", "Done"
        FAILED = "failed", "Failed"

    id = models.AutoField(primary_key=True)
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(blank=True, null=True)
    finished_at = models.DateTimeField(blank=True, null=True)

    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)

    job_type = models.CharField(max_length=64)  # es. "extract_indicators_csv"
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.PENDING)

    # parametri serializzati (quelli dell’endpoint)
    params = models.JSONField()

    # file finale
    output_file = models.FileField(upload_to="exports/", blank=True, null=True)

    # error/debug
    error = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "data_export_job"
        indexes = [models.Index(fields=["status", "job_type", "created_at"])]

    def __str__(self):
        return f"{self.job_type} #{self.id} ({self.status})"


class ExportPreset(models.Model):
    """
    Configurazione salvabile per un export 'extract_indicators'.
    """
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=200, unique=True)

    # params principali (matchano la funzione extract_indicators_to_csv)
    date_from = models.DateField()

    country = models.CharField(max_length=3, blank=True, null=True)  # ISO3
    election_type = models.CharField(max_length=32, blank=True, null=True)

    nuts_level = models.PositiveSmallIntegerField(blank=True, null=True)  # 0..3
    regions = models.JSONField(blank=True, null=True, help_text="Lista di NUTS code (es. ['ITC1','ITC2'])")

    indicators = models.JSONField(blank=True, null=True, help_text="Lista dimensioni (es. ['rile','per108'])")
    positioning_source = models.CharField(max_length=64, blank=True, null=True)  # CHES, Manifesto, etc.

    split_coalitions = models.BooleanField(default=False)
    include_original_coalition = models.BooleanField(default=False)
    fill_down = models.BooleanField(
        default=False,
        help_text="If True, carry the most recent prior positioning value forward when no exact year match exists.",
    )

    notes = models.TextField(blank=True, null=True)

    class Meta:
        db_table = "export_preset"
        indexes = [
            models.Index(fields=["country", "nuts_level"]),
        ]

    def __str__(self):
        return self.name

    def to_job_params(self):
        return {
            "date_from": self.date_from.isoformat(),
            "indicators": self.indicators or None,
            "country": self.country or None,
            "election_type": self.election_type or None,
            "nuts_level": self.nuts_level,
            "regions": self.regions or None,
            "positioning_source": self.positioning_source or None,
            "split_coalitions": self.split_coalitions,
            "include_original_coalition": self.include_original_coalition,
            "fill_down": self.fill_down,
        }
