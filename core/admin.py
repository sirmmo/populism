from django.contrib import admin, messages

# Register your models here.

from core.models import * 

class PartyAliasInline(admin.TabularInline):
    model = PartyAlias
class PartySourceMapInline(admin.TabularInline):
    model = PartySourceMap
class PartyResultsInline(admin.TabularInline):
    model = PartyResults

class PartyRegistryAdmin(admin.ModelAdmin):
    list_display=["id", "canonical_name", "short_name", "country_code", "status"]
    list_filter=["country_code"]
    inlines = [
        PartyAliasInline,
        PartySourceMapInline
    ]
admin.site.register(PartyRegistry, PartyRegistryAdmin)


admin.site.register(PartyAlias)

class PartySourceMapAdmin(admin.ModelAdmin):
    list_display = ["id", "party", "source_system", "source_party_id"]
    list_filter = ["source_system", "party__country_code"] 
admin.site.register(PartySourceMap, PartySourceMapAdmin)


class GeoRegionAdmin(admin.ModelAdmin):
    search_fields=["nuts_code", "name_official"]
    list_display=["id", "nuts_level", "nuts_code", "name_official", "country_code"]
    list_filter=["country_code", "nuts_level"]
admin.site.register(GeoRegion, GeoRegionAdmin)
admin.site.register(GeoRegionHierarchy)

class ElectionEventAdmin(admin.ModelAdmin):
    list_display=["id",'country_code','election_type', 'election_year', 'election_date']
    search_fields=["election_year"]
    list_filter=["country_code", "election_type", "election_year"]
    inlines=[
        PartyResultsInline
    ]
admin.site.register(ElectionEvent, ElectionEventAdmin)
class ElectionEventRegionAdmin(admin.ModelAdmin):
    list_display=['id', 'election', 'region', 'is_reporting_unit']
admin.site.register(ElectionEventRegion, ElectionEventRegionAdmin)

class PartyResultsdmin(admin.ModelAdmin):
    search_fields=["party"]
    list_display=["id", "party", "election", "region", "votes_pct", "seats", "turnout_pct", "source_system"]
    list_filter=["party", "election", "region", "party__country_code"]
admin.site.register(PartyResults, PartyResultsdmin)

class PartyPositioningAdmin(admin.ModelAdmin):
    list_display=["id",'party','dimension', 'value', 'source_system', 'valid_from']
    search_fields=["dimension"]
    list_filter=["dimension", "party__country_code", "party", "source_system", "valid_from"]
admin.site.register(PartyPositioning, PartyPositioningAdmin)


class DataQualityIssuesAdmin(admin.ModelAdmin):
    list_display=["id",'severity','issue_type', 'source_system', 'details', 'party', 'election', 'region']
    search_fields=["details"]
    list_filter=["severity", "issue_type", "source_system", "party", "election", "region"]

admin.site.register(DataQualityIssues, DataQualityIssuesAdmin)
admin.site.register(DimensionDescriptor)


from core.services.coalitions import coalition_split_csv
from django.http import HttpResponse
from django.utils.text import slugify


@admin.action(description="Export split results CSV")
def export_split_csv(modeladmin, request, queryset):
    if queryset.count() != 1:
        messages.error(request, "Seleziona esattamente UNA coalizione per esportare il CSV.")
        return

    coalition = queryset.first()

    try:
        csv_text = coalition_split_csv(coalition, include_original=False)
    except Exception as e:
        messages.error(request, f"Errore export CSV: {e}")
        return

    # filename carino
    cname = coalition.coalition_party.short_name or coalition.coalition_party.canonical_name
    fname = f"coalition_split_{coalition.country_code or coalition.election.country_code}_{coalition.election_id}_{slugify(cname) or coalition.id}.csv"

    resp = HttpResponse(csv_text, content_type="text/csv; charset=utf-8")
    resp["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp



class CoalitionAdmin(admin.ModelAdmin):
    list_display=["id", "coalition_party", "election", "country_code", "valid_from", "valid_to", "source_system"]
    search_fields = ("coalition_party__canonical_name", "coalition_party__short_name", "country_code")
    list_filter = ("country_code", "source_system", "election__election_type")
    actions = [export_split_csv]

admin.site.register(Coalition, CoalitionAdmin)

class CoalitionMembershipAdmin(admin.ModelAdmin):
    list_display=["id", "coalition__coalition_party", "member_party", "weight_type"]

admin.site.register(CoalitionMembership, CoalitionMembershipAdmin)

class ExportJobAdmin(admin.ModelAdmin):
    list_display = ("id", "status", "created_at", "finished_at", "download")
    readonly_fields = ("status", "output_file", "error")

    def download(self, obj):
        if obj.output_file:
            return format_html('<a href="{}">Download</a>', obj.output_file.url)
        return "-"

admin.site.register(DataExportJob, ExportJobAdmin)


from django.contrib import admin, messages
from django.urls import path, reverse
from django.shortcuts import redirect
from django.utils import timezone
from django.utils.html import format_html

from core.models import ExportPreset, DataExportJob


@admin.register(ExportPreset)
class ExportPresetAdmin(admin.ModelAdmin):
    list_display = (
        "name", "date_from", "country", "nuts_level",
        "split_coalitions", "positioning_source", "queue_button"
    )
    list_filter = ("country", "nuts_level", "split_coalitions", "positioning_source")
    search_fields = ("name", "notes")
    readonly_fields = ()

    fieldsets = (
        ("Base", {
            "fields": ("name", "date_from", "country", "election_type")
        }),
        ("Territorio", {
            "fields": ("nuts_level", "regions"),
            "description": "regions è una lista JSON di codici NUTS, es. ['ITC1','ITC2']"
        }),
        ("Indicatori", {
            "fields": ("indicators", "positioning_source"),
            "description": "indicators è una lista JSON, es. ['rile','people_vs_elite','per108']"
        }),
        ("Coalizioni", {
            "fields": ("split_coalitions", "include_original_coalition"),
        }),
        ("Note", {
            "fields": ("notes",),
        }),
    )

    actions = ["queue_export_from_preset"]

    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path(
                "<int:preset_id>/queue/",
                self.admin_site.admin_view(self.queue_export_view),
                name="exportpreset_queue",
            ),
        ]
        return custom + urls

    def queue_button(self, obj):
        url = reverse("admin:exportpreset_queue", args=[obj.id])
        return format_html('<a class="button" href="{}">Queue export</a>', url)
    queue_button.short_description = "Run"
    queue_button.allow_tags = True

    def queue_export_view(self, request, preset_id: int):
        preset = ExportPreset.objects.get(id=preset_id)

        job = DataExportJob.objects.create(
            status=DataExportJob.Status.PENDING,
            params=preset.to_job_params(),
        )
        messages.success(request, f"Export job #{job.id} creato (PENDING).")
        return redirect("admin:core_exportjob_changelist")  # cambia namespace/app se diverso

    @admin.action(description="Queue export for selected presets")
    def queue_export_from_preset(self, request, queryset):
        count = 0
        for preset in queryset:
            DataExportJob.objects.create(
                status=DataExportJob.Status.PENDING,
                params=preset.to_job_params(),
            )
            count += 1
        messages.success(request, f"Creati {count} export job (PENDING).")
