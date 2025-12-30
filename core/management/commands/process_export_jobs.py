# core/management/commands/process_export_jobs.py

from django.core.management.base import BaseCommand
from django.utils import timezone
from django.core.files.base import ContentFile

from core.models import DataExportJob
from core.services.extract_indicators import extract_indicators_to_csv


class Command(BaseCommand):
    help = "Processa gli export job pendenti"

    def handle(self, *args, **opts):
        jobs = DataExportJob.objects.filter(status=DataExportJob.Status.PENDING)[:1]

        for job in jobs:
            job.status = DataExportJob.Status.RUNNING
            job.started_at = timezone.now()
            job.save()

            try:
                csv_text = extract_indicators_to_csv(**job.params)
                filename = f"export_{job.id}.csv"
                job.output_file.save(filename, ContentFile(csv_text))
                job.status = DataExportJob.Status.DONE
            except Exception as e:
                job.status = DataExportJob.Status.FAILED
                job.error = str(e)

            job.finished_at = timezone.now()
            job.save()
