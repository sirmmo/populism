import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0015_exportpreset_fill_down'),
    ]

    operations = [
        # Enhance PartyFamily: add description, unique name, db_table, verbose names
        migrations.AddField(
            model_name='partyfamily',
            name='description',
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='partyfamily',
            name='name',
            field=models.CharField(max_length=200, unique=True),
        ),
        migrations.AlterModelOptions(
            name='partyfamily',
            options={
                'verbose_name': 'Macro-Party Grouping',
                'verbose_name_plural': 'Macro-Party Groupings',
            },
        ),
        migrations.AlterModelTable(
            name='partyfamily',
            table='party_family',
        ),
        # Link PartyRegistry -> PartyFamily
        migrations.AddField(
            model_name='partyregistry',
            name='macro_party',
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name='parties',
                to='core.partyfamily',
                help_text='Macro-party grouping for cross-time analysis of party transformations.',
            ),
        ),
        # Add group_by_macro_party checkbox to ExportPreset
        migrations.AddField(
            model_name='exportpreset',
            name='group_by_macro_party',
            field=models.BooleanField(
                default=False,
                help_text='If True, include macro-party grouping columns (macro_party_id, macro_party_name) in the export.',
            ),
        ),
    ]
