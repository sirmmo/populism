from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0014_exportpreset'),
    ]

    operations = [
        migrations.AddField(
            model_name='exportpreset',
            name='fill_down',
            field=models.BooleanField(
                default=False,
                help_text='If True, carry the most recent prior positioning value forward when no exact year match exists.',
            ),
        ),
    ]
