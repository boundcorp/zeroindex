# Generated migration for block chunking configuration

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('chains', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='chain',
            name='chunk_duration_days',
            field=models.IntegerField(default=1, help_text='Duration of each chunk in days (default: 1 day)'),
        ),
        migrations.AddField(
            model_name='chain',
            name='estimated_blocks_per_day',
            field=models.IntegerField(default=7200, help_text='Estimated blocks produced per day (Ethereum: ~7200 blocks/day)'),
        ),
        migrations.AddField(
            model_name='chain',
            name='average_block_time_seconds',
            field=models.FloatField(default=12.0, help_text='Average time between blocks in seconds (Ethereum: ~12 seconds)'),
        ),
    ]