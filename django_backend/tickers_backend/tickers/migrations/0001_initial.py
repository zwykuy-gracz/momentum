# Generated by Django 5.1.6 on 2025-02-24 22:52

from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='August05Best',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('pct_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'august05_best',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='August05Worst',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('pct_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'august05_worst',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ListOfTickers',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ticker', models.CharField(max_length=20)),
                ('nasdaq_tickers', models.CharField(max_length=20)),
                ('nyse_tickers', models.CharField(max_length=20)),
            ],
            options={
                'db_table': 'list_of_tickers',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ListOfTickersLt10B',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('ticker', models.CharField(max_length=20)),
                ('nasdaq_tickers', models.CharField(max_length=20)),
                ('nyse_tickers', models.CharField(max_length=20)),
            ],
            options={
                'db_table': 'list_of_tickers_lt_10B',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='ListOfTickersLt1B',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('ticker', models.CharField(max_length=20)),
                ('market_cap', models.FloatField()),
                ('nasdaq_tickers', models.BooleanField()),
                ('nyse_tickers', models.BooleanField()),
            ],
            options={
                'db_table': 'list_of_tickers_lt_1B',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='MonthlyChange',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('one_month_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ('three_months_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ('six_months_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ('twelve_months_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'monthly_change',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='November05Best',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('pct_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'november05_best',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='November05Worst',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('pct_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'november05_worst',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='StockData',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('close', models.FloatField()),
                ('high', models.FloatField()),
                ('low', models.FloatField()),
                ('open', models.FloatField()),
                ('volume', models.BigIntegerField()),
                ('ytd', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ('august05', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ('ma50', models.FloatField(blank=True, null=True)),
                ('ma50_above', models.BooleanField(blank=True, null=True)),
                ('ma100', models.FloatField(blank=True, null=True)),
                ('ma100_above', models.BooleanField(blank=True, null=True)),
                ('ma200', models.FloatField(blank=True, null=True)),
                ('ma200_above', models.BooleanField(blank=True, null=True)),
                ('november05', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
                ('weekly_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'stock_data',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='WeeklyChangeBest',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('pct_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'weekly_change_best',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='WeeklyChangeWorst',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('pct_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'weekly_change_worst',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='YtdBest',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('pct_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'ytd_best',
                'managed': False,
            },
        ),
        migrations.CreateModel(
            name='YtdWorst',
            fields=[
                ('id', models.AutoField(primary_key=True, serialize=False)),
                ('date', models.DateField()),
                ('ticker', models.CharField(max_length=20)),
                ('pct_change', models.DecimalField(blank=True, decimal_places=2, max_digits=10, null=True)),
            ],
            options={
                'db_table': 'ytd_worst',
                'managed': False,
            },
        ),
    ]
