from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('contract', '0020_auto_20230126_0903'),
    ]
    operations = [
        migrations.RunSQL("""
            CREATE SEQUENCE IF NOT EXISTS public.contract_code_seq
            INCREMENT 1
            START 1
            MINVALUE 1
            MAXVALUE 9223372036854775807
            CACHE 1;
        """, reverse_sql="""DROP SEQUENCE IF EXISTS public.contract_code_seq;"""),
    ]
