from airflow.models import Variable

POSTGRES_HOST = Variable.get('POSTGRES_HOST')
POSTGRES_DB_NAME = Variable.get('POSTGRES_DB_NAME')
POSTGRES_USER = Variable.get('POSTGRES_USER')
POSTGRES_PASSWORD = Variable.get('POSTGRES_PASSWORD')

# POSTGRES_HOST = 'localhost' # 'host.docker.internal'
# POSTGRES_DB_NAME = 'challenge'
# POSTGRES_USER = 'challenge'
# POSTGRES_PASSWORD = 'challenge'