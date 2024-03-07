import pandas as pd
from google.cloud import bigquery, storage
from functions_framework import cloud_event
from google.oauth2 import service_account
import pyarrow.parquet as pq
import pyarrow as pa
import io
import db_dtypes

@cloud_event
def process_file(event):
    bucket_name = event.data["bucket"]
    file_name = event.data["name"]

    # Crea un cliente de Cloud Storage
    client = storage.Client()

    # Configura tu cliente de BigQuery
    credentials = service_account.Credentials.from_service_account_info({
    "type": "service_account",
    "project_id": "eternal-lodge-414200",
    "private_key_id": "8b0ce759ebf36c99c7759bab3f021793a4b1e9f1",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDb1fTx9w8NYO5i\nt3zkLXqGLUxOM0R3NVA54/HPqcIVmp3hjaUmpU0N7ZgWEakEaOQHFAj+8Lqo2yJD\n2ZHtFGbVLMB5duoEOygpBS0Qd2qSTmAId9mpi4M2mYMI0wkaPZ5774NdHhhUgF6v\npayfk2ujvu8a/PgkrIqJnSRzHtWfyyQB1B9uYqyE6FSG/E55zNtqL8ijNCESjuSs\n62Nr+W5nGnxOm/hvEY6KZbIuR1+0HewVD5KiVvTZFSFLauMPSJoULPVUpDBfL0d4\nJUJBY6OKKue/V8o49mMkD/rsq8On9LwYdy4u2yr7ERs/nxAcae73y1/YhQZVQkiH\nqOT7XG4ZAgMBAAECggEACXBU72gsEGcdaSXncf0dZgIvJOro2UTz3D50IRZQ9+A+\n/+KW6RM/HRZa2bS5qO+vfbnPnpGbtBkPjjH65N7jwOqmW3i+eL7I/UTkf9Dexagh\nka+CrBfKQxeBg6ka+pJTeg8JOP/sK8Gkvx4qDKJ15E6UAdjDd0CkxJ6XhG1aHv2T\nnjVL1A0/XUDrJkoX4hOFcZKTUJi0TmU/aT3dOKv6E0E11TAjwjNHy6yCjDWEPMqo\n8bkUPrwTBB5vGvSpdM4W6nhZORciE4b4ZmeX/8f2y/vUZYiQSKxs/kRuf7UD4yym\nDLB6DoGCws8zchQCsRnkd3xQR9HJwr9NU+rhrVA0MQKBgQDy/sk9/uWL6+4I3qFY\n8ex1zmpebCihSta9B/GJxqVXhuJ/4pFbAJgYppuYShMbGSmJSApvgLmNx87nyx3W\nCkXqpiWkrQ3ezG185Toq/NOYx6Sgjt2UaHWbAtrF3hTLCVbJgY1uQQPAsn7shgHh\nQ/MvoiOFyBM14O8Atx98GDLtSQKBgQDnmd5gt0B/kR3Uxrss1G6AmPu0ahq8QojO\nopL6/0x+NJvTfcX0UYdS2EOsOHdqGB1/b3Vt7pTW2aVd77lyjcHa2tkXmCsNVjUC\nhnR29CRFlOv6tlfnzV4014i5LZyjJsjeZVPrzf49Buw0XO81f+DDsJugDcb5OHe1\n1Qs58qaKUQKBgQCZtmS1C/ZHIdKs17A3JKpRB2cwHblB9qaKY2j+n6NeD9xdy4Pp\njiGojlQk7M7TOIKW7fRz/njiYD/ZTxqrmEoMGlf7qOD6TFUCSbsXEGIF5lyUmGtA\ngyfKC+86dbavjVPSGlrOIOBv2DoEAu1Tg3lla2qPKTZFwelOiYioJmKoyQKBgQCI\n+x7mATzqtn/4W1pVl4eME++7s44AjzGvVcStI9awRplrq1YrvRTW0Qalk/g9DepU\ndy3zSUtLEAuY1bLPqDxiH1KLe/rqtnQ//BpiSOAzL3OrI7I2becsRdad//ZHISdp\nMnFCZOHcYn3OMrFg6TOdSpWhQsTOnrFfIJS1P2l1IQKBgF5pDEHASJnrNY4iS9ml\nhSzpd8y5Nf9IV8HVXL7EGVz4zUKvcYp+luxNtcLId/5akP9rARgMn/JHV7J/PaR7\nAny+F2GNMzR7vZl2/DuN/vRe01rT9lnMKZ37v7peJZA5YxIlfBeyA2HZ5MRUQgFg\n+98Ze+UiElpFAWuLvKy0PBhY\n-----END PRIVATE KEY-----\n",
    "client_email": "757405264928-compute@developer.gserviceaccount.com",
    "client_id": "105814198024489023293",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/757405264928-compute%40developer.gserviceaccount.com",
    "universe_domain": "googleapis.com"
    })

    project_id = 'eternal-lodge-414200'  # Reemplaza con tu ID de proyecto

    # Comprueba si el archivo existe antes de procesarlo (optimización opcional)
    if client.bucket(bucket_name).blob(file_name).exists():
        # Download the Parquet file as binary
        content_binary = client.bucket(bucket_name).blob(file_name).download_as_bytes()
        # Verifica la longitud de los bytes descargados
        print(f"Longitud de bytes descargados: {len(content_binary)}")

        # Intenta imprimir algunos bytes para verificar su contenido
        print(content_binary[:50])

        # Realiza el ETL solo para el archivo 'user.parquet'
        try:
            parquet_file = pq.ParquetFile(io.BytesIO(content_binary))
            user = parquet_file.read().to_pandas()
            print("El archivo fue abierto con éxito")
        except Exception as e:
            print(f"Error al abrir el archivo Parquet: {e}")

            # Realiza las transformaciones específicas para user.parquet
            columns_to_drop = ['elite', 'friends', 'fans', 'compliment_more', 'compliment_profile',
                                'compliment_cute', 'compliment_list', 'compliment_note',
                                'compliment_photos', 'compliment_writer', 'compliment_hot', 'compliment_plain', 'compliment_cool', 'compliment_funny']
            user = user.drop(columns=columns_to_drop).drop_duplicates().dropna()
            user['id'] = user.index
            print('Se realizaron las transformaciones en user.parquet correctamente')

        # Configura tu cliente de BigQuery para cargar los datos transformados
        client_bq = bigquery.Client()

        # Carga la tabla con detección automática de esquemas
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        table_id = 'user'  # Puedes cambiar el nombre de la tabla según tus necesidades

        # Carga el DataFrame en BigQuery
        try:
            client_bq.load_table_from_dataframe(
            user,
            destination=f'{project_id}.{client_bq.dataset("DS_PF").dataset_id}.{table_id}',
            job_config=job_config,
        )
            print("Se añadieron registros a la tabla user correctamente en BigQuery")
        except Exception as e:
            print(f"Error al cargar datos en BigQuery: {e}")

        # Configura la tabla logs para carga incremental
        logs_dataset_id = 'DS_PF'
        logs_table_id = 'logs'

        # Define el esquema de la tabla logs
        logs_schema = [
            bigquery.SchemaField('archivo', 'STRING'),
            bigquery.SchemaField('ultimo_id', 'STRING'),
            bigquery.SchemaField('fecha', 'TIMESTAMP'),
        ]

        # Crea la tabla logs si no existe
        logs_job_config = bigquery.LoadJobConfig()
        logs_job_config.schema = logs_schema
        logs_job_config.write_disposition = bigquery.enums.WriteDisposition.WRITE_APPEND

        # Obtén el último ID cargado desde BigQuery para la tabla logs
        last_loaded_id_query = f"""
            SELECT MAX(ultimo_id) as last_loaded_id
            FROM `{project_id}.{logs_dataset_id}.{logs_table_id}`
        """
        last_loaded_id_df = client_bq.query(last_loaded_id_query).to_dataframe()
        last_loaded_id = last_loaded_id_df['last_loaded_id'][0]

        # Filtra nuevos datos basados en el ID
        if last_loaded_id is not None:
            user_last_row_id = user.iloc[-1]['user_id']
            if user_last_row_id > last_loaded_id:
                logs_df = pd.DataFrame({'archivo': [file_name], 'utimo_id': [user_last_row_id], 'fecha': [pd.Timestamp.now()]})
                client_bq.load_table_from_dataframe(
                    logs_df,
                    f'{project_id}.{logs_dataset_id}.{logs_table_id}',
                    job_config=logs_job_config,
                    if_exists='append'
                )
                print(f"Se añadió registro a la tabla logs desde {file_name} de manera incremental")
            else:
                print(f"No hay nuevos registros desde la última carga en {file_name}")
        else:
            print(f"El archivo {file_name} no es 'user.parquet', no se realizaron transformaciones.")