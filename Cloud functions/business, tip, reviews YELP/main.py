import pandas as pd
from google.cloud import bigquery, storage
from functions_framework import cloud_event
from google.oauth2 import service_account
from pandas_gbq import gbq
import google.cloud
import json
import pyarrow.parquet as pq
import pyarrow as pa


@cloud_event
def process_file(event):
    bucket_name = event.data["bucket"]
    file_name = event.data["name"]
    
    # Crea un cliente de Cloud Storage
    client = storage.Client()
    # Configura tu cliente de BigQuery
    client_bq = bigquery.Client()
    dataset_id = 'DS_PF'
    table_id = file_name.split('.')[0]
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    table_ref = client_bq.dataset(dataset_id).table(table_id)

    
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
    }
        )  # Reemplaza con tu información de cuenta de servicio
    project_id = 'eternal-lodge-414200'  # Reemplaza con tu ID de proyecto

    # Comprueba si el archivo existe antes de procesarlo (optimización opcional)
    if client.bucket(bucket_name).blob(file_name).exists():
        content = client.get_bucket(bucket_name).blob(file_name).download_as_text()
        last_row_id = None
        # Realiza el ETL según el tipo de archivo
        if file_name == 'tip.json':
            print("abriendo archivo tip")
            try:
                business = pd.read_json(content, orient="records", lines=True)
                print("el archivo fue abierto con exito")
            except ValueError as e:
                print(f"Error al leer el archivo JSON: {e}")
            
            # Realiza las transformaciones específicas para tip.json
            tip = tip.drop(columns=['compliment_count']).drop_duplicates().dropna()
            print("columna compliment_count eliminada")
            print("datos nulos y duplicados eliminados")
            tip['id'] = tip.index
            print('se realizaron las transformaciones en tip correctamente')
            client_bq.load_table_from_dataframe(
                    tip,
                    destination=table_ref,
                    job_config=job_config,
                    )
            print(f"Se añadieron registros a tip con exito")
            # se obtiene el ultimo registro
            print("obteniendo registros")
            print(tip.tail())
            last_row_id = int(tip.iloc[-1]['id'])  # Convierte 'id' a entero
            print("se obtuvo el ultimo registro")

            # Configuracion de BQ logs
            logs_id = 'logs'
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect=True
            job_config.write_disposition = bigquery.enums.WriteDisposition.WRITE_APPEND
            # Obtén el último ID cargado desde BigQuery
            last_loaded_id_query = f"""
                SELECT MAX(ultimo_id) as last_loaded_id
                FROM {project_id}.{dataset_id}.{logs_id}
            """
            last_loaded_id_df = client_bq.query(last_loaded_id_query).to_dataframe()
            #verificacion si hay datos nulos en la query
            if last_loaded_id_df['last_loaded_id'].isnull().any():
                #rellena con datos simples 
                last_loaded_id_df['last_loaded_id'].fillna(0).astype(int)
                print(last_loaded_id_df)
            # Filtra nuevos datos basados en el ID
            if last_loaded_id_query is not None:
                
                df =  last_loaded_id_df
                # Carga solo los nuevos datos en BigQuery
                if not df.empty:
                    df_to_append = pd.DataFrame({'archivo': ['tip.json'], 'ultimo_id': [last_row_id], 'fecha': [pd.Timestamp.now()]})
                    # Convierte la columna 'ultimo_id' a int
                    df_to_append['ultimo_id'] = df_to_append['ultimo_id'].astype(int)
                    # Verifica si la tabla está vacía
                    if last_loaded_id_df.empty or pd.isnull(last_loaded_id_df['last_loaded_id'][0]):
                        # Si la tabla está vacía y no hay un último ID, simplemente carga los datos
                        client_bq.load_table_from_dataframe(
                            df_to_append, 
                            destination=f'{project_id}.{dataset_id}.{logs_id}',
                            job_config=job_config,
                        )
                        print(f"Se añadieron nuevos registros a la tabla logs desde {file_name}")
                    else:
                        # Si la tabla tiene datos, realiza la verificación del último ID
                        last_loaded_id = last_loaded_id_df['last_loaded_id'][0]
                        
                        # Verifica si el último ID no es igual al último ID de la columna 'ultimo_id'
                        if not (last_loaded_id_df['last_loaded_id'] == tip['id'].max() and \
                            (last_loaded_id_df['last_loaded_id'] < tip['id'].max())):
                            # Si hay diferencias, carga los nuevos datos
                            client_bq.load_table_from_dataframe(
                                df_to_append,
                                f'{project_id}.{dataset_id}.{logs_id}',
                                job_config=job_config,
                                if_exists='append'  # especifica que la carga es incremental
                            )
                            print(f"Se añadieron registros nuevos a la tabla logs desde {file_name}")
                        else:
                            print(f"No hay nuevos registros diferentes desde la última carga desde {file_name}")

                        print("PROCESO COMPLETO")


        elif file_name == 'business.json':
            print("abriendo archivo business")
            business = pd.read_json(content,orient= "records")
            print("el archivo fue abierto con exito")

            # Realiza las transformaciones específicas para business.json

            datos_business = business.iloc[:, 0:14]
            estados_interes = ['PA', 'FL', 'CA']
            business = datos_business[datos_business['state'].isin(estados_interes)]
            palabras_clave_gastronomico = ['restaurant', 'restaurants' , 'food', 'cafe', 'bar', 'bars' , 'bakery', 'fast food' , 'tacos' , 'salad' , 'bistro', 'diner', 'grill', 'eatery', 'pub', 'tavern', 'pizzeria', 'deli', 'snack bar', 'food truck' , 'pizza' , 'chicken', 'sushi', 'steakhouse', 'seafood', 'ramen', 'noodle bar', 'buffet', 'tapas', 'food court', 'burger joint', 'ice cream shop', 'coffee shop', 'juice bar', 'bagel shop', 'sandwich shop', 'creperie', 'hot dog stand', 'vegetarian', 'vegan', 'bbq', 'barbecue', 'ethnic restaurant', 'fine dining', 'casual dining', 'family restaurant', 'gastropub', 'brewpub', 'teahouse', 'dive bar', 'sports bar', 'wine bar', 'brewery', 'distillery', 'wine tasting room', 'brewpub', 'taproom', 'bottle shop', 'cocktail bar', 'karaoke bar', 'izakaya', 'fondue', 'hot pot ', 'hamburger', 'dim sum restaurant', 'fusion restaurant', 'hot dog', 'noodle shop', 'ramen', 'steak house', 'taco stand', 'taco restaurant']
            business['categories'] = business['categories'].apply(lambda x: x.split(', ') if isinstance(x, str) else x)
            business['categories'] = business['categories'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
            business = business.dropna(subset=['categories'])
            business = business[business['categories'].apply(lambda x: any(keyword in x.lower() for keyword in palabras_clave_gastronomico))]
            palabras_clave_comida_rapida = ['fast food','burger joint', 'tacos', 'pizza', 'pizzeria', 'chicken',  'ramen', 'noodle bar', 'food truck', 'sandwich shop', 'creperie', 'hot dog stand', 'hot dog', 'noodle shop', 'taco stand']
            palabras_clave_cafeterias = ['cafe', 'bakery', 'coffee shop', 'juice bar', 'bagel shop', 'buffet', 'ice cream shop', 'teahouse']
            palabras_clave_bares = ['bar', 'bars', 'pub', 'tavern', 'cocktail bar', 'wine bar', 'brewery', 'sports bar', 'dive bar', 'karaoke bar','gastropub', 'brewpub', 'taproom']
            palabras_clave_restaurantes = ['restaurant', 'bistro', 'diner', 'grill', 'eatery', 'pub',  'deli','sushi',  'tapas', 'food court',  'fine dining', 'casual dining', 'family restaurant',  'fusion restaurant', 'izakaya', 'dim sum restaurant', 'steak house']
            def categorizar_subcategoria(categories):
                for keyword in palabras_clave_comida_rapida:
                    if keyword in categories.lower():
                        return 'Comida rápida'
                for keyword in palabras_clave_cafeterias:
                    if keyword in categories.lower():
                        return 'Cafetería'
                for keyword in palabras_clave_bares:
                    if keyword in categories.lower():
                        return 'Bar'
                for keyword in palabras_clave_restaurantes:
                    if keyword in categories.lower():
                        return 'Restaurante'
                return 'Otro'  # Si ninguna palabra clave coincide, categorizar como 'Otro'

            business['subcategoria'] = business['categories'].apply(categorizar_subcategoria)
            
            columns_to_drop = ['is_open', 'attributes', 'hours','categories']
            business = business.drop(columns=columns_to_drop).drop_duplicates().dropna(how='all').drop_duplicates()
            print("columnas eliminadas")
            print("Se eliminaron los registros nulos y duplicados")
            business['id'] = business.index
            print('se realizaron las transformaciones en business correctamente')
            client_bq.load_table_from_dataframe(
                    business,
                    destination=table_ref,
                    job_config=job_config,
                    )
            print(f"Se añadieron registros a tip con exito")
            # se obtiene el ultimo registro
            print("obteniendo registros")
            print(business.tail())
            last_row_id = int(business.iloc[-1]['id'])  # Convierte 'id' a entero
            print("se obtuvo el ultimo registro")

            # Configuracion de BQ logs
            logs_id = 'logs'
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect=True
            job_config.write_disposition = bigquery.enums.WriteDisposition.WRITE_APPEND
            # Obtén el último ID cargado desde BigQuery
            last_loaded_id_query = f"""
                SELECT archivo, MAX(ultimo_id) as last_loaded_id
                FROM {project_id}.{dataset_id}.{logs_id}
                GROUP BY archivo
            """
            last_loaded_id_df = client_bq.query(last_loaded_id_query).to_dataframe()
            #verificacion si hay datos nulos en la query
            if last_loaded_id_df['last_loaded_id'].isnull().any():
                #rellena con datos simples 
                last_loaded_id_df['last_loaded_id'].fillna(0).astype(int)
                print(last_loaded_id_df)
            # Filtra nuevos datos basados en el ID
            if last_loaded_id_query is not None:
                
                df =  last_loaded_id_df
                # Carga solo los nuevos datos en BigQuery
                if not df.empty:
                    df_to_append = pd.DataFrame({'archivo': ['business.json'], 'ultimo_id': [last_row_id], 'fecha': [pd.Timestamp.now()]})
                    # Convierte la columna 'ultimo_id' a int
                    df_to_append['ultimo_id'] = df_to_append['ultimo_id'].astype(int)
                    # Verifica si la tabla está vacía
                    if last_loaded_id_df.empty or pd.isnull(last_loaded_id_df['last_loaded_id'][0])or last_loaded_id_df['archivo'].iloc[0] != file_name:
                        # Si la tabla está vacía y no hay un último ID, simplemente carga los datos
                        client_bq.load_table_from_dataframe(
                            df_to_append, 
                            destination=f'{project_id}.{dataset_id}.{logs_id}',
                            job_config=job_config,
                        )
                        print(f"Se añadieron nuevos registros a la tabla logs desde {file_name}")
                    else:
                        # Si la tabla tiene datos, realiza la verificación del último ID
                        last_loaded_id = last_loaded_id_df['last_loaded_id'][0]
                        print("last:loaded_id:", last_loaded_id)
                        print(last_loaded_id_df)
                        # Verifica si el último ID no es igual al último ID de la columna 'ultimo_id'
                        if not (last_loaded_id_df['last_loaded_id'] == business['id'].max() and \
                            (last_loaded_id_df['archivo'] == business['archivo'].max()).all()):
                            # Si hay diferencias, carga los nuevos datos
                            client_bq.load_table_from_dataframe(
                                df_to_append,
                                f'{project_id}.{dataset_id}.{logs_id}',
                                job_config=job_config,
                                if_exists='append'  # especifica que la carga es incremental
                            )
                            print(f"Se añadieron registros nuevos a la tabla logs desde {file_name}")
                        else:
                            print(f"No hay nuevos registros diferentes desde la última carga desde {file_name}")

                        print("PROCESO COMPLETO")

        elif file_name == 'review.json':
            print("abriendo archivo review")
            chunk_size = 100000  # Ajusta según sea necesario
            for chunk in pd.read_json(content, orient="records", lines=True, chunksize=chunk_size):
                print("el archivo fue abierto con éxito")
                
                # Realiza las transformaciones específicas para review.json
                review = chunk.dropna().drop_duplicates()
                print("Se eliminaron registros nulos y duplicados")
                review['id'] = chunk.index.astype(int)
                print('se realizaron las transformaciones en review correctamente')
                
                client_bq.load_table_from_dataframe(
                    review,
                    destination=table_ref,
                    job_config=job_config,
                )
                
                print(f"Se añadieron registros a tip con éxito")
                
                # Se obtiene el último registro
                print("obteniendo registros")
                print(review.tail())
                last_row_id = int(review.iloc[-1]['id'])  # Convierte 'id' a entero
                print("se obtuvo el último registro")

            # Configuracion de BQ logs
            logs_id = 'logs'
            job_config = bigquery.LoadJobConfig()
            job_config.autodetect=True
            job_config.write_disposition = bigquery.enums.WriteDisposition.WRITE_APPEND
            # Obtén el último ID cargado desde BigQuery
            last_loaded_id_query = f"""
                SELECT MAX(ultimo_id) as last_loaded_id
                FROM {project_id}.{dataset_id}.{logs_id}
            """
            last_loaded_id_df = client_bq.query(last_loaded_id_query).to_dataframe()
            #verificacion si hay datos nulos en la query
            if last_loaded_id_df['last_loaded_id'].isnull().any():
                #rellena con datos simples 
                last_loaded_id_df['last_loaded_id'].fillna(0).astype(int)
                print(last_loaded_id_df)
            # Filtra nuevos datos basados en el ID
            if last_loaded_id_query is not None:
                
                df =  last_loaded_id_df
                # Carga solo los nuevos datos en BigQuery
                if not df.empty:
                    df_to_append = pd.DataFrame({'archivo': ['review.json'], 'ultimo_id': [last_row_id], 'fecha': [pd.Timestamp.now()]})
                    # Convierte la columna 'ultimo_id' a int
                    df_to_append['ultimo_id'] = df_to_append['ultimo_id'].astype(int)
                    # Verifica si la tabla está vacía
                    if last_loaded_id_df.empty or pd.isnull(last_loaded_id_df['last_loaded_id'][0])or last_loaded_id_df['archivo'].iloc[0] != file_name:
                        # Si la tabla está vacía y no hay un último ID, simplemente carga los datos
                        client_bq.load_table_from_dataframe(
                            df_to_append, 
                            destination=f'{project_id}.{dataset_id}.{logs_id}',
                            job_config=job_config,
                        )
                        print(f"Se añadieron nuevos registros a la tabla logs desde {file_name}")
                    else:
                        # Si la tabla tiene datos, realiza la verificación del último ID
                        last_loaded_id = last_loaded_id_df['last_loaded_id'][0]
                        
                        # Verifica si el último ID no es igual al último ID de la columna 'ultimo_id'
                        if not (last_loaded_id_df['last_loaded_id'] == business['id'].max() and \
                            (last_loaded_id_df['archivo'] == business['archivo'].max()).all()):
                            # Si hay diferencias, carga los nuevos datos
                            client_bq.load_table_from_dataframe(
                                df_to_append,
                                f'{project_id}.{dataset_id}.{logs_id}',
                                job_config=job_config,
                                if_exists='append'  # especifica que la carga es incremental
                            )
                            print(f"Se añadieron registros nuevos a la tabla logs desde {file_name}")
                        else:
                            print(f"No hay nuevos registros diferentes desde la última carga desde {file_name}")

                        print("PROCESO COMPLETO")

                        

        # Define el esquema de la tabla logs
       
        logs_schema = [
            bigquery.SchemaField('archivo', 'STRING'),
            bigquery.SchemaField('ultimo_id', 'INTEGER'),
            bigquery.SchemaField('fecha', 'TIMESTAMP'),
        ]

        # Crea la tabla logs si no existe
        dataset_id = 'DS_PF'
        logs_id = 'logs'
        job_config = bigquery.LoadJobConfig()
        job_config.schema = logs_schema
        job_config.write_disposition = bigquery.enums.WriteDisposition.WRITE_APPEND
        client_bq.create_dataset(dataset_id, exists_ok=True)
        table_ref = client_bq.dataset(dataset_id).table(logs_id)
        try:
            # Intenta obtener la tabla existente
            table = client_bq.get_table(table_ref)
            print(f"La tabla {logs_id} ya existe. No se creará nuevamente.")
        except google.cloud.exceptions.NotFound:
            # Si no se encuentra, crea la nueva tabla con el esquema
            print("Creando tabla logs")
            table = bigquery.Table(table_ref, schema=logs_schema)
            table = client_bq.create_table(table)
            print(f"Se creó la tabla {table_id} con éxito.")
        except Exception as e:
            print(f"Error al verificar la existencia de la tabla {table_id}: {e}")

            
            
