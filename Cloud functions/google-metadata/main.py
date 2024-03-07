import pandas as pd
from google.cloud import bigquery, storage
from functions_framework import cloud_event
from google.oauth2 import service_account
import json
import re


def extract_state(address):
    if address is not None:
        # Utilizar expresión regular para encontrar la cuarta sección y eliminar los números
        parts = address.split(",")
        if len(parts) >= 4:
            match = re.search(r'[A-Za-z]+', parts[3])
            if match:
                return match.group()
    return None

@cloud_event
def process_json_files(event):
    bucket_name = event.data["bucket"]
    folder_name = "metada-sitios"  # Reemplaza con el nombre de tu carpeta
    dataset_id = "DS_PF"  # Reemplaza con tu ID de conjunto de datos en BigQuery
    project_id = "eternal-lodge-414200"  # Reemplaza con tu ID de proyecto

    # Crea un cliente de Cloud Storage
    client_storage = storage.Client()

    # Crea un cliente de BigQuery
    client_bq = bigquery.Client()

    # Lista de archivos JSON en la carpeta
    blobs = client_storage.list_blobs(bucket_name, prefix=f"{folder_name}/")
    json_files = [blob.name for blob in blobs if blob.name.endswith('.json')]

    # Crea un DataFrame vacío para almacenar los datos
    df_combined = pd.DataFrame()

    for json_file in json_files:
        # Descarga el archivo JSON como texto
        content = client_storage.get_bucket(bucket_name).blob(json_file).download_as_text()
        
        # Imprime el contenido para verificar que sea legible
        print(f"Contenido del archivo {json_file}:")
        print(content)
        
        try:
            # Lee el archivo JSON en un DataFrame
            df = pd.read_json(content, orient='records' ,lines=True)  # Agrega lines=True aquí

            print(df.head(5)) 
            # Concatena el DataFrame al conjunto de datos combinado
            df_combined = pd.concat([df_combined, df], ignore_index=True)
            print("Se abrieron los archivos con éxito")

        except Exception as e:
            print(f"Error al procesar el archivo {json_file}: {e}")
    #ETL    
    # Aplicar la función a cada valor de la columna 'address' y guardar los resultados en una nueva columna 'estado'
        df_combined['estado'] = df_combined['address'].apply(extract_state)

    # Lista de estados de interés con sus abreviaturas de dos letras
        estados_interes = ['PA', 'FL',  'CA']
    
    # Eliminar la columna 'address'
        df_combined.drop(columns=['address'], inplace=True)
    
    # Filtrar las filas que tienen los estados de interés
        metadata = df_combined[df_combined['estado'].isin(estados_interes)]
    
    # Definir palabras clave asociadas al rubro gastronómico
        palabras_clave_gastronomico = ['restaurant', 'restaurants' , 'food', 'cafe', 'bar', 'bars' , 'bakery', 'fast food' , 'tacos' , 'salad' , 'bistro', 'diner', 'grill', 'eatery', 'pub', 'tavern', 'pizzeria', 'deli', 'snack bar', 'food truck' , 'pizza' , 'chicken', 'sushi', 'steakhouse', 'seafood', 'ramen', 'noodle bar', 'buffet', 'tapas', 'food court', 'burger joint', 'ice cream shop', 'coffee shop', 'juice bar', 'bagel shop', 'sandwich shop', 'creperie', 'hot dog stand', 'vegetarian', 'vegan', 'bbq', 'barbecue', 'ethnic restaurant', 'fine dining', 'casual dining', 'family restaurant', 'gastropub', 'brewpub', 'teahouse', 'dive bar', 'sports bar', 'wine bar', 'brewery', 'distillery', 'wine tasting room', 'brewpub', 'taproom', 'bottle shop', 'cocktail bar', 'karaoke bar', 'izakaya', 'fondue', 'hot pot ', 'hamburger', 'dim sum restaurant', 'fusion restaurant', 'hot dog', 'noodle shop', 'ramen', 'steak house', 'taco stand', 'taco restaurant']

    # Convertir las cadenas de texto a listas si es necesario
        metadata['category'] = metadata['category'].apply(lambda x: x.split(', ') if isinstance(x, str) else x)

    # Convertir las listas de categorías a strings separados por comas
        metadata['category'] = metadata['category'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

    # Filtrar los valores nulos en el campo 'category'
        metadata = metadata.dropna(subset=['category'])

    # Filtrar el DataFrame para obtener solo las filas con categorías gastronómicas
    locales_gastronomicos = metadata[metadata['category'].apply(lambda x: any(keyword in x.lower() for keyword in palabras_clave_gastronomico))]
    locales_gastronomicos = locales_gastronomicos.drop_duplicates(subset=['gmap_id'])
    locales_gastronomicos = locales_gastronomicos.dropna(subset=['gmap_id'])
    # Definir listas de palabras clave para cada subcategoría
    palabras_clave_comida_rapida = ['fast food','burger joint', 'tacos', 'pizza', 'pizzeria', 'chicken',  'ramen', 'noodle bar', 'food truck', 'sandwich shop', 'creperie', 'hot dog stand', 'hot dog', 'noodle shop', 'taco stand']
    palabras_clave_cafeterias = ['cafe', 'bakery', 'coffee shop', 'juice bar', 'bagel shop', 'buffet', 'ice cream shop', 'teahouse']
    palabras_clave_bares = ['bar', 'bars', 'pub', 'tavern', 'cocktail bar', 'wine bar', 'brewery', 'sports bar', 'dive bar', 'karaoke bar','gastropub', 'brewpub', 'taproom']
    palabras_clave_restaurantes = ['restaurant', 'bistro', 'diner', 'grill', 'eatery', 'pub',  'deli','sushi',  'tapas', 'food court',  'fine dining', 'casual dining', 'family restaurant',  'fusion restaurant', 'izakaya', 'dim sum restaurant', 'steak house']
    
    # Función para categorizar cada local en una sola subcategoría
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

    # Aplicar la función de categorización para crear una nueva columna de subcategoría
    locales_gastronomicos['subcategoria'] = locales_gastronomicos['category'].apply(categorizar_subcategoria)
    # Lista de columnas a eliminar
    columnas_a_eliminar = ['price', 'description', 'relative_results','category']
    # Eliminar las columnas
    locales_gastronomicos = locales_gastronomicos.drop(columns=columnas_a_eliminar)

    locales_gastronomicos = locales_gastronomicos[locales_gastronomicos['state'] != 'Permanently closed']
    locales_gastronomicos['id'] = locales_gastronomicos.index
    print("se realizaron lastransformaciones correctamente")

    # Carga la tabla con detección automática de esquemas
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    table_id = file_name.split('.')[0] 
    table_ref = client_bq.dataset(dataset_id).table(table_id)
    # Carga los DataFrames (ejemplo con tip.json)
    cubo = client.bucket(bucket_name)  # Almacenar el objeto del bucket

    
    try:
        objeto = cubo.blob(locales_gastronomicos)
        contenido = objeto.download_as_text()
        table_id = locales
        df = locales_gastronomicos
        # Cargar a BigQuery el archivo procesado
  
        client_bq.load_table_from_dataframe(
            df,
            destination=table_ref,
            job_config=job_config,
        )
        print(f"Se añadieron registros a {table_id} con exito")
    except Exception as e:
        print(f"Error al procesar el archivo {table_id}: {e}")
        
    # Define el esquema de la tabla logs
    logs_schema = [
        bigquery.SchemaField('archivo', 'STRING'),
        bigquery.SchemaField('ultimo_id', 'STRING'),
        bigquery.SchemaField('fecha', 'TIMESTAMP'),
    ]

    # Crea la tabla logs si no existe
    dataset_id = 'DS_PF'
    table_id = 'logs'
    job_config = bigquery.LoadJobConfig()
    job_config.schema = logs_schema
    job_config.write_disposition = bigquery.enums.WriteDisposition.WRITE_APPEND
    client_bq.create_dataset(dataset_id, exists_ok=True)
    table_ref = client_bq.dataset(dataset_id).table(table_id)
    
    try:
        client_bq.get_table(table_ref)
        print(f"La tabla {table_id} ya existe. No se creará nuevamente.")
    except Exception as e:
        # Si la tabla no existe, crea la nueva tabla
        if 'Not found' in str(e):
            job_config = bigquery.LoadJobConfig()
            job_config.schema = logs_schema

            client_bq.create_table(table_ref, job_config=job_config)
            print(f"Se creó la tabla {table_id} con éxito.")
        else:
            print(f"Error al verificar la existencia de la tabla {table_id}: {e}")
     
    # Obtén el último ID cargado desde BigQuery
    last_loaded_id_query = f"""
        SELECT MAX(ultimo_id) as last_loaded_id
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    last_loaded_id_df = client_bq.query(last_loaded_id_query).to_dataframe()

    # Obtén la última fila de el archivo.
    last_row_id = None # Inicializa last_row_id aquí
    if locales_gastronomicos is not None:
        last_row_id = locales_gastronomicos.iloc[-1]['id']

        # Filtra nuevos datos basados en el ID
        if last_loaded_id is not None:
            df = last_row_id[last_row_id['id'] > last_loaded_id]
        
                # Carga solo los nuevos datos en BigQuery
            if not df.empty:
                df_to_append = pd.DataFrame({'archivo': locales_gastronomicos, 'ultimo_id': [last_row_id], 'fecha': [pd.Timestamp.now()]})
                # Verifica si la tabla está vacía
                if last_loaded_id_df.empty or pd.isnull(last_loaded_id_df['last_loaded_id'][0]):
                    # Si la tabla está vacía, no hay un último ID, simplemente carga los datos
                    client_bq.load_table_from_dataframe(
                        df_to_append,  # Asegúrate de tener el DataFrame correcto aquí
                        destination=f'{project_id}.{dataset_id}.{table_id}',
                        job_config=job_config,
                    )
                    print(f"Se añadieron registros nuevos a la tabla logs desde {nombre_archivo}")
                else:
                    # Si la tabla tiene datos, realiza la verificación del último ID
                    last_loaded_id = last_loaded_id_df['last_loaded_id'][0]
                    
                    # Verifica si el último ID no es igual al último ID de la columna 'ultimo_id' agrupado por 'archivo'
                    if not (last_loaded_id_df.groupby('archivo')['last_loaded_id'].max() == last_loaded_id).all():
                        # Si hay diferencias, carga los nuevos datos
                        client_bq.load_table_from_dataframe(
                            df_to_append,
                            f'{project_id}.{dataset_id}.{table_id}',
                            job_config=job_config,
                            if_exists='append'  # Agrega esta línea para especificar que la carga es incremental
                        )
                        print(f"Se añadieron registros nuevos a la tabla logs desde {nombre_archivo}")
                    else:
                        print(f"No hay nuevos registros diferentes desde la última carga desde {nombre_archivo}")
            else:
                print(f"No hay nuevos registros desde la última carga desde {nombre_archivo}")

    print("Proceso completo")