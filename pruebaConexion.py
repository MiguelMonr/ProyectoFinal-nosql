import requests
import pandas as pd
import pymongo

# Tu clave de la API
api_key = 'c8143bf74c1d797a6756f96808315a09'


#Conexión a la base de datos
client = pymongo.MongoClient("mongodb://mongo_lake:27017")
db = client["pelicuas"]
casa = db["drinks"]
mongo = db["IMDB_m"]

# Lista para almacenar los datos de cada películasp
movie_data_list = [] #Esto es para comprobar que se han guardado los datos

# Lista de claves IMDb (deberás reemplazar esto con la lista obtenida de tu archivo CSV)
keys = list(pd.read_csv("imdb_keys.csv")["keys"])

client = pymongo.MongoClient("mongodb://mongo_lake:27017")
db = client["cocktails"]
casa = db["drinks"]
mongo = db["IMDB_m"] 

# Bucle para recorrer cada clave y realizar la solicitud a la API
for i in range(10): #El range ahorita está en 1 para comprobar que se esten guardando los datos
    key = keys[i]
    url = f"https://api.themoviedb.org/3/find/{key}?api_key={api_key}&external_source=imdb_id"

    response = requests.get(url)

    print(key)
    
    
    
    #Aqui inica la conexion a la api 
    if response.status_code == 200: #En este punto empezamos la limpieza de datos
        
        # Obtener la respuesta como un diccionario
        movie_id = {}
        movie_info = {}  # Diccionario para almacenar datos de movie_info
        media = {}  # Diccionario para almacenar datos de media

        # Extraer datos específicos y guardarlos en movie_info y media
        data = response.json()

        #Procedemos a guardar los datos crudos en mongo
       # for movie_data in data:
        # Insertar los datos en la colección de MongoDB
           # mongo.insert_one(movie_data) #aqui se deben de guardar los datos en mongo
        
        
        #Empezamos el ETL de los datos para subirlos a cassandra. 
        if 'movie_results' in data and len(data['movie_results']) > 0:
            movie_result = data['movie_results'][0]  # Tomar el primer resultado (asumiendo que sea una película)

            # Guardar datos en movie_info
            movie_info['id'] = movie_result.get('id')
            movie_info['adult'] = movie_result.get('adult')
            movie_info['title'] = movie_result.get('title')
            movie_info['original_title'] = movie_result.get('original_title')
            movie_info['original_language'] = movie_result.get('original_language')

            # Guardar el resto de los datos en media
            media_keys = ['backdrop_path', 'overview', 'poster_path', 'media_type', 'genre_ids',
                          'popularity', 'release_date', 'video', 'vote_average', 'vote_count']
            for key, value in movie_result.items():
                if key not in movie_info and key in media_keys:
                    media[key] = value

            # Agregar movie_info y media al diccionario de la película
            movie_data = {'movie_info': movie_info, 'media': media}
            movie_data_list.append(movie_data)
        else:
            print(f"No se encontraron datos de película para {key}")
    else:
        print(f"Error al recuperar datos para {key}. Código de estado:", response.status_code)
        
     
        
            

# Ahora movie_data_list contiene los datos de todas las películas solicitadas
print(len(movie_data_list))
print(movie_data_list[0])
print(movie_data_list[4])