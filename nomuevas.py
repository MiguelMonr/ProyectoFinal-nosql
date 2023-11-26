import requests
import pandas as pd
#import pymongo

# Tu clave de la API
api_key = 'c8143bf74c1d797a6756f96808315a09'


#Conexión a la base de datos
#client = pymongo.MongoClient("mongodb://mongo_lake:27017")
#db = client["pelicuas"]
#casa = db["drinks"]
#mongo = db["IMDB_m"]

# Lista para almacenar los datos de cada películasp
movie_data_list = []

# Lista de claves IMDb (deberás reemplazar esto con la lista obtenida de tu archivo CSV)
keys = list(pd.read_csv("imdb_keys.csv")["keys"])

'''client = pymongo.MongoClient("mongodb://mongo_lake:27017")
db = client["cocktails"]
casa = db["drinks"]
mongo = db["IMDB_m"]''' 

# Bucle para recorrer cada clave y realizar la solicitud a la API
for i in range(10):
    key = keys[i]
    url = f"https://api.themoviedb.org/3/find/{key}?api_key={api_key}&external_source=imdb_id"

    response = requests.get(url)

    print(key)
    
    if response.status_code == 200:
        # Añadir los datos de la película a la lista
        movie_data_list.append(response.json())
    else:
        print(f"Failed to retrieve data for {key}. Status code:", response.status_code)
            

# Ahora movie_data_list contiene los datos de todas las películas solicitadas
print(movie_data_list[0])