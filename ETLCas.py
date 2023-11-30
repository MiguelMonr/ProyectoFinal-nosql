import requests
import pandas as pd


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
for i in range(200):
    key = keys[i]
    url = f"https://api.themoviedb.org/3/find/{key}?api_key={api_key}&external_source=imdb_id"

    response = requests.get(url)
 
    if response.status_code == 200:
        # Añadir los datos de la película a la lista
         data = response.json().get('movie_results', [])
         clean_data = []
         for movie in data: 
                    data = response.json().get('movie_results', [])
                    movie_data_list.extend(data)
    else:
        print(f"Failed to retrieve data for {key}. Status code:", response.status_code)
            

df = pd.DataFrame(movie_data_list)
'''Jugando con el DF
    Guardamos informacion sobre la pelicula/serie
    
'''
df1 = df[['id','title','media_type', 'original_title','release_date']]

#Info sobre idioma

df2 = df[['id','original_language']]  

#Informacion sobre popularidad
df3 = df[['id','popularity', 'vote_average', 'vote_count']]

# Guardar el DataFrame en un archivo CSV
df1.to_csv("Pelicula.csv", index=False)
df2.to_csv("Idioma.csv", index=False)
df3.to_csv("Popularidad.csv", index=False)   

