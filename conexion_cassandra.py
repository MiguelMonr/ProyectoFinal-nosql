from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

auth_provider = PlainTextAuthProvider(username='tu_usuario', password='tu_contraseña') # Si tu cluster requiere autenticación
cluster = Cluster(['127.0.0.1'], port=9042, auth_provider=auth_provider)
session = cluster.connect()


session.execute("CREATE KEYSPACE IF NOT EXISTS movies WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}")
session.set_keyspace('movies')

session.execute("""
    CREATE TABLE IF NOT EXISTS movie_data (
        id int PRIMARY KEY,
        adult boolean,
        title text,
        original_title text,
        original_language text,
        backdrop_path text,
        overview text,
        poster_path text,
        media_type text,
        genre_ids list<int>,
        popularity double,
        release_date text,
        video boolean,
        vote_average double,
        vote_count int
    )
""")

def prepare_data_for_cassandra(movie_data):
    combined_data = {**movie_data['movie_info'], **movie_data['media']}
    return combined_data

transformed_data = [prepare_data_for_cassandra(movie_data) for movie_data in movie_data_list]

insert_query = """
    INSERT INTO movie_data (id, adult, title, original_title, original_language, backdrop_path, overview, poster_path, media_type, genre_ids, popularity, release_date, video, vote_average, vote_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for data in transformed_data:
    session.execute(insert_query, (
        data['id'],
        data['adult'],
        data['title'],
        data['original_title'],
        data['original_language'],
        data['backdrop_path'],
        data['overview'],
        data['poster_path'],
        data['media_type'],
        data['genre_ids'],
        data['popularity'],
        data['release_date'],
        data['video'],
        data['vote_average'],
        data['vote_count']
    ))
