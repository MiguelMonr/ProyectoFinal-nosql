from neo4j import GraphDatabase

# Conexión a la base de datos Neo4j
uri = "bolt://localhost:7687"  # Reemplaza con tu URL
usuario = "neo4j"  # Reemplaza con tu usuario
contrasena = "tu_contraseña"  # Reemplaza con tu contraseña

# Función para cargar datos desde un archivo CSV
def cargar_datos_desde_csv(uri, usuario, contrasena, nombre_nodo, ruta_csv):
    with GraphDatabase.driver(uri, auth=(usuario, contrasena)) as driver:
        with driver.session() as session:
            load_data = (
                f"LOAD CSV WITH HEADERS FROM 'file://{ruta_csv}' AS row "
                f"CREATE (n:{nombre_nodo}) SET n = row"
            )
            session.run(load_data)

# Nombre del nodo a crear en Neo4j para almacenar los datos del CSV
nombre_nodo1 = "Pelicula"
nombre_nodo2 = "Idioma"
nombre_nodo3 = "Popularidad"
# Ruta al archivo CSV
ruta_csv1 = "Pelicula.csv"  # Reemplaza con el nombre de tu archivo CSV
ruta_csv2 = "Popularidad.csv"
ruta_csv3 = "Idioma.csv"

# Llamada a la función para cargar datos desde el CSV
cargar_datos_desde_csv(uri, usuario, contrasena, nombre_nodo1, ruta_csv1)
cargar_datos_desde_csv(uri, usuario, contrasena, nombre_nodo2, ruta_csv2)
cargar_datos_desde_csv(uri, usuario, contrasena, nombre_nodo3, ruta_csv3)