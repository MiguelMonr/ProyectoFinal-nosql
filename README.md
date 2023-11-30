## ProyectoFinal-nosql
Trabajo final para la materia bases de datos no relacionales otoño 2023.

## API. 
A partir de la API de REST Countries hacemos una conexión a través de python con una base de datos MongoDB. 
Posteriormente, hacemos un ETL que cargue la base de datos procesada una base de datos estilo grafo (Neo4j) y a otra columnar respectivamente.
Finalmente realizamos tres busquedas por base de datos. 

## Instrucciones para la instalacion 

Por medio de docker-compose generamos cuatro contenedores. Los servicios se definen en el archivo docker-compose.yaml que se ejecutan en un ambiente aislado.

Docker compose 
 ```bash

docker-compose build
docker-compose up
  ```

Queries en mongo 
 ```bash
db.claves3.aggregate([
  {
    $unwind: "$movie_results"
  },
  {
    $group: {
      _id: "$movie_results.title",
      total_votes: { $sum: "$movie_results.vote_count" },
      average_rating: { $avg: "$movie_results.vote_average" }
    }
  },
  {
    $sort: {
      total_votes: -1
    }
  },
  {
    $limit: 10
  },
  {
    $sort: {
      "_id": 1
    }
  }
])


  ```

 ```bash
db.claves3 .find({ "movie_results.release_date": { $lt: "2022-01-01" } })
  ```
 
 ```bash
db.claves3.aggregate([
    {
        $unwind: "$movie_results"
    },
    {
        $group: {
            _id: "$movie_results.title",
            average_rating: { $avg: "$movie_results.vote_average" }
        }
    },
    {
        $sort: { average_rating: -1 }
    }
])

  ```

Queries en Cassandra 
 ```bash

  ```

 ```bash

  ```

 ```bash

  ```

Queries en Neo4j 
 ```bash

  ```

 ```bash

  ```

 ```bash

  ```

