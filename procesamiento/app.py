from flask import Flask, jsonify
import requests
import json
import numpy as np
import pandas as pd
from confluent_kafka import Consumer, KafkaException, KafkaError
from threading import Thread
import time
from math import sqrt
import redis
#En esta version estamso isnertando los dataframe de pandas dentro de redis
app = Flask(__name__)

# Definir los endpoints para las APIs
VISUALIZACIONES_API = 'http://worker:8080/api/visualizacion'
MOVIES_API = 'http://worker:8080/api/movies'
USUARIOS_API = 'http://worker:8080/api/usuarios'

# Conectar a Redis 
redis_client = redis.Redis(host='redis', port=6379, db=0) #redis -> escritura
redis_client_read = redis.Redis(host='redis-slave1', port=7001, db=0) # redis-slave1 -> lectura
redis_client_resultados = redis.Redis(host='redis', port=6379, db=1)
# Inicializar un DataFrame vacío
df_visualizaciones = pd.DataFrame()
df_movies = pd.DataFrame()  # Nuevo DataFrame para almacenar las películas

def obtener_datos(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

def inicializar_dataframe():
    global df_visualizaciones, df_movies
    datos_visualizaciones = obtener_datos(VISUALIZACIONES_API)
    df_visualizaciones = pd.DataFrame(datos_visualizaciones)
    # Almacenar en Redis
    redis_client.set('df_visualizaciones', df_visualizaciones.to_json())
    print(f"Se inicializó el DataFrame df_visualizaciones con {len(df_visualizaciones)} registros")

    datos_movies = obtener_datos(MOVIES_API)
    df_movies = pd.DataFrame(datos_movies)
    # Almacenar en Redis
    redis_client.set('df_movies', df_movies.to_json())
    print(f"Se inicializó el DataFrame df_movies con {len(df_movies)} películas")

def consumidor_kafka():
    conf = {
        'bootstrap.servers': 'kafka-broker-1:9092',
        'group.id': 'visualizaciones_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    consumer.subscribe(['ideas2'])
    
    global df_visualizaciones
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            nuevo_dato = json.loads(msg.value().decode('utf-8'))
            print(f'Recibido nuevo dato: {nuevo_dato}')
            
            # Convertir el nuevo dato en un DataFrame temporal
            new_df = pd.DataFrame([nuevo_dato])
            
            # Concatenar el nuevo DataFrame con df_visualizaciones
            df_visualizaciones = pd.concat([df_visualizaciones, new_df], ignore_index=True)
            # Almacenar en Redis
            redis_client.set('df_visualizaciones', df_visualizaciones.to_json())
            print(len(df_visualizaciones) , " -- tamaño del ------------------xxxxx")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Iniciar el consumidor de Kafka en un hilo separado
thread = Thread(target=consumidor_kafka)
thread.start()

def obtener_visualizaciones_de_redis():
    data = redis_client_read.get('df_visualizaciones')
    print(data)
    if data:
        json_data = json.loads(data)
        df = pd.DataFrame(json_data)
        return df
    return pd.DataFrame()

def obtener_movies_de_redis():
    data = redis_client_read.get('df_movies')
    if data:
        json_data = json.loads(data)
        df = pd.DataFrame(json_data)
        return df
    return pd.DataFrame()

def similitud_coseno(vector1, vector2):
    sum_xy = 0
    sum_x2 = 0
    sum_y2 = 0
    for x, y in zip(vector1, vector2):
        sum_xy += int(x) * int(y)
        sum_x2 += int(x) * int(x)
        sum_y2 += int(y) * int(y)
    denominador = sqrt(sum_x2) * sqrt(sum_y2)
    if denominador == 0:
        return 0
    else:
        return sum_xy / denominador

def recomendar(user_id, cercanos, movies_filtro):
    print(cercanos , "estos son los -usarios mas cercanos ")
    print(movies_filtro , " filtro de peliculas")
    recomendaciones = {}
    nearest = cercanos
    user_movies = [movie for movie in movies_filtro if movie['idUsuario'] == user_id]
    userRatings = {movie['idMovie']: movie['sumatoria'] for movie in user_movies}
    totalDistance = 0.0
    for i in range(min(10, len(nearest))):
        totalDistance += nearest[i][1]
    if totalDistance == 0.0:
        return []
    for i in range(min(10, len(nearest))):
        weight = nearest[i][1] / totalDistance
        neighbor_id = nearest[i][0]
        neighbor_movies = [movie for movie in movies_filtro if movie['idUsuario'] == neighbor_id]
        neighborRatings = {movie['idMovie']: movie['sumatoria'] for movie in neighbor_movies}
        for movie_id, rating in neighborRatings.items():
            if movie_id not in userRatings:
                if movie_id not in recomendaciones:
                    recomendaciones[movie_id] = rating * weight
                else:
                    recomendaciones[movie_id] += rating * weight
    recomendaciones = sorted(recomendaciones.items(), key=lambda item: item[1], reverse=True)
    recommended_movie_ids = [movie[0] for movie in recomendaciones]

    resultados = []
    for movie_id in recommended_movie_ids[:10]:
        pelicula = next((movie for movie in movies_filtro if movie['idMovie'] == movie_id), None)
        if pelicula:
            elemento = {
                'id': pelicula['id_x'],
                'datos': {
                    'movieId': pelicula['movieId'],
                    'url': pelicula['url']
                }
            }
            resultados.append(elemento)

    return resultados

def coseno_categorias(user_id): #comparacion entre vectores de peliculas segun sus categorias
    movies = obtener_movies_de_redis().to_dict(orient='records')
    genres_set = set(genre for movie in movies for genre in movie['generos'] if genre)
    print(genres_set)
    print("+++++++++++++++++++++++")
    for movie in movies:
        movie['genre_vector'] = [1 if genre in movie['generos'] else 0 for genre in genres_set]

    endpoint = f"{USUARIOS_API}/{user_id}"
    usuario = obtener_datos(endpoint)
    categorias_preferidas = usuario['categoriasPreferidas']
    user_vector = [1 if genre in categorias_preferidas else 0 for genre in genres_set]
    movies_matrix = np.array([movie.get('genre_vector', [0]*len(genres_set)) for movie in movies])
    
    start_time = time.time()
    dot_product = np.dot(movies_matrix, user_vector)
    norm_movies = np.linalg.norm(movies_matrix, axis=1)
    norm_user = np.linalg.norm(user_vector)
    similarity = dot_product / (norm_movies * norm_user)
    
    for movie, sim in zip(movies, similarity):
        movie['similarity'] = sim
    
    end_time = time.time()
    execution_time = end_time - start_time
    print("Tiempo de ejecución:", execution_time, "segundos")

    filtered_movies = movies
    filtered_movies.sort(key=lambda x: x['similarity'], reverse=True)

    resultados = []
    for pelicula in filtered_movies[:10]:
        elemento = {
            'id': pelicula['id'],
            'datos': {
                'movieId': pelicula['movieId'],
                'url': pelicula['url']
            }
        }
        resultados.append(elemento)

    json_resultados = json.dumps(resultados, indent=2) 
    return resultados

def knn_usuarios(user_id, visualizaciones):
    global df_visualizaciones, df_movies
    df_visualizaciones = obtener_visualizaciones_de_redis()
    df_movies = obtener_movies_de_redis()
    print(df_visualizaciones , "dataset de visualizacionea")
    print(df_movies , "dataset de moviess")

    df_filtered_visualizaciones = df_visualizaciones[df_visualizaciones['timeVisualizacion'] > "00:00:10"]
    df_movies_filtro = pd.merge(df_filtered_visualizaciones, df_movies, left_on='idMovie', right_on='movieId', how='inner')

    grouped_movies = df_movies_filtro.groupby('idUsuario')['idMovie'].apply(list).to_dict()

    user_movie = grouped_movies.get(user_id, [])

    similarities = []
    for user, movie_ids in grouped_movies.items():
        similarity = similitud_coseno(user_movie, movie_ids)
        similarities.append((user, similarity))

    sorted_similarities = sorted(similarities, key=lambda item: item[1], reverse=True)
    print( df_movies_filtro , "filtro de plicual")
    recomendaciones_final = [(user, similarity) for user, similarity in sorted_similarities if similarity is not None]
    print(recomendaciones_final)
    print("++++++++++++++++++++++++++++")
    print(df_movies_filtro)
    #(1 , 0.67) , (45,0,97)
    # Llamar a la función recomendar para obtener las recomendaciones finales
    final_recommendations = recomendar(user_id, recomendaciones_final, df_movies_filtro.to_dict(orient='records'))

    return final_recommendations


@app.route("/tamaño", methods=["GET"])
def tamaño():
    df_visualizaciones = obtener_visualizaciones_de_redis()

    if df_visualizaciones is None or df_visualizaciones.empty:
        return "El DataFrame df_visualizaciones está vacío o no está definido", 404
    
    size = len(df_visualizaciones)

    headers = list(df_visualizaciones.columns)    
    data = df_visualizaciones.to_dict(orient='records')
    response_data = {
        "size": size,
        "headers": headers,
        "data": data
    }
    
    return jsonify(response_data), 200

@app.route("/recomendaciones/<user_id>", methods=["GET"])
def recomendar_para_usuario(user_id):
    global df_visualizaciones
    visualizaciones_usuario = df_visualizaciones[df_visualizaciones['idUsuario'] == user_id].to_dict(orient='records')
    
    if len(visualizaciones_usuario) <= 3:
        print("AQUI SE PROBARA LA SIMILTIDUD DEL COSENOcon ENFOCADA A LOS USUARIOS")
        resultados = coseno_categorias(user_id)
        for pelicula in resultados:
            redis_client_resultados.rpush(f"recomendaciones:{user_id}", json.dumps(pelicula))
        return jsonify(resultados), 200
    else:
        start_time = time.time()
        resultados = knn_usuarios(user_id, df_visualizaciones)
        tiempo_ejecucion = time.time() - start_time
        print("el tiempo de ejecucion de KNN-USUARIO ES " , tiempo_ejecucion)
        for pelicula in resultados:
            redis_client_resultados.rpush(f"recomendaciones:{user_id}", json.dumps(pelicula))
        return jsonify(resultados=resultados, tiempo_ejecucion=tiempo_ejecucion), 200

if __name__ == '__main__':
    inicializar_dataframe()
    app.run(host='0.0.0.0', port=5050, debug=True, threaded=True)












