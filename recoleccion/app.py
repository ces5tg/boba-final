from flask import Flask, request, jsonify
from confluent_kafka import Producer, KafkaException
import json
import requests
import redis
app = Flask(__name__)
conf = {'bootstrap.servers': 'kafka-broker-1:9092'}
producer = Producer(conf)
redis_client_resultados = redis.Redis(host='redis', port=6379, db=1)
API_WORKER = "http://worker:8080/api/Usuarios"
@app.route('/recoleccion', methods=['POST'])
def enviar_a_kafka():
    try:
        data = request.get_json()
        # Convertir el JSON completo a formato JSON string para Kafka
        data_value = json.dumps(data)
        # Enviar el mensaje a Kafka en un topic específico
        producer.produce('ideas2', value=data_value.encode('utf-8'), callback=delivery_report)
        # Esperar a que se envíen los mensajes
        producer.flush()
        return jsonify({'message': 'Datos enviados a Kafka correctamente2'}), 200
    
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error al enviar datos a Kafka', 'error': str(e)}), 500

def delivery_report(err, msg):
    if err is not None:
        print(f'Error al entregar mensaje: {err}')
    else:
        print(f'Mensaje entregado a {msg.topic()} [{msg.partition()}]')
@app.route('/login', methods=['POST'])
def login():
    try:
        data = request.get_json()
        response = requests.post(f"{API_WORKER}/login", json=data)
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({'message': 'Error en el inicio de sesión', 'error': response.json()}), response.status_code
    
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error al comunicarse con la API_WORKER', 'error': str(e)}), 500

@app.route('/register', methods=['POST'])
def register():
    try:
        data = request.get_json()
        response = requests.post(f"{API_WORKER}/register", json=data)
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({'message': 'Error en el registro', 'error': response.json()}), response.status_code
    
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error al comunicarse con la API_WORKER', 'error': str(e)}), 500

@app.route('/sendCategorias', methods=['POST'])
def sendCategorias():
    try:
        data = request.get_json()
        response = requests.post(f"http://localhost:5012/api/Usuarios/sendCategorias", json=data)
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({'message': 'Error al enviar categorías', 'error': response.json()}), response.status_code
    
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error al comunicarse con la API_WORKER', 'error': str(e)}), 500
@app.route('/showCategories', methods=['GET'])
def get_categories():
    categorias = {
    'Western', 'Inc.', 'Drama', 'Fantasy', 'Horror', 'Romance', 'Biography', 
    'Adventure', 'War', 'History', 'Music', 'Family', 'Comedy', 'Musical', 
    'Mystery', 'Action', 'Thriller', 'Animation', 'Sci-Fi', 'Crime'
}
    return jsonify(list(categorias))

@app.route('/recomendaciones/<user_id>', methods=['GET'])
def get_recomendaciones(user_id):
    try:
        recomendaciones = []
        # Recuperar todas las recomendaciones almacenadas en Redis
        while True:
            rec = redis_client_resultados.lpop(f"recomendaciones:{user_id}")
            if not rec:
                break
            recomendacion = json.loads(rec)
            recomendaciones.append(recomendacion)
        return jsonify(recomendaciones), 200
    except Exception as e:
        print(e)
        return jsonify({'message': 'Error al obtener recomendaciones', 'error': str(e)}), 500
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3400, debug=True)
