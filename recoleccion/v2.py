from flask import Flask, request, jsonify , redirect , url_for
from confluent_kafka import Producer, KafkaException
import json
import requests
import redis
import time
app = Flask(__name__)
r = redis.StrictRedis(host='localhost', port=6379, db=0)
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)
RECOMENDACIONES_API = "http://127.0.0.1:5050/recomendaciones"

@app.route('/recoleccion', methods=['POST'])#cada vez que el usuario desliza el dedo
def enviar_a_kafka():
    try:
        data = request.get_json()
        value = json.dumps(data)
        id_usuario = data.get('IdUsuario')
        print(id_usuario)
        producer.produce('ideas2', value=value.encode('utf-8'), callback=delivery_report)
        time.sleep(1)
        print("resultados")
        #print(obtener_recomendaciones(id_usuario))
        #return redirect(url_for('obtener_recomendaciones', user_id=id_usuario ))
        return jsonify({'message': 'Datos enviados a Kafka correctamente'}), 200
    except KafkaException as e:
        return jsonify({'error': f'Error al enviar datos a Kafka: {str(e)}'}), 500
    except Exception as e:
        return jsonify({'error': f'Error interno del servidor: {str(e)}'}), 500


def obtener_recomendaciones(user_id):
       # Recuperar una recomendación de Redis y eliminarla
    rec = r.lpop(f"recomendaciones:{user_id}")
    recomendacion = None
    if rec:
        recomendacion = json.loads(rec)

    # Verificar cuántas recomendaciones quedan
    recomendaciones_restantes = r.llen(f"recomendaciones:{user_id}")

    if recomendaciones_restantes <= 6:
        # Solicitar más recomendaciones si quedan 3 o menos
        response = requests.get(f"{RECOMENDACIONES_API}/{user_id}")
        if response.status_code == 200:
            nuevas_recomendaciones = response.json()
            # Almacenar las nuevas recomendaciones en Redis
            for rec in nuevas_recomendaciones:
                r.rpush(f"recomendaciones:{user_id}", json.dumps(rec))

    # Devolver la recomendación actual
    return recomendacion


def delivery_report(err, msg):
    if err is not None:
        print(f'Error al entregar mensaje: {err}')
    else:
        print(f'Mensaje entregado a {msg.topic()} [{msg.partition()}]')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3400, debug=True)
