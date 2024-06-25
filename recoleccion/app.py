from flask import Flask, request, jsonify
from confluent_kafka import Producer, KafkaException
import json

app = Flask(__name__)
conf = {'bootstrap.servers': 'kafka-broker-1:9092'}
producer = Producer(conf)
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3400, debug=True)
