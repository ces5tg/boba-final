#!/bin/bash

# Esperar a que Kafka esté listo
while ! nc -z localhost 9092; do   
  echo "Esperando a que Kafka esté listo..."
  sleep 2 # Espera 2 segundos antes de verificar nuevamente
done

# Crear el tópico 'ideas2'
kafka-topics --create --topic ideas2 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1

echo "Tópico 'ideas2' creado exitosamente."
