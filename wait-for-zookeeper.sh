#!/bin/bash

# Espera hasta que Zookeeper esté disponible
until echo ruok | nc zookeeper2 2181 | grep imok; do
  echo "Esperando a que Zookeeper esté disponible..."
  sleep 5
done

# Una vez que Zookeeper está listo, iniciar Kafka
exec "$@"
