#!/bin/sh

# Verificar si curl está instalado
if ! command -v curl &> /dev/null; then
    echo "curl no está instalado. Instalando..."
    apt-get update
    apt-get install -y curl
fi

# Comando para verificar el estado de tu servicio
curl -sSf http://localhost:8080/healthcheck > /dev/null

# Capturamos el código de salida del comando anterior
if [ $? -ne 0 ]; then
  echo "Error: El servicio no está respondiendo correctamente."
  exit 1
fi

echo "El servicio está en buen estado."
exit 0
