"""
Simulador de sensores IoT: publica una lectura por sensor cada segundo
en la cola 'lecturas' de RabbitMQ. Nunca se conecta a la base de datos.
Incluye picos sobre el umbral y lecturas defectuosas (-999) para probar los disparadores.
"""
import json
import os
import random
import time
from datetime import datetime, timezone

import pika

INTERVALO = float(os.getenv("INTERVALO", "1"))
# sensor -> (valor base, variacion, umbral)
SENSORES = {
    "PRE03-TEMP": (70, 3, 85),  "PRE03-VIB": (4.5, 0.6, 7), "PRE03-ENE": (22, 2, 30),
    "TOR01-TEMP": (65, 3, 80),  "TOR01-VIB": (3.8, 0.5, 6), "TOR01-ENE": (18, 2, 25),
    "EMB02-TEMP": (60, 3, 75),  "EMB02-VIB": (3.0, 0.4, 5), "EMB02-ENE": (14, 2, 20),
}


def conectar():
    params = pika.URLParameters(os.getenv("RABBIT_URL", "amqp://iot:iot123@mensajes:5672/"))
    while True:
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ no disponible, reintentando...")
            time.sleep(3)


def main():
    con = conectar()
    canal = con.channel()
    canal.queue_declare(queue="lecturas", durable=True)
    print(f"Simulador publicando en la cola 'lecturas' cada {INTERVALO}s")
    enviados = 0
    while True:
        for sensor, (base, variacion, umbral) in SENSORES.items():
            valor = random.gauss(base, variacion)
            r = random.random()
            if r < 0.02:
                valor = umbral * random.uniform(1.02, 1.15)   # pico: advertencia
            elif r < 0.03:
                valor = umbral * random.uniform(1.25, 1.40)   # pico: critica
            if random.random() < 0.002:
                valor = -999                          # lectura defectuosa: el disparador la descarta
            mensaje = {"sensor": sensor, "valor": round(valor, 2),
                       "medido_en": datetime.now(timezone.utc).isoformat()}
            canal.basic_publish(exchange="", routing_key="lecturas", body=json.dumps(mensaje),
                                properties=pika.BasicProperties(delivery_mode=2))
            enviados += 1
        if enviados % 900 == 0:
            print(f"{enviados} lecturas publicadas")
        time.sleep(INTERVALO)


if __name__ == "__main__":
    main()
