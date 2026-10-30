"""
Notificador: suscriptor del exchange 'alertas_iot'. Representa al equipo de
operacion que recibe las alertas en tiempo real (correo, Slack, celular...).
Cola 'notificaciones' -> todos los eventos (#)
Cola 'solo_criticas'  -> solo alerta.critica (muestra el ruteo por routing key)
"""
import json
import os
import time

import pika

EXCHANGE = "alertas_iot"
ICONOS = {"alerta.critica": "[CRITICA]", "alerta.advertencia": "[ADVERTENCIA]",
          "lectura.rechazada": "[RECHAZADA]"}


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
    canal.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    canal.queue_declare(queue="notificaciones", durable=True)
    canal.queue_bind(queue="notificaciones", exchange=EXCHANGE, routing_key="#")
    canal.queue_declare(queue="solo_criticas", durable=True,
                        arguments={"x-max-length": 1000})
    canal.queue_bind(queue="solo_criticas", exchange=EXCHANGE, routing_key="alerta.critica")

    def al_recibir(ch, metodo, _props, cuerpo):
        e = json.loads(cuerpo)
        clave = metodo.routing_key
        if clave.startswith("alerta."):
            texto = f"{e['sensor']} ({e['dispositivo']}) = {e['valor']} {e['unidad']}, umbral {e['umbral']}"
        elif clave == "lectura.rechazada":
            texto = f"{e['sensor']} = {e['valor']} -> {e['motivo']}"
        else:
            texto = (f"{e['dispositivo']}: {e['estado_anterior']} -> {e['estado']} "
                     f"por {e['usuario']}, {e['alertas_cerradas']} alertas cerradas")
        print(f"{ICONOS.get(clave, '[DISPOSITIVO]')} {texto}")
        ch.basic_ack(delivery_tag=metodo.delivery_tag)

    canal.basic_consume(queue="notificaciones", on_message_callback=al_recibir)
    print(f"Notificador suscrito a '{EXCHANGE}'")
    canal.start_consuming()


if __name__ == "__main__":
    main()
