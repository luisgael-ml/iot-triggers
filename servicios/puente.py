"""
Puente PostgreSQL -> RabbitMQ (patron outbox).
Los disparadores no pueden hablar con RabbitMQ: dejan cada evento en la tabla
eventos_salida y avisan con pg_notify('eventos_iot'). Este servicio escucha ese
aviso (LISTEN), publica los eventos pendientes en el exchange 'alertas_iot' y
los marca como publicados. Si el puente se detiene, los eventos esperan en la
tabla y se publican al volver: ninguna alerta se pierde.
"""
import json
import os
import time

import pika
import psycopg

CONNINFO = (f"host={os.getenv('DB_HOST', 'datos')} dbname=iotanalytics "
            f"user={os.getenv('DB_USER', 'puente_rmq')} "
            f"password={os.environ['DB_PASSWORD']} sslmode=require")
EXCHANGE = "alertas_iot"

PENDIENTES = """SELECT id, routing_key, payload FROM eventos_salida
                WHERE publicado_en IS NULL ORDER BY id
                LIMIT 100 FOR UPDATE SKIP LOCKED"""


def conectar_bd():
    while True:
        try:
            return psycopg.connect(CONNINFO, autocommit=True)
        except psycopg.OperationalError:
            print("PostgreSQL no disponible, reintentando...")
            time.sleep(3)


def conectar_rabbit():
    params = pika.URLParameters(os.getenv("RABBIT_URL", "amqp://iot:iot123@mensajes:5672/"))
    while True:
        try:
            con = pika.BlockingConnection(params)
            canal = con.channel()
            canal.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            canal.confirm_delivery()           # RabbitMQ confirma cada mensaje recibido
            return con, canal
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ no disponible, reintentando...")
            time.sleep(3)


def publicar_pendientes(bd, canal):
    publicados = 0
    with bd.transaction():
        for id_, clave, payload in bd.execute(PENDIENTES).fetchall():
            try:
                canal.basic_publish(exchange=EXCHANGE, routing_key=clave,
                                    body=json.dumps(payload, default=str, ensure_ascii=False),
                                    properties=pika.BasicProperties(
                                        content_type="application/json", delivery_mode=2))
                bd.execute("UPDATE eventos_salida SET publicado_en = clock_timestamp(), "
                           "intentos = intentos + 1 WHERE id = %s", (id_,))
                publicados += 1
            except pika.exceptions.AMQPError as e:
                bd.execute("UPDATE eventos_salida SET intentos = intentos + 1 WHERE id = %s", (id_,))
                print(f"No se pudo publicar el evento {id_}: {e}")
                break
    return publicados


def main():
    bd = conectar_bd()
    print("Conectado a PostgreSQL con SSL:", bd.info.pgconn.ssl_in_use)
    rmq, canal = conectar_rabbit()
    bd.execute("LISTEN eventos_iot")
    print("Puente escuchando el canal 'eventos_iot'")
    total = publicar_pendientes(bd, canal)     # lo que quedo pendiente mientras no corria
    while True:
        # espera un NOTIFY hasta 5 s; si no llega, revisa de todos modos
        for _ in bd.notifies(timeout=5, stop_after=1):
            pass
        n = publicar_pendientes(bd, canal)
        if n:
            total += n
            print(f"Publicados {n} eventos en '{EXCHANGE}' (total {total})")
        rmq.process_data_events(time_limit=0)  # mantiene viva la conexion (heartbeat)


if __name__ == "__main__":
    main()
