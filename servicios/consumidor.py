"""
Consumidor: toma las lecturas de RabbitMQ y solo hace un INSERT en la tabla
lecturas con el usuario consumidor_iot. No valida ni genera alertas: de eso
se encargan los disparadores de la base de datos.
"""
import json
import os
import time

import pika
import psycopg

CONNINFO = (f"host={os.getenv('DB_HOST', 'datos')} dbname=iotanalytics "
            f"user={os.getenv('DB_USER', 'consumidor_iot')} "
            f"password={os.environ['DB_PASSWORD']} sslmode=require")

INSERTAR = """INSERT INTO lecturas (sensor_id, valor, medido_en)
              SELECT id, %s::numeric, %s::timestamptz FROM sensores WHERE codigo = %s"""


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
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError:
            print("RabbitMQ no disponible, reintentando...")
            time.sleep(3)


def main():
    bd = conectar_bd()
    print("Conectado a PostgreSQL con SSL:", bd.info.pgconn.ssl_in_use)
    con = conectar_rabbit()
    canal = con.channel()
    canal.queue_declare(queue="lecturas", durable=True)
    canal.basic_qos(prefetch_count=50)
    cuenta = {"insertadas": 0, "descartadas": 0}

    def al_recibir(ch, metodo, _props, cuerpo):
        lectura = json.loads(cuerpo)
        filas = bd.execute(INSERTAR, (lectura["valor"], lectura["medido_en"], lectura["sensor"])).rowcount
        if filas:
            cuenta["insertadas"] += 1
        else:                                  # el disparador T1 devolvio NULL
            cuenta["descartadas"] += 1
            print(f"Descartada por t1_lecturas_validar: {lectura['sensor']} = {lectura['valor']}")
        ch.basic_ack(delivery_tag=metodo.delivery_tag)

    def reporte():
        print(f"{cuenta['insertadas']} insertadas · {cuenta['descartadas']} descartadas por disparador")
        con.call_later(30, reporte)

    con.call_later(30, reporte)
    canal.basic_consume(queue="lecturas", on_message_callback=al_recibir)
    print("Consumidor escuchando la cola 'lecturas'")
    canal.start_consuming()


if __name__ == "__main__":
    main()
