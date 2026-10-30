# IoT Analytics Dashboard · Disparadores en PostgreSQL (Tarea 9)

NexoDev Solutions · Programación de Base de Datos (TESE) · Luis Gael Monterrubio López

## Documentación

📄 [Abrir la documentación de la Tarea 9 (PDF)](docs/Documentacion_Tarea9.pdf)

El consumidor solo hace `INSERT` en `lecturas`; los disparadores automatizan todo lo demás: validan, generan alertas, auditan cambios, actualizan campos y envían las alertas a RabbitMQ.

```
Simulador ─► RabbitMQ ─► Consumidor ─► INSERT lecturas ─► disparadores ─► eventos_salida
                ▲                                                          │ pg_notify
                └──── exchange alertas_iot ◄──── Puente (LISTEN) ◄─────────┘
                          └─► Notificador
```

## Disparadores

| Disparador | Evento | Qué hace |
|---|---|---|
| `t1_lecturas_validar` | BEFORE INSERT, por fila | Descarta lecturas fuera del rango físico o de equipos no activos; las audita y encola el evento |
| `t2_lecturas_alerta` | AFTER INSERT, por fila | Crea la alerta de advertencia o crítica si supera el umbral |
| `t3_alertas_notificar` | AFTER INSERT en alertas | Encola la alerta en `eventos_salida` (`alerta.advertencia` / `alerta.critica`) |
| `t4_eventos_avisar` | AFTER INSERT, por sentencia | `pg_notify('eventos_iot')` para despertar al puente |
| `t5_lecturas_estado` | AFTER INSERT, por sentencia con tabla de transición | Actualiza `estado_sensor` una vez por sentencia |
| `t6_auditar_*` | AFTER INSERT/UPDATE/DELETE | Auditoría genérica: datos antes y después, columnas cambiadas y usuario |
| `t7_*_marcar` | BEFORE UPDATE | Llena `actualizado_en` y `actualizado_por` |
| `t8_dispositivo_fuera_servicio` | AFTER UPDATE OF estado, con WHEN | Cierra las alertas pendientes del equipo y avisa |
| `t9_auditoria_*` | BEFORE UPDATE/DELETE/TRUNCATE | Hace la bitácora inmutable |

## Levantar

```bash
docker compose up -d --build
```

- Grafana: http://localhost:3009 (admin / admin123)
- RabbitMQ: http://localhost:15679 (iot / iot123)

Pruebas de funcionamiento y rendimiento:

```bash
docker exec -i triggers-datos psql -U postgres -d iotanalytics < datos/pruebas.sql
```
