-- =====================================================================
-- Pruebas de la Tarea 9: funcionamiento y rendimiento de los disparadores
-- =====================================================================
\pset footer off

\echo '=== A. Lectura normal: T1 la acepta, T2 no genera alerta ==='
INSERT INTO lecturas (sensor_id, valor)
SELECT id, 70 FROM sensores WHERE codigo = 'PRE03-TEMP' RETURNING id, valor;

\echo '=== B. Lectura critica: T2 crea la alerta y T3 la encola para RabbitMQ ==='
INSERT INTO lecturas (sensor_id, valor)
SELECT id, 110 FROM sensores WHERE codigo = 'PRE03-TEMP' RETURNING id, valor;
SELECT a.id AS alerta, s.codigo AS sensor, a.severidad, a.valor, a.umbral
FROM alertas a JOIN sensores s ON s.id = a.sensor_id ORDER BY a.id DESC LIMIT 1;

\echo '=== C. Valor imposible: T1 la descarta (INSERT 0 0), la audita y la encola ==='
INSERT INTO lecturas (sensor_id, valor)
SELECT id, -999 FROM sensores WHERE codigo = 'EMB02-VIB';
SELECT id, tabla, operacion, datos_nuevos ->> 'sensor' AS sensor, detalle
FROM auditoria WHERE operacion = 'RECHAZO' ORDER BY id DESC LIMIT 1;

\echo '=== D. Bandeja de salida: eventos generados por los disparadores ==='
SELECT id, routing_key, payload ->> 'sensor' AS sensor, payload ->> 'valor' AS valor,
       publicado_en IS NOT NULL AS publicado
FROM eventos_salida ORDER BY id DESC LIMIT 3;

\echo '=== E. T5: estado_sensor actualizado por el disparador por sentencia ==='
SELECT s.codigo, e.ultimo_valor, e.total_lecturas, e.ultima_lectura::time(0) AS hora
FROM estado_sensor e JOIN sensores s ON s.id = e.sensor_id ORDER BY s.id LIMIT 3;

\echo '=== F. Rendimiento: 10,000 lecturas normales (sin alertas) en una sola sentencia ==='
\timing on
\echo '--- F1. Sin disparadores ---'
BEGIN;
ALTER TABLE lecturas DISABLE TRIGGER USER;
INSERT INTO lecturas (sensor_id, valor, medido_en)
SELECT s.id, s.umbral_max * 0.8, NOW() + g * INTERVAL '1 ms'
FROM generate_series(1, 10000) g JOIN sensores s ON s.id = 1 + (g % 9);
ROLLBACK;

\echo '--- F2. Con disparadores (T5 por SENTENCIA) ---'
BEGIN;
INSERT INTO lecturas (sensor_id, valor, medido_en)
SELECT s.id, s.umbral_max * 0.8, NOW() + g * INTERVAL '1 ms'
FROM generate_series(1, 10000) g JOIN sensores s ON s.id = 1 + (g % 9);
ROLLBACK;

\echo '--- F3. Con disparadores (T5 reescrito POR FILA) ---'
BEGIN;
ALTER TABLE lecturas DISABLE TRIGGER t5_lecturas_estado;
CREATE FUNCTION trg_lecturas_estado_fila() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO estado_sensor AS e (sensor_id, ultima_lectura, ultimo_valor, total_lecturas)
    VALUES (NEW.sensor_id, NEW.medido_en, NEW.valor, 1)
    ON CONFLICT (sensor_id) DO UPDATE SET
        ultimo_valor   = CASE WHEN EXCLUDED.ultima_lectura >= e.ultima_lectura
                              THEN EXCLUDED.ultimo_valor ELSE e.ultimo_valor END,
        ultima_lectura = GREATEST(e.ultima_lectura, EXCLUDED.ultima_lectura),
        total_lecturas = e.total_lecturas + 1,
        actualizado_en = NOW();
    RETURN NULL;
END $$;
CREATE TRIGGER t5_fila AFTER INSERT ON lecturas
FOR EACH ROW EXECUTE FUNCTION trg_lecturas_estado_fila();
INSERT INTO lecturas (sensor_id, valor, medido_en)
SELECT s.id, s.umbral_max * 0.8, NOW() + g * INTERVAL '1 ms'
FROM generate_series(1, 10000) g JOIN sensores s ON s.id = 1 + (g % 9);
ROLLBACK;
\timing off
