-- =====================================================================
-- TAREA 9 · Programacion del lado del servidor: disparadores (triggers)
-- IoT Analytics Dashboard (NexoDev Solutions) · PostgreSQL 16
-- El consumidor solo hace INSERT en lecturas; los disparadores automatizan
-- el resto: validar, alertar, auditar, actualizar campos y avisar a
-- RabbitMQ (patron outbox + LISTEN/NOTIFY).
-- =====================================================================

-- ============================== TABLAS ==============================
CREATE TABLE dispositivos (
    id             SERIAL PRIMARY KEY,
    codigo         VARCHAR(20) NOT NULL UNIQUE,
    nombre         VARCHAR(60) NOT NULL,
    ubicacion      VARCHAR(60) NOT NULL,
    estado         VARCHAR(15) NOT NULL DEFAULT 'activo'
                   CHECK (estado IN ('activo', 'mantenimiento', 'inactivo')),
    actualizado_en  TIMESTAMPTZ,
    actualizado_por TEXT
);

CREATE TABLE sensores (
    id              SERIAL PRIMARY KEY,
    dispositivo_id  INT NOT NULL REFERENCES dispositivos(id),
    codigo          VARCHAR(20) NOT NULL UNIQUE,
    tipo            VARCHAR(20) NOT NULL CHECK (tipo IN ('temperatura', 'vibracion', 'energia')),
    unidad          VARCHAR(10) NOT NULL,
    umbral_max      NUMERIC(10,2) NOT NULL CHECK (umbral_max > 0),
    actualizado_en  TIMESTAMPTZ,
    actualizado_por TEXT
);

CREATE TABLE lecturas (
    id        BIGSERIAL PRIMARY KEY,
    sensor_id INT NOT NULL REFERENCES sensores(id),
    valor     NUMERIC(10,2) NOT NULL,
    medido_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_lecturas_sensor_fecha ON lecturas (sensor_id, medido_en DESC);
CREATE INDEX idx_lecturas_fecha        ON lecturas (medido_en DESC);

CREATE TABLE alertas (
    id         SERIAL PRIMARY KEY,
    lectura_id BIGINT NOT NULL REFERENCES lecturas(id) ON DELETE CASCADE,
    sensor_id  INT NOT NULL REFERENCES sensores(id),
    severidad  VARCHAR(12) NOT NULL CHECK (severidad IN ('advertencia', 'critica')),
    valor      NUMERIC(10,2) NOT NULL,
    umbral     NUMERIC(10,2) NOT NULL,
    creada_en  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    atendida   BOOLEAN NOT NULL DEFAULT FALSE,
    nota       TEXT
);
CREATE INDEX idx_alertas_pendientes ON alertas (sensor_id) WHERE NOT atendida;

-- Bitacora de auditoria: solo se inserta, nunca se modifica ni se borra
CREATE TABLE auditoria (
    id               BIGSERIAL PRIMARY KEY,
    tabla            TEXT NOT NULL,
    operacion        TEXT NOT NULL CHECK (operacion IN ('INSERT', 'UPDATE', 'DELETE', 'RECHAZO')),
    registro_id      TEXT,
    datos_anteriores JSONB,
    datos_nuevos     JSONB,
    cambios          JSONB,
    detalle          TEXT,
    usuario          TEXT NOT NULL DEFAULT session_user,
    ocurrido_en      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX idx_auditoria_fecha ON auditoria (ocurrido_en DESC);

-- Bandeja de salida (outbox): eventos que el puente publica en RabbitMQ
CREATE TABLE eventos_salida (
    id           BIGSERIAL PRIMARY KEY,
    routing_key  TEXT NOT NULL,
    payload      JSONB NOT NULL,
    creado_en    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    publicado_en TIMESTAMPTZ,
    intentos     INT NOT NULL DEFAULT 0
);
CREATE INDEX idx_eventos_pendientes ON eventos_salida (id) WHERE publicado_en IS NULL;

-- Estado en vivo de cada sensor, lo mantiene un disparador por sentencia
CREATE TABLE estado_sensor (
    sensor_id      INT PRIMARY KEY REFERENCES sensores(id),
    ultima_lectura TIMESTAMPTZ NOT NULL,
    ultimo_valor   NUMERIC(10,2) NOT NULL,
    total_lecturas BIGINT NOT NULL,
    actualizado_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ========================= FUNCIONES AUXILIARES =========================
CREATE FUNCTION fn_clasificar_lectura(p_valor NUMERIC, p_umbral NUMERIC)
RETURNS TEXT LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN p_valor > p_umbral * 1.2 THEN 'critica'
                WHEN p_valor > p_umbral       THEN 'advertencia'
                ELSE 'normal' END;
$$;

CREATE FUNCTION fn_valor_valido(p_tipo TEXT, p_valor NUMERIC)
RETURNS BOOLEAN LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE p_tipo
        WHEN 'temperatura' THEN p_valor BETWEEN -40 AND 150
        WHEN 'vibracion'   THEN p_valor BETWEEN 0 AND 50
        WHEN 'energia'     THEN p_valor BETWEEN 0 AND 1000
        ELSE FALSE END;
$$;

-- ============================ DISPARADORES ============================

-- T1. BEFORE INSERT en lecturas: validar la lectura antes de guardarla.
-- Si el valor esta fuera del rango fisico o el dispositivo no esta activo,
-- registra el rechazo en auditoria, encola un evento para RabbitMQ y
-- devuelve NULL: la fila no se inserta, pero la transaccion continua.
CREATE FUNCTION trg_lecturas_validar() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
    v_sensor  RECORD;
    v_motivo  TEXT;
BEGIN
    SELECT s.codigo, s.tipo, s.unidad, d.codigo AS dispositivo, d.estado
      INTO v_sensor
      FROM sensores s JOIN dispositivos d ON d.id = s.dispositivo_id
     WHERE s.id = NEW.sensor_id;

    IF v_sensor.estado <> 'activo' THEN
        v_motivo := format('dispositivo %s en %s', v_sensor.dispositivo, v_sensor.estado);
    ELSIF NOT fn_valor_valido(v_sensor.tipo, NEW.valor) THEN
        v_motivo := format('valor %s fuera del rango fisico de %s', NEW.valor, v_sensor.tipo);
    END IF;

    IF v_motivo IS NOT NULL THEN
        INSERT INTO auditoria (tabla, operacion, datos_nuevos, detalle)
        VALUES ('lecturas', 'RECHAZO', to_jsonb(NEW) || jsonb_build_object('sensor', v_sensor.codigo), v_motivo);

        INSERT INTO eventos_salida (routing_key, payload)
        VALUES ('lectura.rechazada', jsonb_build_object(
                'sensor', v_sensor.codigo, 'valor', NEW.valor, 'unidad', v_sensor.unidad,
                'motivo', v_motivo, 'medido_en', NEW.medido_en));
        RETURN NULL;                                   -- descarta la fila
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER t1_lecturas_validar
BEFORE INSERT ON lecturas
FOR EACH ROW EXECUTE FUNCTION trg_lecturas_validar();

-- T2. AFTER INSERT en lecturas: generar la alerta si supera el umbral.
CREATE FUNCTION trg_lecturas_alerta() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
    v_umbral NUMERIC;
    v_sev    TEXT;
BEGIN
    SELECT umbral_max INTO v_umbral FROM sensores WHERE id = NEW.sensor_id;
    v_sev := fn_clasificar_lectura(NEW.valor, v_umbral);
    IF v_sev <> 'normal' THEN
        INSERT INTO alertas (lectura_id, sensor_id, severidad, valor, umbral, creada_en)
        VALUES (NEW.id, NEW.sensor_id, v_sev, NEW.valor, v_umbral, NEW.medido_en);
    END IF;
    RETURN NULL;                                       -- en AFTER se ignora
END $$;

CREATE TRIGGER t2_lecturas_alerta
AFTER INSERT ON lecturas
FOR EACH ROW EXECUTE FUNCTION trg_lecturas_alerta();

-- T3. AFTER INSERT en alertas: encolar la notificacion para RabbitMQ.
-- La routing key indica la severidad: alerta.advertencia / alerta.critica.
CREATE FUNCTION trg_alertas_notificar() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
    INSERT INTO eventos_salida (routing_key, payload)
    SELECT 'alerta.' || NEW.severidad,
           jsonb_build_object('alerta_id', NEW.id, 'sensor', s.codigo, 'dispositivo', d.codigo,
                              'valor', NEW.valor, 'umbral', NEW.umbral, 'unidad', s.unidad,
                              'severidad', NEW.severidad, 'medido_en', NEW.creada_en)
    FROM sensores s JOIN dispositivos d ON d.id = s.dispositivo_id
    WHERE s.id = NEW.sensor_id;
    RETURN NULL;
END $$;

CREATE TRIGGER t3_alertas_notificar
AFTER INSERT ON alertas
FOR EACH ROW EXECUTE FUNCTION trg_alertas_notificar();

-- T4. AFTER INSERT en eventos_salida, POR SENTENCIA: despertar al puente.
-- pg_notify se entrega al confirmar la transaccion; una sola vez por sentencia.
CREATE FUNCTION trg_eventos_avisar() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('eventos_iot', 'nuevos');
    RETURN NULL;
END $$;

CREATE TRIGGER t4_eventos_avisar
AFTER INSERT ON eventos_salida
FOR EACH STATEMENT EXECUTE FUNCTION trg_eventos_avisar();

-- T5. AFTER INSERT en lecturas, POR SENTENCIA con tabla de transicion:
-- actualiza estado_sensor una vez por sentencia, aunque se inserten miles de filas.
CREATE FUNCTION trg_lecturas_estado() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
BEGIN
    INSERT INTO estado_sensor AS e (sensor_id, ultima_lectura, ultimo_valor, total_lecturas)
    SELECT DISTINCT ON (sensor_id)
           sensor_id, medido_en, valor, COUNT(*) OVER (PARTITION BY sensor_id)
    FROM nuevas
    ORDER BY sensor_id, medido_en DESC
    ON CONFLICT (sensor_id) DO UPDATE SET
        ultimo_valor   = CASE WHEN EXCLUDED.ultima_lectura >= e.ultima_lectura
                              THEN EXCLUDED.ultimo_valor ELSE e.ultimo_valor END,
        ultima_lectura = GREATEST(e.ultima_lectura, EXCLUDED.ultima_lectura),
        total_lecturas = e.total_lecturas + EXCLUDED.total_lecturas,
        actualizado_en = NOW();
    RETURN NULL;
END $$;

CREATE TRIGGER t5_lecturas_estado
AFTER INSERT ON lecturas
REFERENCING NEW TABLE AS nuevas
FOR EACH STATEMENT EXECUTE FUNCTION trg_lecturas_estado();

-- T6. Auditoria generica (una sola funcion para varias tablas).
-- Guarda los datos antes y despues, solo las columnas que cambiaron y el
-- usuario real de la sesion (session_user, no el dueno de la funcion).
CREATE FUNCTION trg_auditar() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
    v_viejo   JSONB := CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN to_jsonb(OLD) END;
    v_nuevo   JSONB := CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN to_jsonb(NEW) END;
    v_cambios JSONB;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        SELECT jsonb_object_agg(n.key, jsonb_build_object('antes', o.value, 'despues', n.value))
          INTO v_cambios
          FROM jsonb_each(v_nuevo) n JOIN jsonb_each(v_viejo) o USING (key)
         WHERE n.value IS DISTINCT FROM o.value
           AND n.key NOT IN ('actualizado_en', 'actualizado_por');
        IF v_cambios IS NULL THEN RETURN NULL; END IF;   -- UPDATE sin cambios reales
    END IF;

    INSERT INTO auditoria (tabla, operacion, registro_id, datos_anteriores, datos_nuevos, cambios)
    VALUES (TG_TABLE_NAME, TG_OP, COALESCE(v_nuevo, v_viejo) ->> 'id', v_viejo, v_nuevo, v_cambios);
    RETURN NULL;
END $$;

CREATE TRIGGER t6_auditar_sensores
AFTER INSERT OR UPDATE OR DELETE ON sensores
FOR EACH ROW EXECUTE FUNCTION trg_auditar();

CREATE TRIGGER t6_auditar_dispositivos
AFTER INSERT OR UPDATE OR DELETE ON dispositivos
FOR EACH ROW EXECUTE FUNCTION trg_auditar();

-- En lecturas solo se auditan correcciones y borrados (los INSERT son el flujo normal)
CREATE TRIGGER t6_auditar_lecturas
AFTER UPDATE OR DELETE ON lecturas
FOR EACH ROW EXECUTE FUNCTION trg_auditar();

-- T7. BEFORE UPDATE: actualizacion automatica de campos de control.
CREATE FUNCTION trg_marcar_actualizacion() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    NEW.actualizado_en  := NOW();
    NEW.actualizado_por := session_user;
    RETURN NEW;                                        -- devuelve la fila modificada
END $$;

CREATE TRIGGER t7_sensores_marcar
BEFORE UPDATE ON sensores
FOR EACH ROW EXECUTE FUNCTION trg_marcar_actualizacion();

CREATE TRIGGER t7_dispositivos_marcar
BEFORE UPDATE ON dispositivos
FOR EACH ROW EXECUTE FUNCTION trg_marcar_actualizacion();

-- T8. AFTER UPDATE OF estado en dispositivos, con condicion WHEN:
-- al pasar a mantenimiento o inactivo, cierra sus alertas pendientes y avisa.
CREATE FUNCTION trg_dispositivo_fuera_servicio() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE
    v_cerradas INT;
BEGIN
    UPDATE alertas a SET atendida = TRUE,
                         nota = format('Cerrada automaticamente: %s paso a %s', NEW.codigo, NEW.estado)
      FROM sensores s
     WHERE s.id = a.sensor_id AND s.dispositivo_id = NEW.id AND NOT a.atendida;
    GET DIAGNOSTICS v_cerradas = ROW_COUNT;

    INSERT INTO eventos_salida (routing_key, payload)
    VALUES ('dispositivo.' || NEW.estado, jsonb_build_object(
            'dispositivo', NEW.codigo, 'estado_anterior', OLD.estado, 'estado', NEW.estado,
            'alertas_cerradas', v_cerradas, 'usuario', session_user));
    RETURN NULL;
END $$;

CREATE TRIGGER t8_dispositivo_fuera_servicio
AFTER UPDATE OF estado ON dispositivos
FOR EACH ROW
WHEN (OLD.estado = 'activo' AND NEW.estado <> 'activo')
EXECUTE FUNCTION trg_dispositivo_fuera_servicio();

-- T9. BEFORE UPDATE, DELETE o TRUNCATE en auditoria: la bitacora es inmutable.
CREATE FUNCTION trg_auditoria_inmutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'La auditoria es de solo insercion: no se permite % ', TG_OP
          USING ERRCODE = 'insufficient_privilege';
END $$;

CREATE TRIGGER t9_auditoria_inmutable
BEFORE UPDATE OR DELETE ON auditoria
FOR EACH ROW EXECUTE FUNCTION trg_auditoria_inmutable();

-- TRUNCATE no dispara triggers por fila: se protege con uno por sentencia
CREATE TRIGGER t9_auditoria_sin_truncate
BEFORE TRUNCATE ON auditoria
FOR EACH STATEMENT EXECUTE FUNCTION trg_auditoria_inmutable();

-- ======================= USUARIOS Y PERMISOS =======================
-- Consumidor: solo inserta lecturas; todo lo demas lo hacen los disparadores
CREATE ROLE consumidor_iot LOGIN PASSWORD 'consumidor123';
GRANT SELECT (id, codigo) ON sensores TO consumidor_iot;
GRANT INSERT ON lecturas TO consumidor_iot;
GRANT USAGE ON SEQUENCE lecturas_id_seq TO consumidor_iot;

-- Puente hacia RabbitMQ: solo lee y marca la bandeja de salida
CREATE ROLE puente_rmq LOGIN PASSWORD 'puente123';
GRANT SELECT, UPDATE (publicado_en, intentos) ON eventos_salida TO puente_rmq;

-- Operador: cambia umbrales y estados (para probar la auditoria)
CREATE ROLE operador_planta LOGIN PASSWORD 'operador123';
GRANT SELECT ON sensores, dispositivos, lecturas TO operador_planta;
GRANT UPDATE (umbral_max) ON sensores TO operador_planta;
GRANT UPDATE (estado) ON dispositivos TO operador_planta;
GRANT UPDATE (valor) ON lecturas TO operador_planta;

-- Grafana: solo lectura
CREATE ROLE grafana_lector LOGIN PASSWORD 'grafana123';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO grafana_lector;

-- ========================== DATOS INICIALES ==========================
INSERT INTO dispositivos (codigo, nombre, ubicacion) VALUES
('PRE-03', 'Prensa hidraulica 3', 'Linea 1'),
('TOR-01', 'Torno CNC 1',        'Linea 2'),
('EMB-02', 'Embotelladora 2',    'Linea 3');

INSERT INTO sensores (dispositivo_id, codigo, tipo, unidad, umbral_max) VALUES
(1, 'PRE03-TEMP', 'temperatura', '°C',   85.00),
(1, 'PRE03-VIB',  'vibracion',   'mm/s',  7.00),
(1, 'PRE03-ENE',  'energia',     'kWh',  30.00),
(2, 'TOR01-TEMP', 'temperatura', '°C',   80.00),
(2, 'TOR01-VIB',  'vibracion',   'mm/s',  6.00),
(2, 'TOR01-ENE',  'energia',     'kWh',  25.00),
(3, 'EMB02-TEMP', 'temperatura', '°C',   75.00),
(3, 'EMB02-VIB',  'vibracion',   'mm/s',  5.00),
(3, 'EMB02-ENE',  'energia',     'kWh',  20.00);

-- Historico de 1 dia (una lectura cada 30 s por sensor). Se carga con los
-- disparadores de usuario desactivados para no publicar miles de alertas
-- antiguas en RabbitMQ; despues se calcula el estado una sola vez.
ALTER TABLE lecturas DISABLE TRIGGER USER;
SELECT setseed(0.9);
INSERT INTO lecturas (sensor_id, valor, medido_en)
SELECT s.id, ROUND((s.umbral_max * (0.80 + (random() - 0.5) * 0.08))::numeric, 2), t
FROM sensores s
CROSS JOIN generate_series(NOW() - INTERVAL '1 day', NOW() - INTERVAL '1 minute',
                           INTERVAL '30 seconds') AS t;
ALTER TABLE lecturas ENABLE TRIGGER USER;

INSERT INTO estado_sensor (sensor_id, ultima_lectura, ultimo_valor, total_lecturas)
SELECT DISTINCT ON (sensor_id) sensor_id, medido_en, valor, COUNT(*) OVER (PARTITION BY sensor_id)
FROM lecturas ORDER BY sensor_id, medido_en DESC;
ANALYZE;
