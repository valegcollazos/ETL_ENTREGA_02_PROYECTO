-- ============================================================================
-- Data Warehouse ENTREGA 2 — Saber 11 Valle del Cauca (2019, 2020, 2025)
-- Base nueva y separada del DW nacional de la entrega 1.
--
-- Cambios respecto a esquema_dataset.sql (entrega 1):
--   1. COLLATE utf8mb4_bin en todas las tablas: compara texto EXACTO. Con la
--      collation por defecto (_ai_ci) "Si" y "Sí" o "VALLE" y "Valle" cuentan como
--      el mismo valor; INSERT IGNORE descartaba uno y luego el cruce en pandas
--      (que sí distingue) dejaba filas de la fact sin llave.
--   2. VARCHAR más anchos: INSERT IGNORE recorta en silencio el texto largo.
--   3. dim_tiempo.etapa_pandemia (Antes / Durante / Después) para el dashboard.
--
-- Uso manual (si no se creó con Docker):  mysql -u <usuario> -p saber11_dw_e2 < sql/esquema_dw.sql
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_colegio (
    id_colegio INT AUTO_INCREMENT PRIMARY KEY,
    cole_cod_icfes INT NOT NULL,
    cole_nombre_establecimiento VARCHAR(255),
    cole_naturaleza VARCHAR(50),
    cole_calendario VARCHAR(50),
    cole_area_ubicacion VARCHAR(50),
    UNIQUE KEY uq_colegio (cole_cod_icfes)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion INT AUTO_INCREMENT PRIMARY KEY,
    estu_depto_reside VARCHAR(100) NOT NULL,
    estu_mcpio_reside VARCHAR(100) NOT NULL,
    UNIQUE KEY uq_ubicacion (estu_depto_reside, estu_mcpio_reside)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS dim_socioeconomica (
    id_socioeconomico INT AUTO_INCREMENT PRIMARY KEY,
    fami_estratovivienda VARCHAR(50) NOT NULL,
    fami_educacionpadre VARCHAR(120) NOT NULL,
    fami_educacionmadre VARCHAR(120) NOT NULL,
    fami_tieneinternet VARCHAR(20) NOT NULL,
    fami_tienecomputador VARCHAR(20) NOT NULL,
    fami_personashogar VARCHAR(30) NOT NULL,
    estu_nse_individual INT NOT NULL,
    UNIQUE KEY uq_socioeco (
        fami_estratovivienda, fami_educacionpadre, fami_educacionmadre,
        fami_tieneinternet, fami_tienecomputador, fami_personashogar,
        estu_nse_individual
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS dim_tiempo (
    id_tiempo INT AUTO_INCREMENT PRIMARY KEY,
    periodo VARCHAR(10) NOT NULL,
    anio INT NOT NULL,
    semestre INT,
    etapa_pandemia VARCHAR(20) NOT NULL,
    UNIQUE KEY uq_tiempo (periodo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE IF NOT EXISTS fact_resultadossaber11 (
    id_resultado BIGINT AUTO_INCREMENT PRIMARY KEY,
    punt_global INT,
    punt_matematicas INT,
    punt_lectura_critica INT,
    punt_naturales INT,
    punt_sociales_ciudadanas INT,
    punt_ingles INT,
    estu_inse_individual FLOAT,
    id_colegio INT NOT NULL,
    id_socioeconomico INT NOT NULL,
    id_tiempo INT NOT NULL,
    id_ubicacion INT NOT NULL,
    bk_estu_consecutivo VARCHAR(50) NOT NULL,
    UNIQUE KEY uq_estu_consecutivo (bk_estu_consecutivo),
    CONSTRAINT fk_fact_colegio   FOREIGN KEY (id_colegio)        REFERENCES dim_colegio (id_colegio),
    CONSTRAINT fk_fact_socioeco  FOREIGN KEY (id_socioeconomico) REFERENCES dim_socioeconomica (id_socioeconomico),
    CONSTRAINT fk_fact_tiempo    FOREIGN KEY (id_tiempo)         REFERENCES dim_tiempo (id_tiempo),
    CONSTRAINT fk_fact_ubicacion FOREIGN KEY (id_ubicacion)      REFERENCES dim_ubicacion (id_ubicacion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
