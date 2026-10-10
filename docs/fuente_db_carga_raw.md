# Cómo poner la tabla raw de la entrega 1 en el MySQL del proyecto

La rama DB (`read_db`) lee `raw_saber11` de la base `RAW_DB_NAME` (por defecto `saber11`) en el MySQL de Docker. Esa tabla viene de la entrega 1. Se hace **una sola vez** por computador.

## 1. Exportar la tabla desde el MySQL donde quedó la entrega 1

En la terminal (funciona igual en Windows PowerShell, Mac y Linux). Cambia `<base_entrega1>` por el nombre de la base de la entrega 1 y `root` por tu usuario:

```bash
mkdir dumps
mysqldump -u root -p --single-transaction --no-tablespaces --default-character-set=utf8mb4 --result-file=dumps/raw_saber11.sql <base_entrega1> raw_saber11
```

Usa `--result-file` y no `>`: en PowerShell, `>` guarda el archivo en UTF-16 y MySQL no lo puede leer.

Si `mysqldump` no se reconoce en Windows, está en `C:\Program Files\MySQL\MySQL Server 8.0\bin\` (usa la ruta completa entre comillas) o en MySQL Workbench: *Server → Data Export*, marca solo `raw_saber11`, *Export to Self-Contained File*.

## 2. Importarla en el MySQL de Docker

Con `docker compose up -d` ya corriendo:

```bash
docker compose cp dumps/raw_saber11.sql mysql:/tmp/raw_saber11.sql
docker compose exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$RAW_DB_NAME" < /tmp/raw_saber11.sql'
```

## 3. Verificar

```bash
docker compose exec mysql sh -c 'mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -e "SELECT COUNT(*) FROM $RAW_DB_NAME.raw_saber11; SHOW TABLES FROM $DW_DB_NAME;"'
```

Debe mostrar el número de filas de la tabla raw y las 5 tablas del DW vacías.

El archivo `dumps/raw_saber11.sql` **no se sube a GitHub** (está en `.gitignore`). Para Lauren y Carlos, compártelo por Drive.
