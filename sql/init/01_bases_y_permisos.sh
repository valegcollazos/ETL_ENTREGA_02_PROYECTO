#!/bin/bash
# Se ejecuta UNA sola vez, la primera vez que arranca el contenedor de MySQL
# (cuando el volumen está vacío). Crea las dos bases y da permisos al usuario del .env.
set -e
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" <<SQL
CREATE DATABASE IF NOT EXISTS \`${RAW_DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS \`${DW_DB_NAME}\`  CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
GRANT ALL PRIVILEGES ON \`${RAW_DB_NAME}\`.* TO '${MYSQL_USER}'@'%';
GRANT ALL PRIVILEGES ON \`${DW_DB_NAME}\`.*  TO '${MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
SQL
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" "$DW_DB_NAME" < /sql/esquema_dw.sql
echo "Bases ${RAW_DB_NAME} y ${DW_DB_NAME} listas."
