cat << EOF > init-db.sql
CREATE DATABASE ${POSTGRES_DB};
EOF