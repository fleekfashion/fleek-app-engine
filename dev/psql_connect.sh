PORT=5431
DB=ktest
psql "host=127.0.0.1 port=$PORT sslmode=disable dbname=$DB user=postgres"

