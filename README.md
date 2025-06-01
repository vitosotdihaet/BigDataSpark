# Запуск

```bash
docker compose up -d # start the containers
docker exec -it spark-master spark-submit \
--master spark://spark-master:7077 \
--deploy-mode client \
--jars /jdbc-drivers/postgresql-42.7.5.jar \
/apps/star.py # load into psql
docker exec -it spark-master spark-submit \
--master spark://spark-master:7077 \
--deploy-mode client \
--jars "/jdbc-drivers/postgresql-42.7.5.jar,/jdbc-drivers/clickhouse-jdbc-0.4.6.jar" \
--driver-class-path "/jdbc-drivers/postgresql-42.7.5.jar,/jdbc-drivers/clickhouse-jdbc-0.4.6.jar" \
/apps/clickhouse.py # load into clickhouse
./check_marts.sh
docker compose down
```
