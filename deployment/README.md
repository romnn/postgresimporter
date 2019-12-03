To deploy a local database and import dumped csv data files you can use this `docker-compose` 
configuration.

**Disclaimer**: This is not intended for production use!

Make sure to configure `postgresimporter.compose.yml` with your arguments.
Start and stop the `postgres` database, `pgadmin` and the `postgreloader` with
```bash
docker-compose -f postgres.compose.yml -f postgresimporter.compose.yml up --build
docker-compose -f postgres.compose.yml -f postgresimporter.compose.yml down
```

__Hint__: Omit `-f postgresimporter.compose.yml` to only start or stop the postgres database.