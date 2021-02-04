## postgresimporter

[![Build Status](https://travis-ci.com/romnn/postgresimporter.svg?branch=master)](https://travis-ci.com/romnn/postgresimporter)
[![PyPI License](https://img.shields.io/pypi/l/postgresimporter)](https://pypi.org/project/postgresimporter/)
[![PyPI Version](https://img.shields.io/pypi/v/postgresimporter)](https://pypi.org/project/postgresimporter/)
[![PyPI Python versions](https://img.shields.io/pypi/pyversions/postgresimporter)](https://pypi.org/project/postgresimporter/)

This repository provides a python wrapper script based on [pgfutter](https://github.com/lukasmartinelli/pgfutter)
to load dumped csv data into a `postgres` database. It exposes customization hooks
and comes as a container or standalone script.

#### Installation
__Note__: If you want to use `docker`, skip installation and see below.
```bash
pip install postgresimporter # using pip
pipx install postgresimporter # using pipx
```

#### Usage
##### PIP
If you installed the python executable and already have a local postgres database running, run
```bash
postgresimporter \
    path/to/my/csv/files \
    --db-host=localhost \
    --db-port=5432 \
    --db-user=postgres \
    --db-password=example \
    --combine-tables \
    --exclude-regex="^.*sample.*$" \
    --post-load path/to/my/hooks/post-load.sql
```

##### Docker
The same command when using the `docker` container looks like this:
```bash
docker run \
    --network host \
    -v path/to/my/csv/files:/import \
    -v path/to/my/hooks/post-load.sql:/post-load.sql \
    -e DB_HOST=localhost \
    -e DB_PORT=5432 \
    -e DB_USER=postgres \
    -e DB_PASSWORD=example \
    romnn/postgresimporter \
    --post-load=/post-load.sql --combine-tables --exclude-regex="^.*sample.*$" /import
```
_Note_: When using `docker`, environment variables (`-e`) must be used in favor of command 
line arguments for specifying database connection parameters.

The tools will scan the `sources` directory you specify for any `.zip` files and unzip them.
Afterwards, it will scan for any `.csv` files and load them into a table named just like the 
file. Afterwards, it will try to combine any tables with the same prefix. 

#### Usage

See `--help` for __Configuration options__.

If you want to spawn a complete setup including the loader, a `postgres` database and
`pgadmin` as a postgres admin UI, you can use the provided `docker-compose` config:
```bash
docker-compose -f deployment/postgresimporter.compose.yml -f deployment/postgres.compose.yml up
docker-compose -f deployment/postgresimporter.compose.yml -f deployment/postgres.compose.yml down
```
To specify arguments for the `postgresimporter`, modify `deployment/postgresimporter.compose.yml`.

**Notice**: Before using the provided database container, make sure to stop any already running instances of postgres.
When using linux, do:
```
sudo /etc/init.d/postgresql stop
```

#### Hooks
The tool comes with some example hooks and the ability to add your own hooks scripts.
You might have a file `importdir/animals_1.csv` and `importdir/animals_2.csv` that looks like this:
```
name,origin,height
Grizzly,"North America",220
Giraffe,"Africa",600
Wallabie,"Australia",180
```
After importing `importdir/`, you will have three tables:


| Table                 | Content                                                   |
|:--------------------- |:----------------------------------------------------------|
| `import.animals`      | `importdir/animals_1` and `importdir/animals_2` combined  |
| `import.animals_1`    | All from `importdir/animals_1.csv`                        |
| `import.animals_2`    | All from `importdir/animals_2.csv`                        |

All of these tables will have the schema defined by the csv file.
However, all values will naturally be of type `text`.
With the `--post-load` you might want to execute a post load sql script that defines
a typed table and inserts the data like so:
```postgresql
CREATE TABLE public.animals (
    name VARCHAR(200) PRIMARY KEY,
    origin VARCHAR(200),
    height INTEGER
);

INSERT INTO public.animals
SELECT name, origin, height::int
FROM import.animals
```

#### Configuration options
| Option              | Description                   | Default | Required  |
| --------------------|:------------------------------|---------|----------:|
| `sources`           | List of csv files to load. Entries can either be directories or files. | None |yes |
| `--disable-unzip`   | Disables unzipping of any `*.zip` archives in the source directory | False | no |
| `--disable-import`  | Disables import of any `*.csv` files into the database | False | no |
| `--disable-check`   | Disables checking csv row count and database row count after import | False | no |
| `--combine-tables`  | Enabled combining of imported csv file tables into one table named by prefix (e.g. weather_1 & weather_2 -> weather) | False | no |
| `--exclude-regex`   | Files matching this regex will not be processed | None | no |
| `--pre-load`        | List of `*.sql` scripts to be executed before importing into the database (e.g. to clean the database). Entries can either be directories or files. | None | no |
| `--post-load`       | List of `*.sql` scripts to be executed after import (e.g. normalization). . Entries can either be directories or files. | None | no |
| `--all`             | Unzip and import all archives and zip files again | False | no |
| `--db-name`         | PostgreSQL database name | postgres | no |
| `--db-host`         | PostgreSQL database host | localhost | no |
| `--db-port`         | PostgreSQL database port | 5432 | no |
| `--db-user`         | PostgreSQL database user | postgres | no |
| `--db-password`     | PostgreSQL database password | None | no |
| `--log-level`       | Log level (DEBUG, INFO, WARNING, ERROR or FATAL) | INFO | no |

Note: You can also specify database connection settings via `DB_NAME`, `DB_HOST`, `DB_PORT`, `DB_USER` and `DB_PASSWORD` environment variables.

#### Local installation
Clone this repository and run (assuming you have `python` 3.5+ and 
[pgfutter](https://github.com/lukasmartinelli/pgfutter) installed):
```bash
pip install -r requirements.txt  # using pip
pipenv install --dev  # or using pipenv
```

#### Development
If you do not have `pipx` and `pipenv`, install with
```bash
python3 -m pip install --user pipx
python3 -m pipx ensurepath
pipx install pipenv
```

Install all dependencies with
```bash
pipenv install --dev
```

To format, sort imports and check PEP8 conformity, run
```bash
pipenv run black .
pipenv run isort
pipenv run flake8
```

These above checks are also configured as a git pre commit hook together with the test suite.
Before you commit, make sure to run `pre-commit run --all-files` to resolve any
errors in advance.

After merging new changes, a new version is deployed to [pypi.org](https://pypi.org) when the version is tagged
with `bump2version (patch|minor|major)`.

#### Testing
This project is not under active maintenance and not tested for production use.
However, a small test suite is provided and can be run with:
```bash
python -m postgresimporter.tests.run_tests
```
