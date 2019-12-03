CREATE OR REPLACE FUNCTION strip(text) RETURNS TEXT
    AS $$ SELECT NULLIF(regexp_replace($1, E'(^[\\n\\r]+)|(")|([\\n\\r]+$)', '', 'g'), '') $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION has_column(text, text, text) RETURNS BOOLEAN
    AS $$ SELECT EXISTS (SELECT 1
			FROM information_schema.columns
			WHERE table_schema=$1 AND table_name=$2 AND column_name=$3); $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

/*
CREATE FUNCTION parse_timestamp_with_offset(text) RETURNS TIMESTAMP WITH TIME ZONE
    /* "31-JAN-19 03.20.00.000000000 PM +01:00" */
    AS $$ SELECT to_timestamp($1, 'FXDD-MON-YY HH12.MI.SS.          PM TZH:TZM') $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
 */

CREATE OR REPLACE FUNCTION parse_timestamp_with_tz(text, text, text) RETURNS TIMESTAMP WITH TIME ZONE
    /* "28-MAR-19 05.02.10.000000000 AM GMT" */
    AS $$ SELECT to_timestamp($1, $2) AT TIME ZONE $3 AT TIME ZONE (select current_setting('timezone'))$$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION parse_timezone(text) RETURNS text
    /* "28-MAR-19 05.02.10.000000000 AM GMT" */
    AS $$ SELECT (regexp_matches($1, '\d\d-\w\w\w-\d\d \d\d.\d\d.\d\d.?\d{0,9} (?:AM|PM) (\w\w\w)'))[ 1 ] $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION parse_timestamp(text) RETURNS TIMESTAMP WITH TIME ZONE
    /*      "31-JAN-19 03.20.00.000000000 PM +01:00"    */
    /* or   "31-JAN-19 03.20.00 PM +01:00"              */
    /* or   "28-MAR-19 05.02.10.000000000 AM GMT"       */
    /* or   "28-MAR-19 05.02.10 AM GMT"                 */
    /* or   "20190101013449+0000"                       */
    AS $$ SELECT CASE
            WHEN $1 ~ '\d\d-\w\w\w-\d\d \d\d.\d\d.\d\d.\d\d\d\d\d\d\d\d\d (?:AM|PM) (\+|-)\d\d:\d\d'
                THEN to_timestamp($1, 'FXDD-MON-YY HH12.MI.SS.          PM TZH:TZM')
            WHEN $1 ~ '\d\d-\w\w\w-\d\d \d\d.\d\d.\d\d (?:AM|PM) (\+|-)\d\d:\d\d'
                THEN to_timestamp($1, 'FXDD-MON-YY HH12.MI.SS PM TZH:TZM')
            WHEN $1 ~ '\d\d-\w\w\w-\d\d \d\d.\d\d.\d\d.\d\d\d\d\d\d\d\d\d (?:AM|PM) (\w\w\w)'
                THEN parse_timestamp_with_tz($1, 'FXDD-MON-YY HH12.MI.SS.          PM', parse_timezone($1))
            WHEN $1 ~ '\d\d-\w\w\w-\d\d \d\d.\d\d.\d\d (?:AM|PM) (\w\w\w)'
                THEN parse_timestamp_with_tz($1, 'FXDD-MON-YY HH12.MI.SS PM', parse_timezone($1))
            WHEN $1 ~ '\d\d\d\d\d\d\d\d\d\d\d\d\d\d(\+|-)\d\d\d\d'
                THEN to_timestamp($1, 'YYYYMMDDHH24MISS TZHTZM')
            END $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;

CREATE OR REPLACE FUNCTION parse_date(text) RETURNS DATE
    /* "01-FEB-19" */
    AS $$ SELECT to_date($1, 'FXDD-MON-YY') $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;