Kite Foreign Data Wrapper for PostgreSQL
=========================================

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
Kite.

Installation
------------

To compile the Kite foreign data wrapper,

1. To build on POSIX-compliant systems you need to ensure the
   `pg_config` executable is in your path when you run `make`. This
   executable is typically in your PostgreSQL installation's `bin`
   directory. For example:

    ```
    $ export PATH=/usr/local/pgsql/bin/:$PATH
    ```

2. Compile the code using make.

    ```
    $ make USE_PGXS=1
    ```

3.  Finally install the foreign data wrapper.

    ```
    $ make USE_PGXS=1 install
    ```

Usage
-----

The following parameters can be set on a Kite foreign server object:

  * `host`: List of addresses or hostname:port of the Kite servers separated by comma ','.
  * `fetch_size`: This option specifies the number of rows kite_fdw should
    get in each fetch operation. It can be specified for a foreign table or
    a foreign server. The option specified on a table overrides an option
    specified for the server. The default is `100`.

The following parameters can be set on a Kite foreign table object:

  * `fragcnt`: The number of fragment per query. The default value is -1 (all segments).
  * `table_name`: Path of Kite table.
  * `fetch_size`: Same as `fetch_size` parameter for foreign server.
  * `fmt`: data format. csv and parquet are supported.
  * `csv_delimiter`: csv delimiter. Default is ','.
  * `csv_quote`: csv quote. Default is '"'.
  * `csv_escape`: csv escape character. Default is '"'.
  * `csv_header`: csv file with header line. Default is false.
  * `csv_nullstr`: csv NULL string. Default is empty string.

Examples
--------

```sql

SET timezone = 'PST8PDT';
-- optimizer MUST set to off in order to generate partial agg plan
SET optimizer = off;
SET optimizer_trace_fallback = on;
-- If gp_enable_minmax_optimization is on, it won't generate aggregate functions pushdown plan.
SET gp_enable_minmax_optimization = off;

-- load extension first time after install
CREATE EXTENSION kite_fdw;

-- create server object
CREATE SERVER kite_server
	FOREIGN DATA WRAPPER kite_fdw
	OPTIONS (host '127.0.0.1:7878', fragcnt '-1');

-- create user mapping
CREATE USER MAPPING FOR CURRENT_USER SERVER kite_server;

-- create foreign table
CREATE FOREIGN TABLE warehouse
	(
		warehouse_id int,
		warehouse_name text,
		warehouse_created timestamp
	)
	SERVER kite_server
	OPTIONS (table_name 'warehouse*', fmt 'csv', 
	csv_delimiter '|', csv_quote '"', csv_escape '"', csv_header 'false', csv_nullstr '',
	mpp_execute 'all segments');

-- select from table
SELECT * FROM warehouse ORDER BY 1;

warehouse_id | warehouse_name | warehouse_created
-------------+----------------+-------------------
           1 | UPS            | 10-JUL-20 00:00:00
           2 | TV             | 10-JUL-20 00:00:00
           3 | Table          | 10-JUL-20 00:00:00

