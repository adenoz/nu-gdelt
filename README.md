# GDELT with Nushell

The Global Database of Events, Language and Tone ([GDELT](https://www.gdeltproject.org/data.html)) project claims to be largest, most comprehensive, and highest resolution open database of human society ever created. GDELT data goes back decades and continues to get updated every 15mins. The link provided is well worth exploring if you are not familiar with this fascinating dataset.

`nu-gdelt` grabs that new data each 15mins from the raw compressed CSV files, casts the data attributes to the correct types and saves the data into parquet files that are stored in monthly partitioned directories. The raw CSV files are stored in a Bronze partitioned directory, and the parquet files are stored in a Silver partitioned directory - adopting the data lake [medallion architecture](https://www.databricks.com/glossary/medallion-architecture).

`nu-gdelt` uses [nushell](https://www.nushell.sh/) scripts while harnessing [duckdb](https://duckdb.org/) for the data transformations.

`nu-gdelt` is intended to be initiated by a cron job every 15mins, such as in the following:

```shell
*/15 * * * * /full/file/path/filename.nu
```

Once the data builds up in your own personal data lake, you can query it using *duckdb* rather simply. For example, to read in one months of GDELT data:

```sql
SELECT * FROM read_parquet(silver/2024/09/*.parquet) LIMIT 5;
```

As your data grows, it stays organised and in a form that is efficient and easy to query.

This repo is under current development.


