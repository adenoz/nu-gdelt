# GDELT with Nushell

The Global Database of Events, Language and Tone ([GDELT](https://www.gdeltproject.org/data.html)) project claims to be largest, most comprehensive, and highest resolution open database of human society ever created. GDELT data goes back decades and continues to get updated every 15mins. The link provided is well worth exploring if you are not familiar with this fascinating dataset.

`nu-gdelt` grabs that new data each 15mins from the raw compressed CSV files, casts the data attributes to the correct types and saves the data into parquet files that are stored in monthly partitioned directories. The raw CSV files are stored in a Bronze partitioned directory, and the parquet files are stored in a Silver partitioned directory - adopting the data lake [medallion architecture](https://www.databricks.com/glossary/medallion-architecture).

`nu-gdelt` uses [nushell](https://www.nushell.sh/) scripts while harnessing [duckdb](https://duckdb.org/) for the data transformations.

`nu-gdelt` is intended to be initiated by a cron job every 15mins, such as in the following:

```shell
*/15 * * * * /full/file/path/filename.nu
```

Once the data builds up in your own personal data lake, you can query it using *duckdb* rather simply. For example, to view the schema and details of the data, you could run:

```sql
duckdb
.mode line
DESCRIBE SELECT * FROM read_parquet('silver/2024/09/*.parquet');
```
Or, to see some aggregates of all of your data, you could run:

```shell
SUMMARIZE SELECT * FROM read_parquet('silver/2024/10/*.parquet');
```

As your data grows, it stays organised and in a form that is efficient and easy to query.

The final parquet files have the following schema. This schema closely matches that of the GDELT source, as per the [documentation](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf).

|--------------------------------------------|
|      column_name      | column_type | null |
|-----------------------|-------------|------|
| GlobalEventID         | INTEGER     | YES  |
| Day                   | INTEGER     | YES  |
| MonthYear             | INTEGER     | YES  |
| Year                  | INTEGER     | YES  |
| FractionDate          | FLOAT       | YES  |
| Actor1Code            | VARCHAR     | YES  |
| Actor1Name            | VARCHAR     | YES  |
| Actor1CountryCode     | VARCHAR     | YES  |
| Actor1KnownGroupCode  | VARCHAR     | YES  |
| Actor1EthnicCode      | VARCHAR     | YES  |
| Actor1Religion1Code   | VARCHAR     | YES  |
| Actor1Religion2Code   | VARCHAR     | YES  |
| Actor1Type1Code       | VARCHAR     | YES  |
| Actor1Type2Code       | VARCHAR     | YES  |
| Actor1Type3Code       | VARCHAR     | YES  |
| Actor2Code            | VARCHAR     | YES  |
| Actor2Name            | VARCHAR     | YES  |
| Actor2CountryCode     | VARCHAR     | YES  |
| Actor2KnownGroupCode  | VARCHAR     | YES  |
| Actor2EthnicCode      | VARCHAR     | YES  |
| Actor2Religion1Code   | VARCHAR     | YES  |
| Actor2Religion2Code   | VARCHAR     | YES  |
| Actor2Type1Code       | VARCHAR     | YES  |
| Actor2Type2Code       | VARCHAR     | YES  |
| Actor2Type3Code       | VARCHAR     | YES  |
| IsRootEvent           | INTEGER     | YES  |
| EventCode             | VARCHAR     | YES  |
| EventBaseCode         | VARCHAR     | YES  |
| EventRootCode         | VARCHAR     | YES  |
| QuadClass             | INTEGER     | YES  |
| GoldsteinScale        | FLOAT       | YES  |
| NumMentions           | INTEGER     | YES  |
| NumSources            | INTEGER     | YES  |
| NumArticles           | INTEGER     | YES  |
| AvgTone               | FLOAT       | YES  |
| Actor1Geo_Type        | INTEGER     | YES  |
| Actor1Geo_FullName    | VARCHAR     | YES  |
| Actor1Geo_CountryCode | VARCHAR     | YES  |
| Actor1Geo_ADM1Code    | VARCHAR     | YES  |
| Actor1Geo_ADM2Code    | VARCHAR     | YES  |
| Actor1Geo_Lat         | FLOAT       | YES  |
| Actor1Geo_Long        | FLOAT       | YES  |
| Actor1Geo_FeatureID   | VARCHAR     | YES  |
| Actor2Geo_Type        | INTEGER     | YES  |
| Actor2Geo_FullName    | VARCHAR     | YES  |
| Actor2Geo_CountryCode | VARCHAR     | YES  |
| Actor2Geo_ADM1Code    | VARCHAR     | YES  |
| Actor2Geo_ADM2Code    | VARCHAR     | YES  |
| Actor2Geo_Lat         | FLOAT       | YES  |
| Actor2Geo_Long        | FLOAT       | YES  |
| Actor2Geo_FeatureID   | VARCHAR     | YES  |
| ActionGeo_Type        | INTEGER     | YES  |
| ActionGeo_FullName    | VARCHAR     | YES  |
| ActionGeo_CountryCode | VARCHAR     | YES  |
| ActionGeo_ADM1Code    | VARCHAR     | YES  |
| ActionGeo_ADM2Code    | VARCHAR     | YES  |
| ActionGeo_Lat         | INTEGER     | YES  |
| ActionGeo_Long        | INTEGER     | YES  |
| ActionGeo_FeatureID   | VARCHAR     | YES  |
| DATEADDED             | BIGINT      | YES  |
| SOURCEURL             | VARCHAR     | YES  |
|--------------------------------------------|

## Logging

`nu-gdelt` also produces and saves logs into a custom log file called `gdelt.log`. The format for logs is simply `Datetime`, `Severtity` and `Message`. The log file has been formatted in such a way to make it easy to read. Additionally, because we are using `nushell`, we can very easily navigate and filter our logs using the following command:

```shell
open gdelt.log | lines | split column " - " | rename "Datetime" "Severity" "Message" | into value
```

This command will provide a table of log data that can be filtered and sorted, including by time as the `Datetime` data is read in as an actual date date type.

If you want to observe the logs as the program is running, you can run the following:

```shell
tail -f gdelt.log
```

`CTRL+C` to exit tailing.

Future iterations of the logging functionality will include auto-rotating of logs, archiving and compression etc.

--

This repo is under active development.


