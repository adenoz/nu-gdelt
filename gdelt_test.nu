#!/usr/local/bin
#
# GDELT downloading of latest 15mins data
#
# This script will download latest GDELT data
# and save a zipped CSV in a Bronze directory
# and a named and typed parquet file in a Silver directory
# It expects `duckdb` to be installed, which it should be!
#
# Designed to be run with cron job every 15mins
# Source data is updated every 15mins
#
# Example:
#
# */15 * * * * /full/file/path/filename.nu
#
# Then DuckDB SQL, read in one month of GDELT data:
#
# SELECT * FROM read_parquet(silver/2024/10/*.parquet) LIMIT 5;
#
#=============================================================

def logger [importance message] {
  let date = date now | format date "%Y-%m-%d %H:%M:%S"
  let logger = $"($date) - ($importance) - ($message)\n"
  $logger | save --append gdelt.log
}

# logger "INFO" "Testing logging"

let TIME_START = date now
let URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# This function extracts the unique identifier 
# from the GDELT lastupdate .txt file. The exports file.
def get_identifier [url] {
  # logger "INFO" "Starting to grab unique identifer"
  let status = http get -fe $url | get status
  match $status {
    200 => (# logger "INFO" "Got 200 code! Grabbing identifier" |
           http get $url |
           lines |
           first |
           split row " " |
           last |
           split words |
           first 6 |
           last)
    3.. => (logger "WARNING " "Got a 3** status code attempting to get identifier, trying again in 1min" |
            sleep 1min | get_identifier $url)
    4.. => (logger "WARNING" "Got a 4** status code attempting to get identifier, trying again in 1min" |
            sleep 1min | get_identifier $url)
    5.. => (logger "WARNING" "Got a 5** status code attempting to get identifier, trying again in 1min" |
            sleep 1min | get_identifier $url)  
    _ => (logger "WARNING" "Got a weird response for a status code attempting to get identifier,
          trying again in 1min" | sleep 1min | get_identifier $url)
  }
}

def get_year [url] {
  let index = get_identifier $url
  let year = $index | into string | str substring 0..3
  return $year
}

def get_month [url] {
  let index = get_identifier $url
  let month = $index | into string | str substring 4..5
  return $month
}

# Capture year and month from file name, and mkdirs
def set_dirs [url] {
  logger "INFO" "Setting directories"
  let year = get_year $url
  let month = get_month $url
  if (is-not-empty $"bronze/($year)") == false {mkdir $"bronze/($year)"}
  if (is-not-empty $"bronze/($year)/($month)") == false {mkdir $"bronze/($year)/($month)"} 
  if (is-not-empty $"silver/($year)") == false {mkdir $"silver/($year)"}
  if (is-not-empty $"silver/($year)/($month)") == false {mkdir $"silver/($year)/($month)"} 
  # if (is-not-empty $"gold/($year)") == false {mkdir $"gold/($year)"}
  # if (is-not-empty $"gold/($year)/($month)") == false {mkdir $"gold/($year)/($month)"} 
  logger "INFO" "Directories all set"
}

# Fetch http status code from a given URL, then request download if 200
def get_data [url]: any -> int {
  mut fetch_attempts = 0
  let status = http get -fe $url | get status
  match $status {
    200 => (logger "INFO" "Got a good 200 status code!" | downloader $url)
    3.. => (logger "WARNING" "Got a 3** status code, will try again in 1min" |
            sleep 1min | get_data $url)
    4.. => (logger "WARNING" "Got a 4** status code, will try again in 1min" |
            sleep 1min | get_data $url)
    5.. => (logger "WARNING" "Got a 5** status code, will try again in 1min" |
            sleep 1min | get_data $url)    
    _ => (logger "WARNING" "Got a weird http response code, will try again in 1min" |
          sleep 1min | get_date $url)
  }
}

# This function downloads the most recent gdelt data
# and saves it in bronze storage as zipped csv, as per source
def downloader [url] {
  logger "INFO" "Starting download attempt..."
  try {
    let identifier = get_identifier $url
    let year = get_year $url
    let month = get_month $url
    http get (http get $url |
    lines |
    first |
    split row " " |
    last) |
    save $"bronze/($year)/($month)/($identifier).csv.zip" -f |
    logger "INFO" "Downloading of the data seems complete!"
    } catch { logger "WARNING" "Failed to download zipped csv, will try again in 1min" |
      sleep 1min | downloader $url}
}

def unzipper [url] {
  logger "INFO" "Starting to try to unzip"
  let identifier = get_identifier $url
  let year = get_year $url
  let month = get_month $url
  unzip $"bronze/($year)/($month)/($identifier).csv.zip" -d temp_data/ |
  sleep 2sec | # this is to allow time for unzipping
  (logger "INFO" "Compressed data has been grabbed and unzipped.")
}

# Convert CSV to Parquet format
# Rename columns and set data types
def duck_parquet [url] {
  logger "INFO" "Starting to get ducky with it."
  try {
    duckdb -c "COPY (SELECT * FROM read_csv('temp_data/*.export.CSV', columns={
     'GlobalEventID': 'INTEGER',
     'Day': 'INTEGER',
     'MonthYear': 'INTEGER',
     'Year': 'INTEGER',
     'FractionDate': 'FLOAT',
     'Actor1Code': 'VARCHAR',
     'Actor1Name': 'VARCHAR',
     'Actor1CountryCode': 'VARCHAR',
     'Actor1KnownGroupCode': 'VARCHAR',
     'Actor1EthnicCode': 'VARCHAR',
     'Actor1Religion1Code': 'VARCHAR',
     'Actor1Religion2Code': 'VARCHAR',
     'Actor1Type1Code': 'VARCHAR',
     'Actor1Type2Code': 'VARCHAR',
     'Actor1Type3Code': 'VARCHAR',
     'Actor2Code': 'VARCHAR',
     'Actor2Name': 'VARCHAR',
     'Actor2CountryCode': 'VARCHAR',
     'Actor2KnownGroupCode': 'VARCHAR',
     'Actor2EthnicCode': 'VARCHAR',
     'Actor2Religion1Code': 'VARCHAR',
     'Actor2Religion2Code': 'VARCHAR',
     'Actor2Type1Code': 'VARCHAR',
     'Actor2Type2Code': 'VARCHAR',
     'Actor2Type3Code': 'VARCHAR',
     'IsRootEvent': 'INTEGER',
     'EventCode': 'VARCHAR',
     'EventBaseCode': 'VARCHAR',
     'EventRootCode': 'VARCHAR',
     'QuadClass': 'INTEGER',
     'GoldsteinScale': 'FLOAT',
     'NumMentions': 'INTEGER',
     'NumSources': 'INTEGER',
     'NumArticles': 'INTEGER',
     'AvgTone': 'FLOAT',
     'Actor1Geo_Type': 'INTEGER',
     'Actor1Geo_FullName': 'VARCHAR',
     'Actor1Geo_CountryCode': 'VARCHAR',
     'Actor1Geo_ADM1Code': 'VARCHAR',
     'Actor1Geo_ADM2Code': 'VARCHAR',
     'Actor1Geo_Lat': 'FLOAT',
     'Actor1Geo_Long': 'FLOAT',
     'Actor1Geo_FeatureID': 'VARCHAR',
     'Actor2Geo_Type': 'INTEGER',
     'Actor2Geo_FullName': 'VARCHAR',
     'Actor2Geo_CountryCode': 'VARCHAR',
     'Actor2Geo_ADM1Code': 'VARCHAR',
     'Actor2Geo_ADM2Code': 'VARCHAR',
     'Actor2Geo_Lat': 'FLOAT',
     'Actor2Geo_Long': 'FLOAT',
     'Actor2Geo_FeatureID': 'VARCHAR',
     'ActionGeo_Type': 'INTEGER',
     'ActionGeo_FullName': 'VARCHAR',
     'ActionGeo_CountryCode': 'VARCHAR',
     'ActionGeo_ADM1Code': 'VARCHAR',
     'ActionGeo_ADM2Code': 'VARCHAR',
     'ActionGeo_Lat': 'INTEGER',
     'ActionGeo_Long': 'INTEGER',
     'ActionGeo_FeatureID': 'VARCHAR',
     'DATEADDED': 'BIGINT',
     'SOURCEURL': 'VARCHAR'
    })) TO 'temp_data/temp.parquet' (FORMAT 'parquet');"
    logger "INFO" "Data appears to have been successfully saved in temp loc, validating..."
  } catch { |err| $err.msg | logger "ERROR" "Failed to cast and save to parquet!"}
}

# Determine if a parquet file was saved where expected
# This won't actually confirm if THIS file has been saved as parquet...
# only that there is at least one parquet file there. need to improve
def valid_parquet [url] {
  let pval = ls temp_data/ | get name | find parquet
  let ptest = $pval | is-not-empty
  match $ptest {
    true => (logger "INFO" "Parquet successfully saved in temp directory.")
    false => (logger "WARNING" "Data was NOT saved as temp Parquet file, trying again..." | duck_parquet $url)
    _ => (logger "ERROR" "Something weird happened! Couldn't understand if temp Parquet file saved.")
  }
}

# Move and rename parquet to appropriate directory in monthly silver partition
def move_rename_parquet [url] {
  logger "INFO" "Moving and renaming parquet"
  try {
    let year = get_year $url
    let month = get_month $url
    let index = get_identifier $url
    mv temp_data/temp.parquet $"silver/($year)/($month)/($index).parquet"
  } catch { logger "ERROR" "Transerring parquet to partitioned dir failed" }
}

# Checking if parquet can be found where it is supposed to be
def confirm_parquet [url] {
  let year = get_year $url
  let month = get_month $url
  let index = get_identifier $url
  let confirm = ls $"silver/($year)/($month)" | find $"($index).parquet" | is-not-empty
  match $confirm {
    true => (logger "INFO" "Parquet file successfully saved in expected Silver dir!" |
            rm $"temp_data/($index).export.CSV") 
    false => (logger "ERROR" "Parquet file NOT detected in correct final dir!")
  }
}

# Finishing up by printing time taken
def finish_up [] {
  let time_finished = date now
  let duration = $time_finished - $TIME_START
  let secs = $duration | into string | split row " " | first
  logger "INFO" $"Task took ($secs) seconds, and is now complete."
}

def main [] {
  logger "INFO" "Starting to grab most recent GDELT data..."
  set_dirs $URL
  get_data $URL
  unzipper $URL
  duck_parquet $URL
  valid_parquet $URL
  move_rename_parquet $URL
  confirm_parquet $URL
  finish_up
}
