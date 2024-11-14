Clickhouse

```clickhouse
INSERT INTO measurements
SELECT *
FROM file('measurements.csv', 'CSV')
SETTINGS format_csv_delimiter = ';'
```

```clickhouse
CREATE TABLE measurements
(
    `city` String,
    `measurement` Float32,
)
ENGINE = MergeTree
ORDER BY city

```

10 mins to load the data to table!

```clickhouse
SELECT city,  min(measurement) AS min, round(avg(measurement), 1) AS avg, max(measurement) AS max 
FROM measurements 
GROUP BY city
ORDER BY city
```
~7 seconds
