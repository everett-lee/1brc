Trying out [1 billion row challenge](https://github.com/gunnarmorling/1brc/tree/main) with a few different approaches.

### Rust
A fairly naive approach, relying on mmap and processing subsections of the file 
in parallel for speedup vs baseline.

### Polars

### Clickhouse
What if the entire file is dumped to an OLAP store?

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

This takes around 10 mins to load the data!

```clickhouse
SELECT city,  min(measurement) AS min, round(avg(measurement), 1) AS avg, max(measurement) AS max 
FROM measurements 
GROUP BY city
ORDER BY city
```

But once stored generating the city-wise aggregations takes around 7 seconds.