Trying out [1 billion row challenge](https://github.com/gunnarmorling/1brc/tree/main) with a few different approaches.

### Specs
Indicative run times based on Lenovo Thinkpad

Intel i5-8250U CPU @ 1.60GHz, 8 cores  + 23 Gi RAM

### Rust
A fairly naive approach, relying on mmap and processing subsections of the file 
in parallel for speedup vs baseline (around 4 minutes with `Bufreader`, single thread).

Run time: ~22 seconds

### Polars

Use streaming mode to handle large input file.

Run time: ~35 seconds

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

Takes around 10 mins to load the data!

```clickhouse
SELECT city,  min(measurement) AS min, round(avg(measurement), 1) AS avg, max(measurement) AS max 
FROM measurements 
GROUP BY city
ORDER BY city
```

Once stored generating the city-wise aggregations takes around 7 seconds.