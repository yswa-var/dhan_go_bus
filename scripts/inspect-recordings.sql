SELECT
    count(*) AS rows,
    count(DISTINCT (source_topic, source_partition, source_offset)) AS source_coordinates,
    min(received_at_ns_utc) AS first_received_at_ns_utc,
    max(received_at_ns_utc) AS last_received_at_ns_utc
FROM read_parquet('recordings/**/*.parquet', hive_partitioning = false);

SELECT
    source_topic,
    source_partition,
    min(source_offset) AS first_offset,
    max(source_offset) AS last_offset,
    count(*) AS rows
FROM read_parquet('recordings/**/*.parquet', hive_partitioning = false)
GROUP BY source_topic, source_partition
ORDER BY source_topic, source_partition;

SELECT source_topic, source_partition, source_offset, count(*) AS copies
FROM read_parquet('recordings/**/*.parquet', hive_partitioning = false)
GROUP BY source_topic, source_partition, source_offset
HAVING count(*) > 1
ORDER BY copies DESC, source_topic, source_partition, source_offset;

SELECT packet_type, octet_length(raw_packet) AS raw_bytes, count(*) AS rows
FROM read_parquet('recordings/**/*.parquet', hive_partitioning = false)
GROUP BY packet_type, raw_bytes
ORDER BY packet_type, raw_bytes;

