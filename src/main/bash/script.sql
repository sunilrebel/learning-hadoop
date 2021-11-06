Huge optimization if your queries are only on certain partitions
CREATE TABLE customers ( name STRING,
address STRUCT<street:STRING, city:STRING, state:STRING, zip:INT> )
PARTITIONED BY (country STRING);
