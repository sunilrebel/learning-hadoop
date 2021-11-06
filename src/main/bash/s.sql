  CREATE TABLE customers (
    name STRING,
    address STRUCT<street:STRING, city:STRING, state:STRING, zip:INT> )
  PARTITIONED BY (country STRING);


/path/to/table/data/customers/country=CA/
/path/to/table/data/customers/country=GB/


