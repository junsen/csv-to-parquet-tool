# csv-to-parquet-tool
Convert data in csv to Parquet format file
- load csv data
- read schema for avro file
- construct avro data with csv data
- output to parquet file
```bash
mvn clean
mvn compile
mvn install
java -jar ./target/csv-to-parquet-tool-1.0-SNAPSHOT.jar <csv-data-path>
```


