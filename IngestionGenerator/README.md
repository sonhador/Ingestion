Ingestion Generator
=========
Uploads local file to Ingestion Gateway for saving into HDFS

Example Usage
----
java -jar target/IngestionGenerator-0.0.1-SNAPSHOT-jar-with-dependencies.jar ./data http://localhost:8080/IngestionGateway/ingest/hdfs/memory

Build
----
mvn clean package

Version
----
1.0

Credit
----
"MongJu Jung" <mjung@pivotal.io> 
