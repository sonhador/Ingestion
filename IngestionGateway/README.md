Ingestion Gateway
=========
Ingests into HDFS via HTTP POST interface & Egests from HDFS via HTTP GET interface.

Notables
----
saves into HDFS in multiple files, each in Block-size as specified in hdfs-site.xml

Example Urls
----
for ingest, http://localhost:8080/IngestionGateway/ingest/hdfs/memory
for egest,  http://localhost:8080/IngestionGateway/egest/hdfs/memory/20150225

Usage
----
for ingest, http://localhost:8080/IngestionGateway/ingest/hdfs/{some directory}
for egest,  http://localhost:8080/IngestionGateway/egest/hdfs/{some directory}/{some date pattern}

Build & Deploy
----
mvn clean tomcat:redeploy

Version
----
1.0

Credit
----
"MongJu Jung" <mjung@pivotal.io> 
