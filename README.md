# spark-demo
Some spark demo applications

## Building project
$sbt clean assembly

## Project list

* `MockCollector`: It collects the data and filters the bad-format data.
* `MockParser`: It reads the data from HDFS and parses it in json format.
* `MockWriter`: It reads the json format data and writes it into ElasticSearch.
* `MockAnalyser`: It reads the data from HDFS and leverages Spark SQL to extract some useful information.
