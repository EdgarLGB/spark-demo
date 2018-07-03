# spark-demo
Spark batch application aiming to analyse the [NASH-HTTP](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) logs.

## Building project
$ sbt clean assembly

## Project list

* `MockCollector`: It collects the data and filters the bad-format data.
* `MockParser`: It reads the data from HDFS and parses it in json format.
* `MockWriter`: It reads the json format data and writes it into ElasticSearch.
* `MockAnalyser`: It reads the data from HDFS and leverages Spark SQL to extract some useful information.
