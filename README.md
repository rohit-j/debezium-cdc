# debezium-cdc
Using debezium embedded for Change Data Capture (CDC) to Target as a standalone runner.

### Installation
* To invoke a build and run tests: `mvn clean install`

### Running Debezium cdc
* `cd target`
* `cp ../runner.sh .`
* `./runner.sh ../scripts/mysql.properties ../scripts/iceberg.properties`