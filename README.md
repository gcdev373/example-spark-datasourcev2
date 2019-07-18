<!---
 *
 * Copyright 2019 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
--->
# example-spark-datasourcev2
A very simple Java implementation of the Apache Spark DataSourceV2 API.

This example is compatible with **Spark 2.4.3.**

#Building
The jar file containing the DataSource is built with the following command
```text
$ mvn package
```

#Testing
The DataSource can be demonstrated from the pyspark shell.

Pyspark should be launched with the following command:
```text
$ pyspark --jars ./target/example-datasource-1.0.jar
```
You should see something like
```text
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.4.3
      /_/

Using Python version 3.7.3 (default, Jun 24 2019 04:54:02)
SparkSession available as 'spark'.
```
Then from within the pyspark shell, type the commands below:
```text
>>> df = spark.read.format('example.ExampleDataSource').load()
>>> df.show()
```
In order to display the data provided by the DataSource
```text
+-------+---+
|   name|age|
+-------+---+
|  Alfie| 24|
| Bertie| 36|
|Charlie| 48|
| Debbie| 60|
|  Ernie| 72|
|Frankie| 84|
| Gettie| 96|
+-------+---+

```
