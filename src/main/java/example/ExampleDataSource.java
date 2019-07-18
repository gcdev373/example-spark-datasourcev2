/*
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
 */
package example;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.types.*;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class ExampleDataSource implements DataSourceV2, ReadSupport {
    public static final StructType Schema = new StructType(
            new StructField[]{
                    new StructField("name", DataTypes.StringType, false, new MetadataBuilder().build()),
                    new StructField("age", DataTypes.IntegerType, false, new MetadataBuilder().build())
            }
    );

    public DataSourceReader createReader(DataSourceOptions dataSourceOptions) {
        return new ExampleDataSourceReader();
    }
}

class ExamplePartitionReader implements InputPartitionReader<InternalRow> {
    private static class Person {
        private String name;
        private int age;
        public Person (String name, int age){
            this.name = name;
            this.age = age;
        }

        public int getAge() {return age;}
        public byte[] getNameBytes() {return name.getBytes(StandardCharsets.UTF_8);}
        public UTF8String getNameUnsafe() { return UTF8String.fromBytes(getNameBytes());}
    }

    private static Person[] data = new Person[]{
            new Person ("Alfie", 24),
            new Person ("Bertie", 36),
            new Person ("Charlie", 48),
            new Person ("Debbie", 60),
            new Person ("Ernie", 72),
            new Person ("Frankie", 84),
            new Person ("Gettie", 96),
    };

    private int index = 0;

    public boolean next() throws IOException {
        return index < data.length;
    }

    public InternalRow get() {
        GenericInternalRow genericInternalRow = new GenericInternalRow(new Object[]{data[index].getNameUnsafe(), data[index].getAge()});

        index++;

        return genericInternalRow;
    }

    public void close() throws IOException {

    }
}


class ExampleInputPartition implements InputPartition, Serializable {
    public InputPartitionReader createPartitionReader() {
        return new ExamplePartitionReader();
    }
}


//Could extend SupportsPushDownFilters and/or SupportsPushDownRequiredColumns
//and/or other subinterfaces of DataSourceReader
//See https://spark.apache.org/docs/2.4.3/api/java/index.html?org/apache/spark/sql/sources/v2/DataSourceV2.html
class ExampleDataSourceReader implements DataSourceReader {

    public StructType readSchema() {
        return ExampleDataSource.Schema;
    }

    public List<InputPartition<InternalRow>> planInputPartitions() {
        List<InputPartition<InternalRow>> partitions = new ArrayList();

        //Only one partition
        partitions.add(new ExampleInputPartition());
        return partitions;
    }
}
