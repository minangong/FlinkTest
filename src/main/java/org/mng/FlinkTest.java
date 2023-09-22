package org.mng;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTest {
    public static  void main(String[] args) throws Exception {
        // 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.Source
        CkJDBCSource ckJDBCSource = new CkJDBCSource("select * from dataflow.student ;");

        RowTypeInfo rowTypeInfo = getRowTypeInfo();
        DataStream<Row> rowDataStreamSource = env.addSource(ckJDBCSource,rowTypeInfo);

        //3. datastream输出
//        rowDataStreamSource.print("gua");

        // 4.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //5.创建一张表
//        Table rowDataTable = tableEnv.fromDataStream(rowDataStreamSource);
        tableEnv.createTemporaryView("sensor",rowDataStreamSource);
        String sql = "select id,name,age from sensor where age > 20";
        Table tableResult = tableEnv.sqlQuery(sql);


        // 8.输出
        tableEnv.toAppendStream(tableResult, Row.class).print("sql");



        env.execute("mng");

    }

    private static RowTypeInfo getRowTypeInfo() {
        TypeInformation[] types = new TypeInformation[3]; // 3个字段
        String[] fieldNames = new String[3];

        types[0] = BasicTypeInfo.INT_TYPE_INFO;
        types[1] = BasicTypeInfo.STRING_TYPE_INFO;
        types[2] = BasicTypeInfo.INT_TYPE_INFO;

        fieldNames[0] = "id";
        fieldNames[1] = "name";
        fieldNames[2] = "age";

        return new RowTypeInfo(types, fieldNames);
    }
}
