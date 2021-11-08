package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class VehicleTelematics {
    public static void main(String[] args) {
        
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> s2 = env.readTextFile(params.get("input"));

        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> s3 = 
        s2.flatMap(new FlatMapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            public void flatMap(String input, Collector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> collector) throws Exception {
                String[] dataArray = input.split("\n");
                for (String line : dataArray) {
                    String[] data = line.split(",");
                    Integer[] intData = new Integer[data.length];
                    for (int i = 0; i < data.length; i++) {
                        intData[i] = Integer.parseInt(data[i]);
                    }

                    collector.collect(new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(intData[0], intData[1], intData[2], intData[3], intData[4], intData[5], intData[6], intData[7] ));
                }
            }

        });
    }
}