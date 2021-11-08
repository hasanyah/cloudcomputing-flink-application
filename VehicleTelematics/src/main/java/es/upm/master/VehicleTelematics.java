package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

public class VehicleTelematics {
    public static void main(String[] args) {
        
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Find a solution to increase the size of the task manager
        /*
        
        final int parallelism = 1;
        final Configuration configuration = new Configuration();
        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(parallelism, configuration);
        */
        
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

        KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> s4 = s3.keyBy(0);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> s5 = s4.reduce(new ReduceFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> accumulator, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> input) throws Exception {
                return new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(accumulator.f0, accumulator.f1, accumulator.f2, accumulator.f3, accumulator.f4, accumulator.f5, accumulator.f6, accumulator.f7);
            }
        });

        // emit result
        if (params.has("output")) {
            s5.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            s5.print();
        }

        try {
            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}