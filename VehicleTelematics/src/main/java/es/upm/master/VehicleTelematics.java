package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
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

        KeyedStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple> keyedStream = s3.keyBy(1);
        
        
        // Dealing with the speed radar: cars with speed >= 90
        // store the Time[0], VID[1], XWay[3], Seg[6], Dir[5], Spd[2]
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> speedFilteredCars = s3.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f2 >= 20; 
            }
        });

        // SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> s5 = s4.reduce(new ReduceFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
        //     @Override
        //     public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> accumulator, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> input) throws Exception {
        //         return new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, input.f6, input.f7);
        //     }
        // });



        // SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> s5 = s4.reduce(new ReduceFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
        //     public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> accumulator, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> input) throws Exception {
        //         return new Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>(accumulator.f0, accumulator.f1, accumulator.f2, accumulator.f3, accumulator.f4, accumulator.f5, accumulator.f6, accumulator.f7);
        //     }
        // });

        // emit result
        if (params.has("output")) {
            speedFilteredCars.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            speedFilteredCars.print();
        }

        try {
            env.execute();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public class CarData extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
        public CarData() {
            super();
        }
    }

    public static class SimpleSum implements WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow> {
        public void apply(Tuple tuple, GlobalWindow countWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
            Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
            Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> first = iterator.next();
            Integer time;
            Integer vid;
            Integer spd;
            Integer xway;
            Integer dir;
            Integer seg;
            
            if(first!=null){
                time = first.f0;
                vid = first.f1;
                spd = first.f2;
                xway = first.f3;
                dir = first.f5;
                seg = first.f6;
            }
            // while(iterator.hasNext()){
            //     Tuple3<Long, String, Double> next = iterator.next();
            //     ts = next.f0;
            //     temp += next.f2;
            // }
            out.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(time, vid, spd, xway, dir, seg));
        }
    }
}