package master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import java.util.Iterator;

public class VehicleTelematics {

    static final Integer SPEED_LIMIT = 90;
    public static void main(String[] args) {
        
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> inputStream = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<CarData> inputMap = inputStream.flatMap(new FlatMapFunction<String, CarData>() {
            public void flatMap(String input, Collector<CarData> collector) throws Exception {
                String[] dataArray = input.split("\n");
                for (String line : dataArray) {
                    String[] data = line.split(",");
                    Integer[] intData = new Integer[data.length];
                    for (int i = 0; i < data.length; i++) {
                        intData[i] = Integer.parseInt(data[i]);
                    }
                    collector.collect(new CarData(intData[0], intData[1], intData[2], intData[3], intData[4], intData[5], intData[6], intData[7]));
                }
            }
        });

        // Dealing with the speed radar: cars with speed >= 90
        SingleOutputStreamOperator<CarData> speedFilteredCars = inputMap.filter(new FilterFunction<CarData>() {
            @Override
            public boolean filter(CarData in) throws Exception {
                return in.f2 >= SPEED_LIMIT; 
            }
        });


        //////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////
        // The first report

        SingleOutputStreamOperator<SpeedRadarData> speedRadarData = 
        speedFilteredCars.assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<CarData>() {
                @Override
                public long extractAscendingTimestamp(CarData input) {
                    return input.f0*1000;
                }
            }
        ).map(new MapFunction<CarData, SpeedRadarData>() {
            public SpeedRadarData map(CarData in) throws Exception{
                return new SpeedRadarData(in.f0, in.f1, in.f3, in.f6, in.f5, in.f2);
            }
        });

        //////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////


        //////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////
        // 2nd report

        int segStart = 52;
        int segEnd = 56;

        SingleOutputStreamOperator<CarData> segmentFilteredCars = inputMap.filter(new FilterFunction<CarData>() {
            @Override
            public boolean filter(CarData in) throws Exception {
                return in.f6 == segStart || in.f6 == segEnd; 
            }
        });
        
        KeyedStream<CarData, DirCarKeySegStructure> carsKeyedByIdDirSeg = 
            segmentFilteredCars.keyBy(
                new KeySelector<CarData, DirCarKeySegStructure>() {

                @Override
                public DirCarKeySegStructure getKey(CarData value) throws Exception {
                    return new DirCarKeySegStructure(value.f1, value.f5, value.f6);
                }
            }
        );
        
        SingleOutputStreamOperator<CarData> carsReducedForAvgSpeedCalc = carsKeyedByIdDirSeg.reduce(
            new ReduceFunction<CarData>() {
                public CarData reduce(CarData value1, CarData value2) throws Exception {
                    if (value1.f5 == 0) {
                        if (value2.f7 > value1.f7) {
                            return value2;
                        } else { 
                            return value1;
                        }
                    } else {
                        if (value2.f7 > value1.f7) {
                            return value1;
                        } else { 
                            return value2;
                        }
                    }
                }
            }
        );
        
        KeyedStream<CarData, DirCarKeyStructure> customKeyedCars = carsReducedForAvgSpeedCalc.assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<CarData>() {
                @Override
                public long extractAscendingTimestamp(CarData input) {
                    return input.f0*1000;
                }
            }
        ).keyBy(
                new KeySelector<CarData, DirCarKeyStructure>() {

                @Override
                public DirCarKeyStructure getKey(CarData value) throws Exception {
                    return new DirCarKeyStructure(value.f1, value.f5);
                }
            }
        );

        SingleOutputStreamOperator<AverageSpeedData> avgSpeedRadarSumSlidingCounteWindows =
                customKeyedCars.countWindow(2,1).apply(new AverageSpeedCalculator());

        //////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////


        //////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////
        // 3rd report

        KeyedStream<CarData, Tuple> keyedStreamFullData = inputMap.assignTimestampsAndWatermarks(
            new AscendingTimestampExtractor<CarData>() {
                @Override
                public long extractAscendingTimestamp(CarData input) {
                    return input.f0*1000;
                }
            }
        ).keyBy(1);

        SingleOutputStreamOperator<AccidentData> accidentReportSumSlidingCounteWindows =
                keyedStreamFullData.countWindow(4,1).apply(new AccidentReporter());

        //////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////

        // emit result
        if (params.has("outputfolder")) {
            speedRadarData.writeAsCsv(params.get("outputfolder")+"speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            avgSpeedRadarSumSlidingCounteWindows.writeAsCsv(params.get("outputfolder")+"avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
            accidentReportSumSlidingCounteWindows.writeAsCsv(params.get("outputfolder")+"accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            speedRadarData.print();
        }

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class CarData extends Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
        public CarData() {
            super();
        }

        public CarData(Integer time, Integer vid, Integer spd, Integer xway, Integer lane, Integer dir, Integer seg, Integer pos) {
            super(time, vid, spd, xway, lane, dir, seg, pos);
        }
    }

    public static class SpeedRadarData extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {
        public SpeedRadarData() {
            super();
        }

        public SpeedRadarData(Integer time, Integer vid, Integer xway, Integer seg, Integer dir, Integer spd) {
            super(time, vid, xway, seg, dir, spd);
        }
    }

    public static class AverageSpeedData extends Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> {
        public AverageSpeedData() {
            super();
        }

        public AverageSpeedData(Integer time1, Integer time2, Integer vid, Integer xway, Integer dir, Integer avgSpd) {
            super(time1, time2, vid, xway, dir, avgSpd);
        }
    }

    public static class AccidentData extends Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> {
        public AccidentData() {
            super();
        }

        public AccidentData(Integer time1, Integer time2, Integer vid, Integer xway, Integer seg, Integer dir, Integer pos) {
            super(time1, time2, vid, xway, seg, dir, pos);
        }
    }

    public static class DirCarKeySegStructure extends Tuple3<Integer, Integer, Integer> {
        public DirCarKeySegStructure() {
            super();
        }

        public DirCarKeySegStructure(Integer vid, Integer dir, Integer seg) {
            super(vid, dir, seg);
        }
    }

    public static class DirCarKeyStructure extends Tuple2<Integer, Integer> {
        public DirCarKeyStructure() {
            super();
        }

        public DirCarKeyStructure(Integer vid, Integer dir) {
            super(vid, dir);
        }
    }

    public static class AverageSpeedCalculator implements WindowFunction<CarData, AverageSpeedData, DirCarKeyStructure, GlobalWindow> {
        public void apply(DirCarKeyStructure key, GlobalWindow timeWindow, Iterable<CarData> input, Collector<AverageSpeedData> out) throws Exception {
            Iterator<CarData> iterator = input.iterator();
            CarData first = iterator.next();
            int time1 = 0;
            int time2 = 0;
            int vid = 0;
            int xway = 0;
            int dir = 0;
            int speed = 0;
            int seg = 0;
            int pos = 0;
            if(first!=null){
                time1 = first.f0;
                vid = first.f1;
                xway = first.f3;
                dir = first.f5;
                speed = first.f2;
                seg = first.f6;
                pos = first.f7;
            }
        
            CarData next = first;
            while(iterator.hasNext()){
                next = iterator.next();
            }
            if (first.f6 == 52 && next.f6 == 56) {
                time2 = next.f0;
                speed = (int)(((next.f7 - first.f7) / (time2 - time1)) * 60 * 60 / 1000 * 0.621371);
                if (speed > 60)
                    out.collect(new AverageSpeedData(time1, time2, vid, xway, dir, speed));
            }
        }
    }

    public static class AccidentReporter implements WindowFunction<CarData, AccidentData, Tuple, GlobalWindow> {
        public void apply(Tuple key, GlobalWindow countWindow, Iterable<CarData> input, Collector<AccidentData> out) throws Exception {
            Iterator<CarData> iterator = input.iterator();
            CarData first = iterator.next();
            int time1 = 0;
            int time2 = 0;
            int vid = 0;
            int xway = 0;
            int seg = 0;
            int dir = 0;
            int pos = 0;
            int counter = 1;

            if(first!=null){
                time1 = first.f0;
                vid = first.f1;
                xway = first.f3;
                seg = first.f6;
                dir = first.f5;
                pos = first.f7;
            }
            while(iterator.hasNext()){
                CarData next = iterator.next();
                time2 = next.f0;

                if (next.f7 == pos) {
                    counter++;        
                } else {
                    counter = 1;
                    time1 = next.f0;
                    vid = next.f1;
                    xway = next.f3;
                    seg = next.f6;
                    dir = next.f5;
                    pos = next.f7;
                }
                
                if (counter >= 4)
                    out.collect(new AccidentData(time1, time2, vid, xway, seg, dir, pos));
            }
        }
    }
}