package es.upm.master;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import java.util.Iterator;

public class VehicleTelematics {

    // TODO: Change the Speed limit to 90 when done
    static final Integer SPEED_LIMIT = 20;
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
        
        DataStreamSource<String> inputStream = env.readTextFile(params.get("input"));

        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
        // store the Time[0], VID[1], XWay[3], Seg[6], Dir[5], Spd[2]
        SingleOutputStreamOperator<CarData> speedFilteredCars = inputMap.filter(new FilterFunction<CarData>() {
            @Override
            public boolean filter(CarData in) throws Exception {
                return in.f2 >= SPEED_LIMIT; 
            }
        });

        // Remove the unnecessary columns
        SingleOutputStreamOperator<SpeedRadarData> speedRadarData = 
        speedFilteredCars.map(new MapFunction<CarData, SpeedRadarData>() {
            public SpeedRadarData map(CarData in) throws Exception{
                return new SpeedRadarData(in.f0, in.f1, in.f3, in.f6, in.f5, in.f2);
            }
        });

        int avgSpeed = 60;
        int segStart = 52;
        int segEnd = 56;

        SingleOutputStreamOperator<CarData> segmentFilteredCars = inputMap.filter(new FilterFunction<CarData>() {
            @Override
            public boolean filter(CarData in) throws Exception {
                return in.f6 >= segStart && in.f6 <= segEnd; 
            }
        });
        
        KeyedStream<CarData, Tuple> keyedStream = segmentFilteredCars.keyBy(1);
        SingleOutputStreamOperator<AverageSpeedData> sumSlidingCounteWindows =
                keyedStream.countWindow(2,1).apply(new AverageSpeedCalculator());

        SingleOutputStreamOperator<AverageSpeedData> speedAndSegmentFilteredCars = sumSlidingCounteWindows.filter(new FilterFunction<AverageSpeedData>() {
            @Override
            public boolean filter(AverageSpeedData in) throws Exception {
                return in.f5 >= avgSpeed; 
            }
        });

        // emit result
        if (params.has("outputfolder")) {
            speedRadarData.writeAsCsv(params.get("outputfolder")+"speedfines.csv", FileSystem.WriteMode.OVERWRITE);
            speedAndSegmentFilteredCars.writeAsCsv(params.get("outputfolder")+"avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE);
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

    public static class AverageSpeedCalculator implements WindowFunction<CarData, AverageSpeedData, Tuple, GlobalWindow> {
        public void apply(Tuple tuple, GlobalWindow countWindow, Iterable<CarData> input, Collector<AverageSpeedData> out) throws Exception {
            Iterator<CarData> iterator = input.iterator();
            CarData first = iterator.next();
            int time1 = 0;
            int time2 = 0;
            int vid = 0;
            int xway = 0;
            int dir = 0;
            int avgSpd = 0;
            int seg = 0;
            int pos = 0;
            boolean changeInDirection = false;
            boolean skippedSegment = false;
            boolean timeInconsistency = false;
            boolean lonely = true;
            if(first!=null){
                time1 = first.f0;
                vid = first.f1;
                xway = first.f3;
                dir = first.f5;
                avgSpd = first.f2;
                seg = first.f6;
                pos = first.f7;
            }
            int previousPosition = 0;
            while(iterator.hasNext()){
                CarData next = iterator.next();
                if (next.f5 != dir) {
                    changeInDirection = true;
                    break;
                }
                if (next.f6 == seg) {
                    if (next.f7 > previousPosition)
                        previousPosition = next.f7;
                    else
                        continue;
                }
                if (next.f6 - seg != 1) {
                    skippedSegment = true;
                }
                if (next.f0 <= time1) {
                    timeInconsistency = true;
                }

                time2 = next.f0;
                avgSpd = (next.f7 - pos) / (time2 - time1);
                lonely = false;
            }
            if (!changeInDirection && !skippedSegment && !timeInconsistency && !lonely) {
                out.collect(new AverageSpeedData(time1, time2, vid, xway, dir, avgSpd));
            }
        }
    }
}