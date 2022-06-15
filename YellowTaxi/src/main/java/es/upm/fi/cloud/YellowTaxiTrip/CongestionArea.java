package es.upm.fi.cloud.YellowTaxiTrip;

import java.util.Date;
import java.text.*;
import java.sql.Timestamp;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

public class CongestionArea {
	public static void main(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStreamSource<String> inputStream = env.readTextFile(params.get("input"));

		SingleOutputStreamOperator<Tuple3<Long, Integer, Double>> inputMap = inputStream.map(new MapFunction<String, Tuple4<Long, Integer, Double, Double>>() {
			public Tuple4<Long, Integer, Double, Double> map(String s) throws Exception {
				String[] data = s.split(",");
				return new Tuple4(
					dateToTimestamp(data[1]),
					1, 
					NumberUtils.toDouble(data[16]), // Total amount
					(data.length > 17 ? NumberUtils.toDouble(data[17]) : 0.0d) // Congestion surcharge
				);
			}
		}).filter(
			new FilterFunction<Tuple4<Long, Integer, Double, Double>>() {
            @Override
            public boolean filter(Tuple4<Long, Integer, Double, Double> in) throws Exception {
                return in.f3 > 0.0d; 
            }
        }).map(new MapFunction<
			Tuple4<Long, Integer, Double, Double>,
			Tuple3<Long, Integer, Double>>() {
				public Tuple3<Long, Integer, Double> map(Tuple4<Long, Integer, Double, Double> in) throws Exception {
					return new Tuple3<Long, Integer, Double>(in.f0, in.f1, in.f2);
				}
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, Integer, Double>>() {
			@Override
			public long extractAscendingTimestamp(Tuple3<Long, Integer, Double> element) {
				return element.f0;
			}
		}).windowAll(TumblingEventTimeWindows.of(Time.hours(1))
		).aggregate(new ProcessWindow());

		// Test output to see if we got the input correctly so far
		// SingleOutputStreamOperator<TaxiData> allData = inputMap.filter(new FilterFunction<TaxiData>() {
        //     @Override
        //     public boolean filter(TaxiData in) throws Exception {
        //         return true; 
        //     }
        // });

		// // Format the pu and do time/dates to keep only the dates, 1 (so that we sum it in the following step), totalAmount
		// SingleOutputStreamOperator<Tuple3<String, Integer, Double>> congestedData = allData.filter(new FilterFunction<TaxiData>() {
        //     @Override
        //     public boolean filter(TaxiData in) throws Exception {
        //         return in.f17 > 0.0d; 
        //     }
        // }).
		// map(new MapFunction<TaxiData, Tuple3<String, Integer, Double>>() {
        //     public Tuple3<String, Integer, Double> map(TaxiData in) throws Exception{
        //         return new Tuple3<String, Integer, Double>(
		// 			in.f1.split(" ")[0].replace('-', '/'),
		// 			1,
		// 			in.f16
		// 		);
        //     }
        // });

		// // Reduce trips to get the following format: DATE,NUMBER_OF_TRIPS,TOTAL_AMOUNT

		// SingleOutputStreamOperator<Tuple3<String, Integer, Double>> totalsByDate = 
		// congestedData
		// 	.keyBy(0)

		// 	.reduce(
		// 		new ReduceFunction<Tuple3<String, Integer, Double>>() {
		// 			public Tuple3<String, Integer, Double> reduce(Tuple3<String, Integer, Double> t1, Tuple3<String, Integer, Double> t2) throws Exception {
		// 				return new Tuple3<String, Integer, Double>(t1.f0, t1.f1+t2.f1, t1.f2+t2.f2);
		// 			}
		// 		}
    	// 	);

		if (params.has("output")) {
            inputMap.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            inputMap.print();
        }

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	private static long dateToTimestamp(String sDate) throws ParseException {
		return Timestamp.valueOf(sDate).getTime();
	}

	private static class ProcessWindow implements AggregateFunction<
		Tuple3<Long, Integer, Double>, 
		Tuple3<Long, Integer, Double>, 
		Tuple2<Integer, Double>> {
			@Override
			public Tuple3<Long, Integer, Double> createAccumulator() {
				return new Tuple3<Long, Integer, Double>(0L, 0, 0.0);
			}
			@Override
			public Tuple3<Long, Integer, Double> add(
				Tuple3<Long, Integer, Double> value, 
				Tuple3<Long, Integer, Double> acc) {
					return new Tuple3<Long, Integer, Double>(value.f0, value.f1+acc.f1, acc.f2+value.f2);
			}
			@Override
			public Tuple2<Integer, Double> getResult(Tuple3<Long, Integer, Double> acc) {
				return new Tuple2<>(acc.f1, acc.f2);
			}
			// @Override
			// public Tuple2<Integer, Double> getResult(Tuple3<Long, Integer, Double> acc) {
			// 		return acc.f1 / acc.f0;
			// }
			@Override
			public Tuple3<Long, Integer, Double> merge(
				Tuple3<Long, Integer, Double> acc, 
				Tuple3<Long, Integer, Double> acc1) {
					return new Tuple3<Long, Integer, Double>(acc.f0, acc.f1+acc1.f1, acc.f2+acc1.f2);
			}
 	}
}