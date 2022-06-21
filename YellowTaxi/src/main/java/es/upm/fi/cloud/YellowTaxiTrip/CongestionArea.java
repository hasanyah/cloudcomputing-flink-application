package es.upm.fi.cloud.YellowTaxiTrip;

import java.util.Date;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.sql.Timestamp;
import java.text.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateUtils;
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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


public class CongestionArea {
	public static void main(String[] args) {
		final ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStreamSource<String> inputStream = env.readTextFile(params.get("input"));

		SingleOutputStreamOperator<Tuple3<String, Integer, Double>> dailyAverages = inputStream.map(new MapFunction<String, Tuple4<Long, Integer, Double, Double>>() {
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
		}).windowAll(TumblingEventTimeWindows.of(Time.days(1))
		).apply(new CongestionAreaFunction());

		if (params.has("output")) {
            dailyAverages.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            dailyAverages.print();
        }

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
	}

	private static long dateToTimestamp(String sDate) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		TimeZone.setDefault(TimeZone.getTimeZone("GMT+2"));
		return formatter.parse(sDate).getTime();
	}

	private static double round(double value, int places) {
		if (places < 0) throw new IllegalArgumentException();

		BigDecimal bd = new BigDecimal(Double.toString(value));
		bd = bd.setScale(places, RoundingMode.HALF_UP);
		return bd.doubleValue();
	}

	private static class ProcessWindow implements AggregateFunction<
		Tuple3<Long, Integer, Double>, 
		Tuple3<Long, Integer, Double>, 
		Tuple3<String, Integer, Double>> {
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
			public Tuple3<String, Integer, Double> getResult(Tuple3<Long, Integer, Double> acc) {
				DateFormat df = new SimpleDateFormat("yyyy/MM/dd");
				return new Tuple3<>(df.format(acc.f0), acc.f1, round(acc.f2/acc.f1, 2));
			}
			@Override
			public Tuple3<Long, Integer, Double> merge(
				Tuple3<Long, Integer, Double> acc, 
				Tuple3<Long, Integer, Double> acc1) {
					return new Tuple3<Long, Integer, Double>(acc.f0, acc.f1+acc1.f1, acc.f2+acc1.f2);
			}
 	}

	public static class CongestionAreaFunction implements AllWindowFunction<Tuple3<Long, Integer, Double>, Tuple3<String, Integer, Double>, TimeWindow> {
		@Override
		public void apply(TimeWindow window, Iterable<Tuple3<Long, Integer, Double>> input, Collector<Tuple3<String, Integer, Double>> output) throws Exception {
			double costAvg = 0;
			int numberOfTrips = 0;
			long firstElementTime = input.iterator().next().f0;
			for (Tuple3<Long, Integer, Double> entry : input) {
				costAvg += entry.f2;
				numberOfTrips++;
			}
			if (numberOfTrips == 0) {
				throw new NullPointerException("No taxi report in this window");
			}
			costAvg /= numberOfTrips;
		
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy/MM/dd");
			String realDateStr = formatter.format(firstElementTime);
			
			output.collect(new Tuple3<String, Integer, Double>(
				realDateStr, 
				numberOfTrips, 
				round(costAvg, 2)
			));
		}
	}
}