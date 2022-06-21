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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple19;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.triggers.*;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class SaturatedVendor {
    public static void main(String[] args){
        final ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStreamSource<String> inputStream = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> dailyAverages = inputStream.map(new MapFunction<String, Tuple4<Integer, Long, Long, Integer>>() {
			public Tuple4<Integer, Long, Long, Integer> map(String s) throws Exception {
				String[] data = s.split(",");
				return new Tuple4(
                    Integer.parseInt(data[0]), // VendorID
					dateToTimestamp(data[1]),  //pickup
					dateToTimestamp(data[2]),  //dropoff
					1
				);
			}}
		).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Integer, Long, Long, Integer>>() {
			@Override
			public long extractAscendingTimestamp(Tuple4<Integer, Long, Long, Integer> element) {
				return element.f1;
			}
		}).keyBy(0).window(SlidingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1000, TimeUnit.MILLISECONDS)))
		.trigger(CountTrigger.of(2))
		.apply(new WindowFunction<Tuple4<Integer, Long, Long, Integer>, Tuple4<Integer, String, String, Integer>,Tuple, TimeWindow>() {
			public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple4<Integer, Long, Long ,Integer>> input, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
				Iterator<Tuple4<Integer, Long, Long, Integer>> iterator = input.iterator();
				Tuple4<Integer, Long, Long ,Integer> first= iterator.next();
				Tuple4<Integer, Long, Long, Integer> next = first;
				Integer trips = first.f3;
				while(iterator.hasNext()){
					next = iterator.next();
					trips+=next.f3;
				}
				Long diff = next.f1 -first.f2;
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
				if(diff < 10*60*1000 /* && trips ==2 */){
					out.collect(new Tuple4(first.f0,df.format(first.f1), df.format(next.f2), trips));
				}
			}
		});



		/* countWindow(2,1).apply(new WindowFunction<Tuple4<Integer, Long, Long, Integer>, Tuple4<Integer, String, String, Integer>,Tuple, GlobalWindow>() {
			public void apply(Tuple key, GlobalWindow timeWindow, Iterable<Tuple4<Integer, Long, Long ,Integer>> input, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
				Iterator<Tuple4<Integer, Long, Long, Integer>> iterator = input.iterator();
				Tuple4<Integer, Long, Long ,Integer> first= iterator.next();
				Tuple4<Integer, Long, Long, Integer> next = first;
				Integer trips = first.f3;
				while(iterator.hasNext()){
					next = iterator.next();
					trips+=next.f3;
				}
				Long diff = next.f1 -first.f2;
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
				if(diff < 10*60*1000 && trips ==2){
					out.collect(new Tuple4(first.f0,df.format(first.f1), df.format(next.f2), trips));
				}
			}
		}); */

		/* SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> test2 = dailyAverages.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Integer, Long, Long, Integer, Long, Long>>() {
			@Override
			public long extractAscendingTimestamp(Tuple6<Integer, Long, Long, Integer, Long, Long> element) {
				return element.f1;
			}
		}).keyBy(0).window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple6<Integer, Long, Long, Integer, Long, Long>>(){
			@Override
			public long extract(Tuple6<Integer, Long, Long, Integer, Long, Long> v1){
				return v1.f5+(10*60*1000);
			}
		}
		))
		.apply(new WindowFunction<Tuple6<Integer, Long, Long, Integer, Long, Long>, Tuple4<Integer, String, String, Integer>, Tuple, TimeWindow>(){
			public void apply(Tuple key, TimeWindow timeWindow, Iterable<Tuple6<Integer, Long, Long, Integer, Long, Long>> input, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
				Iterator<Tuple6<Integer, Long, Long, Integer, Long, Long>> iterator = input.iterator();
				Tuple6<Integer, Long, Long, Integer, Long, Long> curr = iterator.next();
				
				Tuple6<Integer, Long, Long, Integer, Long, Long> first = curr;
				Tuple6<Integer, Long, Long, Integer, Long, Long> next = curr;
				Integer trips = 2; boolean aux =false;
				while(iterator.hasNext() &&!aux){
					next = iterator.next();
					if(){

					}
					else{

					}
					next = iterator.next();
					trips++;
				}
				DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
				TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
				out.collect(new Tuple4(first.f0,df.format(first.f1), df.format(next.f2), trips));
				
			}
		}); */
	
		
		
		//.countWindow(2,1).apply(new Testing());
		

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

    private static Long dateToTimestamp(String sDate) throws ParseException {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		return formatter.parse(sDate).getTime();
	}

	/* public static class Testing implements WindowFunction<Tuple5<Integer, Long, Long, Integer, Long>, Tuple4<Integer, String, String, Integer>, Integer, GlobalWindow>{
		@Override
		public void apply(Integer key, GlobalWindow timeWindow, Iterable<Tuple5<Integer, Long, Long, Integer, Long>> input, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
			Iterator<Tuple5<Integer, Long, Long, Integer, Long>> iterator = input.iterator();
			Tuple5<Integer, Long, Long, Integer, Long> curr = iterator.next();
			
			Tuple5<Integer, Long, Long, Integer, Long> first = curr;
			Tuple5<Integer, Long, Long, Integer, Long> next = curr;
			int trips = 1; boolean aa = false;
			while(iterator.hasNext() && !aa){
				next = iterator.next();
				if(((next.f1-curr.f2)*60)>=10){
					aa = true;
				}
				else{
					trips++;
					curr = next;
				}
			}
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			out.collect(new Tuple4(first.f0, df.format(first.f1), df.format(curr.f2), trips));
			
		}
	} */

	
}	