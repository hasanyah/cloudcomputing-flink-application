package es.upm.fi.cloud.YellowTaxiTrip;

import java.util.TimeZone;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Iterator;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.*;
import org.apache.flink.streaming.api.windowing.evictors.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

public class SaturatedVendor {
    public static void main(String[] args){
        final ParameterTool params = ParameterTool.fromArgs(args);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		DataStreamSource<String> inputStream = env.readTextFile(params.get("input"));

        SingleOutputStreamOperator<Tuple4<Integer, String, String, Integer>> saturatedVendors = inputStream.map(new MapFunction<String, Tuple4<Integer, Long, Long, Integer>>() {
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
		}).keyBy(0)
		.window(GlobalWindows.create())
		.trigger(CustomCountTrigger.of(2))
		.evictor(new Evictor<Tuple4<Integer, Long, Long, Integer>, GlobalWindow>() {
			@Override
            public void evictBefore(Iterable<TimestampedValue<Tuple4<Integer, Long, Long, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            }
			
			@Override
            public void evictAfter(Iterable<TimestampedValue<Tuple4<Integer, Long, Long, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
				Iterator<TimestampedValue<Tuple4<Integer, Long, Long, Integer>>> iterator = elements.iterator();
				if (iterator.hasNext()) {
					iterator.next();
					iterator.remove();
				}
            }
        })
		.apply(new WindowFunction<
			Tuple4<Integer, Long, Long, Integer>, 
			Tuple4<Integer, String, String, Integer>,
			Tuple, 
			GlobalWindow>() {
				public void apply(Tuple key, GlobalWindow timeWindow, Iterable<Tuple4<Integer, Long, Long ,Integer>> input, Collector<Tuple4<Integer, String, String, Integer>> out) throws Exception {
					Iterator<Tuple4<Integer, Long, Long, Integer>> iterator = input.iterator();
					Tuple4<Integer, Long, Long ,Integer> first = iterator.next();
					Tuple4<Integer, Long, Long, Integer> next;

					DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
					if (iterator.hasNext()) {
						next = iterator.next();
						long diff = next.f1 -first.f2;
						if(diff < 10*60*1000 /* && trips ==2 */){
							out.collect(new Tuple4(first.f0,df.format(first.f1), df.format(next.f2), 2));
						}
					}
				}
		});
		saturatedVendors.addSink(new PrintSinkFunction<>());

        if (params.has("output")) {
        	saturatedVendors.writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }
        else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            saturatedVendors.print();
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

	public static class CustomCountTrigger<W extends Window> extends Trigger<Object, W> {
		private static final long serialVersionUID = 1L;
	
		private final long maxCount;
	
		private final ReducingStateDescriptor<Long> stateDesc =
				new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);
	
		private CustomCountTrigger(long maxCount) {
			this.maxCount = maxCount;
		}
	
		@Override
		public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
			ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
			count.add(1L);
			if (count.get() >= maxCount) {
				return TriggerResult.FIRE;
			}
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
			return TriggerResult.CONTINUE;
		}
	
		@Override
		public void clear(W window, TriggerContext ctx) throws Exception {
			ctx.getPartitionedState(stateDesc).clear();
		}
	
		@Override
		public boolean canMerge() {
			return true;
		}
	
		@Override
		public void onMerge(W window, OnMergeContext ctx) throws Exception {
			ctx.mergePartitionedState(stateDesc);
		}
	
		@Override
		public String toString() {
			return "CustomCountTrigger(" +  maxCount + ")";
		}
	
		/**
		 * Creates a trigger that fires once the number of elements in a pane reaches the given count.
		 *
		 * @param maxCount The count of elements at which to fire.
		 * @param <W> The type of {@link Window Windows} on which this trigger can operate.
		 */
		public static <W extends Window> CustomCountTrigger<W> of(long maxCount) {
			return new CustomCountTrigger<>(maxCount);
		}
	
		private static class Sum implements ReduceFunction<Long> {
			private static final long serialVersionUID = 1L;
	
			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return value1 + value2;
			}
	
		}
	}
}	