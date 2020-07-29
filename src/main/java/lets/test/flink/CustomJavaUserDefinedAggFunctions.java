package lets.test.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;
import java.util.Map;

public class CustomJavaUserDefinedAggFunctions {
	public static boolean isCloseCalled = false;

	public CustomJavaUserDefinedAggFunctions() {
	}

	public static class MultiArgSum extends AggregateFunction<Long, MultiArgSumAcc> {
		public MultiArgSum() {
		}

		public MultiArgSumAcc createAccumulator() {
			MultiArgSumAcc acc = new MultiArgSumAcc();
			acc.count = 0L;
			return acc;
		}

		public void accumulate(MultiArgSumAcc acc, long in1, long in2) {
			acc.count += in1 + in2;
		}

		public void retract(MultiArgSumAcc acc, long in1, long in2) {
			acc.count -= in1 + in2;
		}

		public Long getValue(MultiArgSumAcc acc) {
			return acc.count;
		}
	}

	public static class MultiArgSumAcc {
		public long count;

		public MultiArgSumAcc() {
		}
	}

	public static class MultiArgCount extends AggregateFunction<Long, MultiArgCountAcc> {
		public MultiArgCount() {
		}

		public MultiArgCountAcc createAccumulator() {
			MultiArgCountAcc acc = new MultiArgCountAcc();
			acc.count = 0L;
			return acc;
		}

		public void accumulate(MultiArgCountAcc acc, Object in1, Object in2) {
			if (in1 != null && in2 != null) {
				++acc.count;
			}

		}

		public void retract(MultiArgCountAcc acc, Object in1, Object in2) {
			if (in1 != null && in2 != null) {
				--acc.count;
			}

		}

		public void merge(MultiArgCountAcc accumulator, Iterable<MultiArgCountAcc> iterable) {
			MultiArgCountAcc otherAcc;
			for(Iterator var3 = iterable.iterator(); var3.hasNext(); accumulator.count += otherAcc.count) {
				otherAcc = (MultiArgCountAcc)var3.next();
			}

		}

		public Long getValue(MultiArgCountAcc acc) {
			return acc.count;
		}
	}

	public static class MultiArgCountAcc {
		public long count;

		public MultiArgCountAcc() {
		}
	}

	public static class DataViewTestAgg extends AggregateFunction<Long, DataViewTestAccum> {
		public DataViewTestAgg() {
		}

		public DataViewTestAccum createAccumulator() {
			DataViewTestAccum accum = new DataViewTestAccum();
			accum.map = new MapView(Types.STRING, Types.INT);
			accum.count = 0L;
			return accum;
		}

		public void accumulate(DataViewTestAccum accumulator, String a, long b) {
			try {
				if (!accumulator.map.contains(a)) {
					accumulator.map.put(a, 1);
					++accumulator.count;
				}

				accumulator.list.add(b);
			} catch (Exception var6) {
				var6.printStackTrace();
			}

		}

		public Long getValue(DataViewTestAccum accumulator) {
			long sum = accumulator.count;

			Long value;
			try {
				for(Iterator var4 = accumulator.list.get().iterator(); var4.hasNext(); sum += value) {
					value = (Long)var4.next();
				}
			} catch (Exception var6) {
				var6.printStackTrace();
			}

			return sum;
		}

		public void close() {
			isCloseCalled = true;
		}
	}

	public static class DataViewTestAccum {
		public MapView<String, Integer> map;
		public MapView<String, Integer> map2;
		public long count;
		private ListView<Long> list;

		public DataViewTestAccum() {
			this.list = new ListView(Types.LONG);
		}

		public ListView<Long> getList() {
			return this.list;
		}

		public void setList(ListView<Long> list) {
			this.list = list;
		}
	}

	public static class CountDistinctWithRetractAndReset extends CountDistinct {
		public CountDistinctWithRetractAndReset() {
		}

		public void retract(CountDistinctAccum accumulator, long id) {
			try {
				Integer cnt = (Integer)accumulator.map.get(String.valueOf(id));
				if (cnt != null) {
					cnt = cnt - 1;
					if (cnt <= 0) {
						accumulator.map.remove(String.valueOf(id));
						--accumulator.count;
					} else {
						accumulator.map.put(String.valueOf(id), cnt);
					}
				}
			} catch (Exception var5) {
				var5.printStackTrace();
			}

		}

		public void resetAccumulator(CountDistinctAccum acc) {
			acc.map.clear();
			acc.count = 0L;
		}
	}

	public static class CountDistinctWithMergeAndReset extends CountDistinctWithMerge {
		public CountDistinctWithMergeAndReset() {
		}

		public void resetAccumulator(CountDistinctAccum acc) {
			acc.map.clear();
			acc.count = 0L;
		}
	}

	public static class CountDistinctWithMerge extends CountDistinct {
		public CountDistinctWithMerge() {
		}

		public void merge(CountDistinctAccum acc, Iterable<CountDistinctAccum> it) {
			Iterator iter = it.iterator();

			while(iter.hasNext()) {
				CountDistinctAccum mergeAcc = (CountDistinctAccum)iter.next();
				acc.count += mergeAcc.count;

				try {
					Iterator itrMap = mergeAcc.map.iterator();

					while(itrMap.hasNext()) {
						Map.Entry<String, Integer> entry = (Map.Entry)itrMap.next();
						String key = (String)entry.getKey();
						Integer cnt = (Integer)entry.getValue();
						if (acc.map.contains(key)) {
							acc.map.put(key, (Integer)acc.map.get(key) + cnt);
						} else {
							acc.map.put(key, cnt);
						}
					}
				} catch (Exception var9) {
					var9.printStackTrace();
				}
			}

		}
	}

	public static class CountDistinct extends AggregateFunction<Long, CountDistinctAccum> {
		public CountDistinct() {
		}

		public CountDistinctAccum createAccumulator() {
			CountDistinctAccum accum = new CountDistinctAccum();
			accum.map = new MapView(Types.STRING, Types.INT);
			accum.count = 0L;
			return accum;
		}

		public void accumulate(CountDistinctAccum accumulator, String id) {
			try {
				Integer cnt = (Integer)accumulator.map.get(id);
				if (cnt != null) {
					cnt = cnt + 1;
					accumulator.map.put(id, cnt);
				} else {
					accumulator.map.put(id, 1);
					++accumulator.count;
				}
			} catch (Exception var4) {
				var4.printStackTrace();
			}

		}

		public void accumulate(CountDistinctAccum accumulator, long id) {
			try {
				Integer cnt = (Integer)accumulator.map.get(String.valueOf(id));
				if (cnt != null) {
					cnt = cnt + 1;
					accumulator.map.put(String.valueOf(id), cnt);
				} else {
					accumulator.map.put(String.valueOf(id), 1);
					++accumulator.count;
				}
			} catch (Exception var5) {
				var5.printStackTrace();
			}

		}

		public Long getValue(CountDistinctAccum accumulator) {
			return accumulator.count;
		}
	}

	public static class CountDistinctAccum {
		public MapView<String, Integer> map;
		public long count;

		public CountDistinctAccum() {
		}
	}

	public static class WeightedAvgWithRetract extends WeightedAvg {
		public WeightedAvgWithRetract() {
		}

		public void retract(WeightedAvgAccum accumulator, long iValue, int iWeight) {
			accumulator.sum -= iValue * (long)iWeight;
			accumulator.count -= iWeight;
		}

		public void retract(WeightedAvgAccum accumulator, int iValue, int iWeight) {
			accumulator.sum -= (long)(iValue * iWeight);
			accumulator.count -= iWeight;
		}
	}

	public static class WeightedAvgWithMergeAndReset extends WeightedAvgWithMerge {
		public WeightedAvgWithMergeAndReset() {
		}

		public void resetAccumulator(WeightedAvgAccum acc) {
			acc.count = 0;
			acc.sum = 0L;
		}
	}

	public static class WeightedAvgWithMerge extends WeightedAvg {
		public WeightedAvgWithMerge() {
		}

		public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
			WeightedAvgAccum a;
			for(Iterator iter = it.iterator(); iter.hasNext(); acc.sum += a.sum) {
				a = (WeightedAvgAccum)iter.next();
				acc.count += a.count;
			}

		}

		public String toString() {
			return "myWeightedAvg";
		}
	}

	public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {
		public WeightedAvg() {
		}

		public WeightedAvgAccum createAccumulator() {
			return new WeightedAvgAccum();
		}

		public Long getValue(WeightedAvgAccum accumulator) {
			return accumulator.count == 0 ? null : accumulator.sum / (long)accumulator.count;
		}

		public void accumulate(WeightedAvgAccum accumulator, long iValue, int iWeight, int x, String string) {
			accumulator.sum += (iValue + (long)Integer.parseInt(string)) * (long)iWeight;
			accumulator.count += iWeight;
		}

		public void accumulate(WeightedAvgAccum accumulator, long iValue, int iWeight) {
			accumulator.sum += iValue * (long)iWeight;
			accumulator.count += iWeight;
		}

		public void accumulate(WeightedAvgAccum accumulator, int iValue, int iWeight) {
			accumulator.sum += (long)(iValue * iWeight);
			accumulator.count += iWeight;
		}
	}

	public static class WeightedAvgAccum {
		public long sum = 0L;
		public int count = 0;

		public WeightedAvgAccum() {
		}
	}

	public static class OverAgg0 extends AggregateFunction<Long, Accumulator0> {
		public OverAgg0() {
		}

		public Accumulator0 createAccumulator() {
			return new Accumulator0();
		}

		public Long getValue(Accumulator0 accumulator) {
			return 1L;
		}

		public void accumulate(Accumulator0 accumulator, long iValue, int iWeight) {
		}

		public boolean requiresOver() {
			return true;
		}
	}

	public static class Accumulator0 extends Tuple2<Long, Integer> {
		public Accumulator0() {
		}
	}
}