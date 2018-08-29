package rsworks.hive;

import java.math.BigDecimal;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@SuppressWarnings("deprecation")
@Description(name = "map_sum", value = "_FUNC_(key, num)  - Creates a map the sum numbers with the given key/numbers pairs." 
		 + "\n  option _FUNC_(key, num, accept), map_sum(map<key, num>)"
, extended = "Example:\n" +
			"  id name price\n" +
			"  1  A    100\n" +
			"  2  B    300\n" +
			"  3  A    500\n\n" +
			"  > SELECT map_sum(name, price) FROM item;\n" +
			"  [A:600, B:300]\n\n" +
			"  > SELECT map_sum(name, price, if(name = 'A', true, false)) FROM item;\n" +
			"  [A:600]\n\n" +
			"  > SELECT map_sum(map(name, price)) FROM item;\n" +
			"  [A:600, B:300]"
			)
public class MapSum extends UDAF {

	private static final String NAME = "map_sum";

	public MapSum() {
	}

	public String getFunctionName() { return NAME; }

	public static class IntResult {
		public HashMap<String, Integer> sum = null;

		public IntResult() {
			sum = new HashMap<String, Integer>();
		}

		public IntResult clone() {
			IntResult clone = new IntResult();
			clone.sum = this.sum == null ? null : new HashMap<String, Integer>(this.sum);
			return clone;
		}

		public void add(String key, Integer num) {
			if (key == null) key = "";
			if (num == null) return;
			Integer x = sum.get(key);
			if (x == null) {
				x = 0;
			}
			sum.put(key, x + num);
		}

		public void addAll(HashMap<String, Integer> sum) {
			if (sum != null) {
				for (String key : sum.keySet()) {
					add(key, sum.get(key));
				}
			}
		}
	}

	public static class DoubleResult {
		public HashMap<String, Double>  sum = null;

		public DoubleResult() {
			sum = new HashMap<String, Double>();
		}

		public DoubleResult clone() {
			DoubleResult clone = new DoubleResult();
			clone.sum = this.sum == null ? null : new HashMap<String, Double>(this.sum);
			return clone;
		}

		public void add(String key, Double num) {
			if (key == null) key = "";
			if (num == null) return;
			Double x = sum.get(key);
			if (x == null) {
				x = 0d;
			}
			sum.put(key, x + num);
		}

		public void addAll(HashMap<String, Double> sum) {
			if (sum != null) {
				for (String key : sum.keySet()) {
					add(key, sum.get(key));
				}
			}
		}
	}

	public static class DecimalResult {
		public HashMap<String, BigDecimal>  sum = null;

		public DecimalResult() {
			sum = new HashMap<String, BigDecimal>();
		}

		public DecimalResult clone() {
			DecimalResult clone = new DecimalResult();
			clone.sum = this.sum == null ? null : new HashMap<String, BigDecimal>(this.sum);
			return clone;
		}

		public void add(String key, BigDecimal num) {
			if (key == null) key = "";
			if (num == null) return;
			BigDecimal x = sum.get(key);
			if (x == null) {
				x = new BigDecimal(0);
			}
			sum.put(key, x.add(num));
		}

		public void addAll(HashMap<String, BigDecimal> sum) {
			if (sum != null) {
				for (String key : sum.keySet()) {
					add(key, sum.get(key));
				}
			}
		}
	}

	public static class StringResult {
		public HashMap<String, String>  sum = null;

		public StringResult() {
			sum = new HashMap<String, String>();
		}

		public StringResult clone() {
			StringResult clone = new StringResult();
			clone.sum = this.sum == null ? null : new HashMap<String, String>(this.sum);
			return clone;
		}

		public void add(String key, String num) {
			if (key == null) key = "";
			if (num == null) return;
			String x = sum.get(key);
			if (x == null) {
				x = "0";
			}
			try {
				BigDecimal d = new BigDecimal(x).add(new BigDecimal(num));
				sum.put(key, d.toPlainString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void addAll(HashMap<String, String> sum) {
			if (sum != null) {
				for (String key : sum.keySet()) {
					add(key, sum.get(key));
				}
			}
		}
	}

	/**
	 * _FUNC_(string, integer)
	 */
	public static class MapSumIntEvaluator implements UDAFEvaluator {

		public IntResult tmp;

		public MapSumIntEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String key, Integer num) {
			if (tmp == null) {
				tmp = new IntResult();
			}
			tmp.add(key, num);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public IntResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(IntResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, Integer> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(string, integer, boolean)
	 */
	public static class MapSumIntIgnoreEvaluator implements UDAFEvaluator {

		public IntResult tmp;

		public MapSumIntIgnoreEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String key, Integer num, Boolean accept) {
			if (tmp == null) {
				tmp = new IntResult();
			}
			if (accept != null && accept)
				tmp.add(key, num);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public IntResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(IntResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, Integer> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(map<string, integer>)
	 */
	public static class MapSumMapIntEvaluator implements UDAFEvaluator {

		public IntResult tmp;

		public MapSumMapIntEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(HashMap<String, Integer> map) {
			if (tmp == null) {
				tmp = new IntResult();
			}
			tmp.addAll(map);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public IntResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(IntResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, Integer> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(string, double)
	 */
	public static class MapSumDoubleEvaluator implements UDAFEvaluator {

		public DoubleResult tmp;

		public MapSumDoubleEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String key, Double num) {
			if (tmp == null) {
				tmp = new DoubleResult();
			}
			tmp.add(key, num);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public DoubleResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(DoubleResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, Double> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(string, double, boolean)
	 */
	public static class MapSumDoubleIgnoreEvaluator implements UDAFEvaluator {

		public DoubleResult tmp;

		public MapSumDoubleIgnoreEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String key, Double num, Boolean accept) {
			if (tmp == null) {
				tmp = new DoubleResult();
			}
			if (accept != null && accept)
				tmp.add(key, num);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public DoubleResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(DoubleResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, Double> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(map<string, double>)
	 */
	public static class MapSumMapDoubleEvaluator implements UDAFEvaluator {

		public DoubleResult tmp;

		public MapSumMapDoubleEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(HashMap<String, Double> map) {
			if (tmp == null) {
				tmp = new DoubleResult();
			}
			tmp.addAll(map);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public DoubleResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(DoubleResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, Double> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(string, decimal)
	 */
	public static class MapSumBigDecimalEvaluator implements UDAFEvaluator {

		private DecimalResult tmp;

		public MapSumBigDecimalEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String key, BigDecimal num) {
			if (tmp == null) {
				tmp = new DecimalResult();
			}
			tmp.add(key, num);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public DecimalResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(DecimalResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, BigDecimal> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(string, decimal, boolean)
	 */
	public static class MapSumBigDecimalIgnoreEvaluator implements UDAFEvaluator {

		private DecimalResult tmp;

		public MapSumBigDecimalIgnoreEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String key, BigDecimal num, Boolean accept) {
			if (tmp == null) {
				tmp = new DecimalResult();
			}
			if (accept != null && accept)
				tmp.add(key, num);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public DecimalResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(DecimalResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, BigDecimal> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(map<string, decimal>)
	 */
	public static class MapSumMapDecimalEvaluator implements UDAFEvaluator {

		public DecimalResult tmp;

		public MapSumMapDecimalEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(HashMap<String, BigDecimal> map) {
			if (tmp == null) {
				tmp = new DecimalResult();
			}
			tmp.addAll(map);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public DecimalResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(DecimalResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, BigDecimal> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(string, string)
	 */
	public static class MapSumStringEvaluator implements UDAFEvaluator {

		public StringResult tmp;

		public MapSumStringEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String key, String num) {
			if (tmp == null) {
				tmp = new StringResult();
			}
			tmp.add(key, num);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public StringResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(StringResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, String> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(string, string, boolean)
	 */
	public static class MapSumStringIgnoreEvaluator implements UDAFEvaluator {

		public StringResult tmp;

		public MapSumStringIgnoreEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String key, String num, Boolean accept) {
			if (tmp == null) {
				tmp = new StringResult();
			}
			if (accept != null && accept)
				tmp.add(key, num);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public StringResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(StringResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, String> terminate() {
			return tmp.sum;
		}
	}

	/**
	 * _FUNC_(map<string, string>)
	 */
	public static class MapSumMapStringEvaluator implements UDAFEvaluator {

		public StringResult tmp;

		public MapSumMapStringEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(HashMap<String, String> map) {
			if (tmp == null) {
				tmp = new StringResult();
			}
			tmp.addAll(map);
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public StringResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(StringResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else {
				tmp.addAll(otherTmp.sum);
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public HashMap<String, String> terminate() {
			return tmp.sum;
		}
	}
}

