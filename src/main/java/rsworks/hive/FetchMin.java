package rsworks.hive;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

@SuppressWarnings("deprecation")
@Description(name = "fetch_min", value = "_FUNC_(expr, array)  - Return the Array of the minimum value of expr"
, extended = "Example:\n" +
			"  id name price\n" +
			"  1  A    100\n" +
			"  2  B    300\n" +
			"  3  C    500\n\n" +
			"  > SELECT fetch_min(price, array(id, name, price)) FROM item;\n" +
			"  [1, A, 100]")
public class FetchMin extends UDAF {

	private static final String NAME = "fetch_min";

	public FetchMin() {
	}

	public String getFunctionName() { return NAME; }

	public static class TempResult {
		public String        min_str = null;
		public Integer       min_num = null;
		public ArrayList<String> col = null;

		public TempResult() {
		}

		public TempResult clone() {
			TempResult clone = new TempResult();
			clone.min_str = this.min_str == null ? null : new String(this.min_str);
			clone.min_num = this.min_num == null ? null : new Integer(this.min_num);
			clone.col     = this.col     == null ? null : new ArrayList<String>(col);
			return clone;
		}

		public void set(String val, ArrayList<String> col) {
			this.min_str = val == null ? null : new String(val);
			this.col = col     == null ? null : new ArrayList<String>(col);
		}

		public void set(Integer val, ArrayList<String> col) {
			this.min_num = val == null ? null : new Integer(val);
			this.col = col     == null ? null : new ArrayList<String>(col);
		}
	}

	/**
	 * _FUNC_(string, array)
	 */
	public static class FetchMinStrEvaluator implements UDAFEvaluator {

		private TempResult tmp;

		public FetchMinStrEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(String val, ArrayList<String> col) {
			if (val == null) {
				return false;
			}
			if (tmp == null) {
				tmp = new TempResult();
			}
			if (tmp.min_str == null || tmp.min_str.compareTo(val) > 0) {
				tmp.set(val, col);
			}
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public TempResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(TempResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else if (otherTmp != null && otherTmp.min_str != null) {
				if (tmp.min_str == null || tmp.min_str.compareTo(otherTmp.min_str) > 0) {
					tmp = otherTmp.clone();
				}
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public ArrayList<String> terminate() {
			return tmp.col;
		}
	}

	/**
	 * _FUNC_(int, array)
	 */
	public static class FetchMinNumEvaluator implements UDAFEvaluator {

		private TempResult tmp;

		public FetchMinNumEvaluator() {
		}

		// Hiveから呼ばれ、UDFAEvaluatorクラスのインスタンスを初期化する
		public void init() {
			tmp = null;
		}

		// 新たな行のデータを処理して、集計バッファに入れる
		public boolean iterate(Integer val, ArrayList<String> col) {
			if (val == null) {
				return false;
			}
			if (tmp == null) {
				tmp = new TempResult();
			}
			if (tmp.min_num == null || tmp.min_num.compareTo(val) > 0) {
				tmp.set(val, col);
			}
			return true;
		}

		// 処理中の集計内容を永続化可能な形で返す
		public TempResult terminatePartial() {
			return tmp;
		}

		// terminatePartialが返す部分的な集計結果を、処理中の集計の内容にマージする
		public boolean merge(TempResult otherTmp) {
			if (otherTmp == null) {
				return false;
			}
			if (tmp == null) {
				tmp = otherTmp.clone();
			} else if (otherTmp != null && otherTmp.min_num != null) {
				if (tmp.min_num == null || tmp.min_num.compareTo(otherTmp.min_num) > 0) {
					tmp = otherTmp.clone();
				}
			}
			return true;
		}

		// 最終的な集計結果をHiveに返す
		public ArrayList<String> terminate() {
			return tmp.col;
		}
	}
}

