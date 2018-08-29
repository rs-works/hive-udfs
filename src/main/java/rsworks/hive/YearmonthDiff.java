package rsworks.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "yearmonthdiff", value = "_FUNC_(yearmonth1, yearmonth2)  - Returns the number of days between yearmonth1 and yearmonth2"
, extended = "yearmonth1 and yearmonth2 are strings in the format 'yyyyMM' or 'yyyy-MM'. The time parts are ignored.If yearmonth1 is earlier than yearmonth2, the result is negative.\n" +
			"Example:\n" +
			"   > SELECT yearmonthdiff('201401', '201312') FROM dual;\n" +
			"  1")
public class YearmonthDiff extends UDF {

	private static final String NAME = "yearmonthdiff";

	public YearmonthDiff() {
	}

	public String getFunctionName() { return NAME; }

	public Integer evaluate(String val1, String val2) {
		if (val1 == null || "".equals(val1)) {
			return null;
		}
		if (val2 == null || "".equals(val2)) {
			return null;
		}
		val1 = val1.replaceAll("-", "");
		val2 = val2.replaceAll("-", "");
		return diff(Integer.valueOf(val1), Integer.valueOf(val2));
	}

	public Integer evaluate(Integer val1, Integer val2) {
		if (val1 == null || "".equals(val1)) {
			return null;
		}
		if (val2 == null || "".equals(val2)) {
			return null;
		}
		return diff(val1, val2);
	}

	public Integer diff(Integer val1, Integer val2) {
		return (val1 / 100 * 12 + val1 % 100) - (val2 / 100 * 12 + val2 % 100);
	}
}

