package rsworks.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "yearmonth", value = "_FUNC_(date)  - Return the Year and Month value of date"
, extended = "Example:\n" +
			"  > SELECT yearmonth('2013-01-01') FROM dual;\n" +
			"  201301\n" +
			"  > SELECT yearmonth('2013-1-1') FROM dual;\n" +
			"  201301")
public class Yearmonth extends UDF {

	private static final String NAME = "yearmonth";

	public Yearmonth() {
	}

	public String getFunctionName() { return NAME; }

	public String evaluate(String val) {
		if (val == null || "".equals(val)) {
			return null;
		}
		String[] date = val.split("-");
		return date[0] + 
				(date[1].length() < 2 ? "0" : "") + 
				date[1];
	}
}

