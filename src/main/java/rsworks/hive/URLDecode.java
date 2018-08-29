package rsworks.hive;

import java.net.URLDecoder;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "urldecode", value = "_FUNC_(str)  - Returns urldecoded string"
, extended = "Example: \n" +
			"  > SELECT urldecode('http%3a%2f%2fexample%2ecom%2f') FROM dual; \n" +
			"  http://example.com/"
		)
public class URLDecode extends UDF {

	private static final String NAME = "urldecode";

	public String getFunctionName() { return NAME; }

	public String evaluate(String val) {
		String to_val = null;
		if(val != null) {
			try {
				to_val = URLDecoder.decode(val, "UTF-8");
			} catch (Exception e) {
				to_val = new String(val);
			};
		}
		return to_val;
	}
}