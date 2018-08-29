package rsworks.hive;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.CHMCache;
import com.maxmind.db.Reader;

@UDFType(deterministic = true)
@Description(
  name = "geoip",
value = "_FUNC_(ip,property,database)  - looks a property for an IP address from"
+ "a library loaded\n"
+ "The GeoLite2 database comes separated. To load the GeoLite2 use ADD FILE.\n"
+ "Usage:\n"
+ " > _FUNC_(ip, \"COUNTRY_NAME\", \"./GeoLite2-City.mmdb\")")
public class GeoIP extends GenericUDF {

	private static final String NAME = "geoip";

	public GeoIP() {
	}

	public String getFunctionName() { return NAME; }

	public static final String COUNTRY_NAME = "COUNTRY_NAME";
	public static final String COUNTRY_CODE = "COUNTRY_CODE";
	public static final String CITY = "CITY";
	public static final String TIME_ZONE = "TIME_ZONE";
	public static final String LATITUDE = "LATITUDE";
	public static final String LONGITUDE = "LONGITUDE";
	public static final String POSTAL_CODE = "POSTAL_CODE";

	private ObjectInspectorConverters.Converter[] converters;
	private static HashMap<String, Reader> databases = new HashMap<String, Reader>();

	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		if (arguments.length != 3) {
			throw new UDFArgumentLengthException("_FUNC_ accepts 3 arguments. " + arguments.length
					+ " found.");
		}
		for (int i = 0; i < arguments.length; i++) {
			if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
				throw new UDFArgumentTypeException(i,
						"A string argument was expected but an argument of type " + arguments[i].getTypeName()
						+ " was given.");
			}
		}
		converters = new ObjectInspectorConverters.Converter[arguments.length];
		for (int i = 0; i < arguments.length; i++) {
			converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
					PrimitiveObjectInspectorFactory.writableStringObjectInspector);
		}
		return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
	}

	@Override
	public Object evaluate(GenericUDF.DeferredObject[] arguments) throws HiveException {
		assert (arguments.length == 3);
		String ip = ((Text) converters[0].convert(arguments[0].get())).toString();
		String attributeName = ((Text) converters[1].convert(arguments[1].get())).toString();
		String databaseName = ((Text) converters[2].convert(arguments[2].get())).toString();
		Reader reader;
		if (!databases.containsKey(databaseName)) {
			File file = new File(databaseName);
			if (!file.exists()) {
				throw new HiveException(databaseName + " does not exist");
			}
			try {
				// cache reader
				reader = new Reader(file, new CHMCache());
				databases.put(databaseName, reader);
			} catch (IOException ex) {
				throw new HiveException(ex);
			}
		} else {
			reader = databases.get(databaseName);
		}
		String retVal = "";
		try {
			InetAddress address = InetAddress.getByName(ip);
			JsonNode response = reader.get(address);
			if (attributeName.equals(COUNTRY_NAME)) {
				retVal = response.get("country").get("names").get("en").asText();
			} else if (attributeName.equals(COUNTRY_CODE)) {
				retVal = response.get("country").get("iso_code").asText();
			} else if (attributeName.equals(CITY)) {
				retVal = response.get("city").get("names").get("en").asText();
			} else if (attributeName.equals(TIME_ZONE)) {
				retVal = response.get("location").get("time_zone").asText();
			} else if (attributeName.equals(LATITUDE)) {
				retVal = response.get("location").get("latitude").asText();
			} else if (attributeName.equals(LONGITUDE)) {
				retVal = response.get("location").get("longitude").asText();
			} else if (attributeName.equals(POSTAL_CODE)) {
				retVal = response.get("postal").get("code").asText();
			}
		} catch (Exception ex) {
			return null;
		}
		if (retVal == null) {
			return null;
		}
		return new Text(retVal);
	}

	@Override
	public String getDisplayString(String[] children) {
		assert (children.length == 3);
		return "_FUNC_( " + children[0] + ", " + children[1] + ", " + children[2] + " )";
	}
}
