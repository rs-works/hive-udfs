package rsworks.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import rsworks.hive.Yearmonth;

/**
yearmonth(date) - Return the Year and Month value of date
Example:
  > SELECT yearmonth('2013-01-01') FROM dual;             
  201301
  > SELECT yearmonth('2013-1-1') FROM dual;
  201301
 */
public class YearmonthTest {

	@Test
	public void testString() {
		try {
			Yearmonth func = new Yearmonth();
			{
				String ret = func.evaluate("2013-01-01");
				assertEquals("201301", ret);
			}
			{
				String ret = func.evaluate("2013-1-1");
				assertEquals("201301", ret);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testSpeed() {
		String date = "2014-12-01";
		for (int j = 0; j < 10; j++) {
			Yearmonth udf = new Yearmonth();
			long s = System.currentTimeMillis();
			for (int i = 0; i < 100000; i++) {
				udf.evaluate(date);
			}
			long e = System.currentTimeMillis();
			if (j == 0) continue;
			assertTrue("ms:"+String.valueOf(e-s), e-s < 300);
		}
	}
}

