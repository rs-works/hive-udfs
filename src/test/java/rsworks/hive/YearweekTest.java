package rsworks.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import rsworks.hive.Yearweek;

/**
	yearweek(date) - Return the Year of the Week value of date. Monday starting point
	Example:
	  > SELECT yearweek('2013-01-07') FROM dual;
	  201301
	  > SELECT yearweek('2013-01-01') FROM dual;
	  201253
 */
public class YearweekTest {

	@Test
	public void testString() {
		try {
			Yearweek func = new Yearweek();
			{
				String ret = func.evaluate("2013-01-01");
				assertEquals("201253", ret);
			}
			{
				String ret = func.evaluate("2013-01-07");
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
			Yearweek func = new Yearweek();
			long s = System.currentTimeMillis();
			for (int i = 0; i < 100000; i++) {
				func.evaluate(date);
			}
			long e = System.currentTimeMillis();
			if (j == 0) continue;
			assertTrue("ms:"+String.valueOf(e-s), e-s < 500);
			System.out.println("ms:"+String.valueOf(e-s));
		}
	}
}

