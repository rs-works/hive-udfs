package rsworks.hive;

import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
import org.junit.Test;

import rsworks.hive.YearweekDiff;

/**
yearweekdiff(yearweek1, yearweek2) - Returns the number of days between yearweek1 and yearweek2
yearweek1 and yearweek2 are strings in the format 'yyyyWeek' or 'yyyy-Week'. 
The time parts are ignored.If yearweek1 is earlier than yearweek2, the result is negative.
Example:
   > SELECT yearweekdiff('201401', '201352') FROM dual;
   1
*/
public class YearweekDiffTest {

	@Test
	public void testInteger() {
		try {
			YearweekDiff func = new YearweekDiff();
			{
				int ret = func.evaluate(201351,201401);
				assertTrue(ret == -2);
			}
			{
				int ret = func.evaluate(201352,201401);
				assertTrue(ret == -1);
			}
			{
				int ret = func.evaluate(201401,201401);
				assertTrue(ret == 0);
			}
			{
				int ret = func.evaluate(201402,201401);
				assertTrue(ret == 1);
			}
			{
				int ret = func.evaluate(201403,201401);
				assertTrue(ret == 2);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testString() {
		try {
			YearweekDiff func = new YearweekDiff();
			{
				int ret = func.evaluate("201351","201401");
				assertTrue(ret == -2);
			}
			{
				int ret = func.evaluate("201352","201401");
				assertTrue(ret == -1);
			}
			{
				int ret = func.evaluate("201401","201401");
				assertTrue(ret == 0);
			}
			{
				int ret = func.evaluate("201402","201401");
				assertTrue(ret == 1);
			}
			{
				int ret = func.evaluate("201403","201401");
				assertTrue(ret == 2);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	@Ignore
	@Test
	public void testSpeed() {
		String week1 = "201746";
		String week2 = "201701";
		for (int j = 0; j < 10; j++) {
			YearweekDiff func = new YearweekDiff();
			long s = System.currentTimeMillis();
			for (int i = 0; i < 100000; i++) {
				func.evaluate(week1, week2);
			}
			long e = System.currentTimeMillis();
			if (j == 0) continue;
			assertTrue("ms:"+String.valueOf(e-s), e-s < 500);
			System.out.println("ms:"+String.valueOf(e-s));
		}
	}
}

