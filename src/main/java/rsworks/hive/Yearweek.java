package rsworks.hive;

import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "yearweek", value = "_FUNC_(date)  - Return the Year of the Week value of date. Monday starting point"
, extended = "Example:\n" +
			"  > SELECT yearweek('2013-01-07') FROM dual;\n" +
			"  201301\n" +
			"  > SELECT yearweek('2013-01-01') FROM dual;\n" +
			"  201253")
public class Yearweek extends UDF {

	private static final String NAME = "yearweek";

	public Yearweek() {
	}

	public String getFunctionName() { return NAME; }

	public String evaluate(String val) {
		if (val == null || "".equals(val)) {
			return null;
		}
		String date = new String (val.length() > 10 ? val.substring(0, 10) : val);
		return getYearWeekString(date);
	}

	/**
	 * 日付から年と週数を返す 
	 * 月曜日を週の起点とする
	 * 年跨ぎ処理：その年の最初の月曜日以前は昨年の最終週として扱う
	 *             ex).2013-01-06(日) → 201253、 2013-01-07(月) → 201301
	 * @param date 'YYYY-MM-DD'形式
	 * @return yearweek 'YYYYWW'形式
	 */
	private static String getYearWeekString(String date) {
		if (date == null || date.length() == 0) return null;

		// 週数を取得
		String[] ymd = date.split("-");
		Calendar cal = Calendar.getInstance();
		cal.set(Integer.parseInt(ymd[0]), Integer.parseInt(ymd[1]) -1, Integer.parseInt(ymd[2]));
		cal.setMinimalDaysInFirstWeek(7);
		cal.setFirstDayOfWeek(Calendar.MONDAY);
		int year = cal.get(Calendar.YEAR);
		int weekNum = cal.get(Calendar.WEEK_OF_YEAR);
		
		// その年最初の月曜までは前年の最終週に寄せる
		if (cal.get(Calendar.MONTH) == Calendar.JANUARY  && weekNum >= 52) {
			year--;
		}

		return year + 
				(weekNum < 10 ? "0" : "") + 
				weekNum;
	}
}

