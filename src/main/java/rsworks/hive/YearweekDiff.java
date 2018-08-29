package rsworks.hive;

import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "yearweekdiff", value = "_FUNC_(yearweek1, yearweek2)  - Returns the number of days between yearweek1 and yearweek2"
, extended = "yearweek1 and yearweek2 are strings in the format 'yyyyWeek' or 'yyyy-Week'. The time parts are ignored.If yearweek1 is earlier than yearweek2, the result is negative.\n" +
			"Example:\n" +
			"   > SELECT yearweekdiff('201401', '201352') FROM dual;\n" +
			"  1")
public class YearweekDiff extends UDF {

	private static final String NAME = "yearweekdiff";

	public YearweekDiff() {
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
		return diff(val1, val2);
	}

	public Integer evaluate(Integer val1, Integer val2) {
		if (val1 == null || "".equals(val1)) {
			return null;
		}
		if (val2 == null || "".equals(val2)) {
			return null;
		}
		return diff(val1.toString(), val2.toString());
	}

	public Integer diff(String val1, String val2) {
		String start = getYearWeekTermString(val2)[0];
		String end   = getYearWeekTermString(val1)[1];
		Integer diff = null;
		try { 
			diff = diffDateTime(end, start);
			diff = (diff < 0 ? diff - 7 : diff) / 7;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return diff;
	}

	/**
	 * 年と週数から開始日付と終了日付を返す
	 * 月曜日を週の起点とする
	 * @param yearweek 'YYYYWW'形式
	 * @return String[] [0]:開始日付 [1]:終了日付
	 */
	private static String[] getYearWeekTermString(String yearweek){
		int year = Integer.parseInt(yearweek.substring(0, 4));
		int week = Integer.parseInt(yearweek.substring(4, 6));
		Calendar cal = Calendar.getInstance();
		cal.set(year, Calendar.JANUARY, 1);
		while (cal.get(Calendar.DAY_OF_WEEK) != Calendar.MONDAY) {
			cal.add(Calendar.DATE, 1);
		}

		String[] term = new String[2];
		cal.add(Calendar.DATE, (week - 1) * 7);
		term[0] = dateString(cal);

		cal.add(Calendar.DATE, 6);
		term[1] = dateString(cal);

		return term;
	}

	/**
	 * 日付文字列を取得
	 *     YYYY-MM-DD形式
	 * @param cal
	 * @return
	 */
	private static String dateString(Calendar cal) {
		return cal.get(Calendar.YEAR) + 
				"-" +
				(cal.get(Calendar.MONTH) + 1 < 10 ? "0" : "") + 
				(cal.get(Calendar.MONTH) + 1) +
				"-" +
				(cal.get(Calendar.DATE) < 10 ? "0" : "") + 
				cal.get(Calendar.DATE);
	}

	/**
	 * 差分の取得
	 */
	private static int diffDateTime(String dateA, String dateB) throws Exception{
		Calendar calA = getCalendarDateTime(dateA);
		Calendar calB = getCalendarDateTime(dateB);
		long oneDay = 1000 * 60 * 60 * 24;

		long diff = (calA.getTimeInMillis() - calB.getTimeInMillis()) / oneDay;
		return (int)diff;
	}

	/**
	 * "yyyy-MM-dd HH:mm:ss"形式の文字列から、日付をカレンダー形式で取得
	 * @param dateTimeStr
	 * @return
	 */
	private static Calendar getCalendarDateTime(String dateTimeStr) {
		String[] ymd = dateTimeStr.substring(0, 10).split("-");
		String[] his = null;
		if(dateTimeStr.length() > 10){
			his = dateTimeStr.substring(11, 19).split(":");
		}else{
			his = new String[]{"0","0","0"};
		}

		int y = Integer.parseInt(ymd[0]);
		int m = Integer.parseInt(ymd[1]);
		int d = Integer.parseInt(ymd[2]);
		int h = Integer.parseInt(his[0]);
		int i = Integer.parseInt(his[1]);
		int s = Integer.parseInt(his[2]);

		return new GregorianCalendar(y, m - 1, d, h, i, s);
	}
}

