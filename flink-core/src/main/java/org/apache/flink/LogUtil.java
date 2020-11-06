package org.apache.flink;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LogUtil {

	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public static void info(String format, Object... objects) {
		String msg = String.format("[INFO] " + dateFormat.format(new Date()) + " " + format, objects);
		System.out.println(msg);
	}

}
