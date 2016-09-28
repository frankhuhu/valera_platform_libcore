package libcore.valera;

public class ValeraUtil {
	
	private static final String TAG = "ValeraDebug";
	private static final boolean DEBUG = true;

	public static String getCallingStack(String delimer) {

		StackTraceElement[] cause = Thread.currentThread().getStackTrace();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cause.length; i++) {
			sb.append(cause[i].toString());
			sb.append(delimer);
		}
		
		for (int i = 0; i < sb.length(); i++)
			if (sb.charAt(i) == ' ')
				sb.setCharAt(i, '_');
		
		return sb.toString();
	}
	
	public static String getCallingStack(String delimer, int level) {

		StackTraceElement[] cause = Thread.currentThread().getStackTrace();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < cause.length; i++) {
			if (level <= 0) break;
			level--;
			
			sb.append(cause[i].toString());
			sb.append(delimer);
		}
		
		for (int i = 0; i < sb.length(); i++)
			if (sb.charAt(i) == ' ')
				sb.setCharAt(i, '_');
		
		return sb.toString();
	}
	
	public static void valeraAssert(boolean condition, String msg) {
		if (condition == false) {
			System.logE("Valera Assert Failed: " + msg);
			System.logE(getCallingStack("\n"));
			System.exit(-1);
		}
	}
	
	public static void valeraAbort(Exception e) {
		System.err.println(e.toString());
		e.printStackTrace();
		System.exit(-1);
	}
	
	public static void valeraDebugPrint(String log) {
		if (DEBUG) {
			System.logI(TAG + ": " + log);
		}
	}
}
