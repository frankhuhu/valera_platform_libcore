package libcore.valera;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;


public class ValeraConfig {

	private static boolean sValeraEnable = false;
	private static int sValeraMode = ValeraConstant.MODE_NONE;
	private static String sPkgName = null;
	private static boolean sReplayNetwork = false;
	private static String sActionTraceFilePath = null;
	private static String sSchedulerFilePath = null;
	private static boolean sTraceInputEvent = false;
	private static long sConfigLoadTime = 0;
	
	public static boolean isValeraEnabled() {
		return sValeraEnable;
	}
	
	public static int getValeraMode() {
		return sValeraMode;
	}
	
	public static String getPackageName() {
		return sPkgName;
	}
	
	public static boolean canReplayNetwork() {
		return sReplayNetwork;
	}
	
	public static boolean canTraceInputEvent() {
		return sTraceInputEvent;
	}
	
	public static String getActionTraceFilePath() {
		return sActionTraceFilePath;
	}
	
	public static String getSchedulerFilePath() {
		return sSchedulerFilePath;
	}
	
	public static long getConfigLoadTime() {
		// Libcore cannot use SystemClock as it is below android 
		// framework api. Thus we use System.nanoTime. 
		// Config load time is pretty close to app start time.
		return sConfigLoadTime;
	}
	
	public static void loadConfig() {
		sConfigLoadTime = System.nanoTime() / 1000000;
		sValeraEnable = Thread.currentThread().valeraIsEnabled();
		sPkgName = Thread.currentThread().valeraPackageName();
		
		sValeraMode = Thread.currentThread().valeraGetMode();
		ValeraUtil.valeraAssert(
				sValeraMode == ValeraConstant.MODE_RECORD ||
				sValeraMode == ValeraConstant.MODE_REPLAY ||
				sValeraMode == ValeraConstant.MODE_NONE,
				"Invalid valera mode.");
		System.logI("Valera mode is " + sValeraMode);
		
		Scanner scanner = null;
		try {
			File optFile = new File("/data/data/" + sPkgName + "/valera/option.txt");
			FileInputStream fis = new FileInputStream(optFile);
			scanner = new Scanner(fis);
			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				if (line.startsWith("trace_action=")) {
					String ss[] = line.split("=");
					ValeraUtil.valeraAssert(ss.length == 2, "Format error: " + line);
					String filename = ss[1].trim();
					sActionTraceFilePath = filename;
				} else if (line.startsWith("schedule=")) {
					String ss[] = line.split("=");
					ValeraUtil.valeraAssert(ss.length == 2, "Format error: " + line);
					String filename = ss[1].trim();
					sSchedulerFilePath = filename;
				} else if (line.startsWith("network_replay=")) {
					String ss[] = line.split("=");
					ValeraUtil.valeraAssert(ss.length == 2, "Format error: " + line);
					try {
						int enabled = Integer.parseInt(ss[1]);
						ValeraUtil.valeraAssert(enabled == 0 | enabled == 1, 
								"Network Replay Option can only be 0 or 1.");
						sReplayNetwork = enabled == 1;
					} catch (NumberFormatException nfe) {
						ValeraUtil.valeraAbort(nfe);
					}
				} else if (line.startsWith("trace_inputevent=")) {
					String ss[] = line.split("=");
					ValeraUtil.valeraAssert(ss.length == 2, "Format error: " + line);
					try {
						int enabled = Integer.parseInt(ss[1]);
						ValeraUtil.valeraAssert(enabled == 0 | enabled == 1, 
								"Trace inputevent option can only be 0 or 1.");
						sTraceInputEvent = enabled == 1;
					} catch (NumberFormatException nfe) {
						ValeraUtil.valeraAbort(nfe);
					}
				}
			}
		} catch (FileNotFoundException fnfe) {
			ValeraUtil.valeraDebugPrint("Could not find option.txt. Valera mode is " + sValeraMode);
		} finally {
			if (scanner != null)
				scanner.close();
		}
	}
}
