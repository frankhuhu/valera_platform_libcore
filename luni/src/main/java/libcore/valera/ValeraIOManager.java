package libcore.valera;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.PlainSocketImpl;
import java.net.PlainSocketImpl.PlainSocketInputStream;
import java.net.PlainSocketImpl.PlainSocketOutputStream;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.harmony.xnet.provider.jsse.OpenSSLSocketImpl;
import org.apache.harmony.xnet.provider.jsse.NativeCrypto.SSLHandshakeCallbacks;
import org.apache.harmony.xnet.provider.jsse.OpenSSLSocketImpl.SSLInputStream;
import org.apache.harmony.xnet.provider.jsse.OpenSSLSocketImpl.SSLOutputStream;


class IOIndexEntry {
	static final int CONN_TYPE_HTTP = 1;
	static final int CONN_TYPE_HTTPS = 2;

	static final int INDEX_STATE_NO_DATA = 0;
	static final int INDEX_STATE_HAS_DATA = 1;
	static final int INDEX_STATE_RELEASE_DATA = 2;

	int connId;
	String operation;
	String uri;
	int type;
	int state;
	int numRequest;
	int numResponse;
	LinkedList<IOEntry> requests;
	LinkedList<IOEntry> responses;

	IOIndexEntry(int connId, String op, String uri, int type, int numRequest,
			int numResponse) {
		this.connId = connId;
		this.operation = op;
		this.uri = uri;
		this.type = type;
		this.state = INDEX_STATE_NO_DATA;
		this.numRequest = numRequest;
		this.numResponse = numResponse;
		this.requests = new LinkedList<IOEntry>();
		this.responses = new LinkedList<IOEntry>();
	}

	void addRequestEntry(IOEntry entry) {
		this.requests.addLast(entry);
	}

	void addResponseEntry(IOEntry entry) {
		this.responses.addLast(entry);
	}

	void releaseData() {
		while (requests.poll() != null)
			;
		while (responses.poll() != null)
			;
		this.state = INDEX_STATE_RELEASE_DATA;
	}
}

class IOIndexEntryWrapper {
	LinkedList<IOIndexEntry> list;

	// int curPos;

	public IOIndexEntryWrapper() {
		this.list = new LinkedList<IOIndexEntry>();
		// this.curPos = 0;
		// this.list = al;
	}
}

abstract class IOEntry {
	int tag;

	IOEntry(int tag) {
		this.tag = tag;
	}
}

abstract class NetworkEntry extends IOEntry {
	int connId;

	NetworkEntry(int tag, int connId) {
		super(tag);
		this.connId = connId;
	}
}

class PlainSocketRead extends NetworkEntry {
	long time;
	Exception exception;
	int nread;
	byte[] buffer;

	PlainSocketRead(int tag, int connId, long time, Exception e, int nread,
			byte[] buffer) {
		super(tag, connId);
		this.time = time;
		this.exception = e;
		this.nread = nread;
		this.buffer = buffer;
	}
}

class PlainSocketWrite extends NetworkEntry {
	long time;
	Exception exception;
	byte[] buffer;

	PlainSocketWrite(int tag, int connId, long time, Exception e, byte[] buffer) {
		super(tag, connId);
		this.time = time;
		this.exception = e;
		this.buffer = buffer;
	}
}

class SSLSocketRead extends NetworkEntry {
	long time;
	Exception exception;
	int nread;
	byte[] buffer;

	SSLSocketRead(int tag, int connId, long time, Exception e, int nread,
			byte[] buffer) {
		super(tag, connId);
		this.time = time;
		this.exception = e;
		this.nread = nread;
		this.buffer = buffer;
	}
}

class SSLSocketWrite extends NetworkEntry {
	long time;
	Exception exception;
	byte[] buffer;

	SSLSocketWrite(int tag, int connId, long time, Exception e, byte[] buffer) {
		super(tag, connId);
		this.time = time;
		this.exception = e;
		this.buffer = buffer;
	}
}

public class ValeraIOManager {
	private final static String TAG = "ValeraIOManager";
	private final static String LOG_FILE = "io.bin";
	private final static String INDEX_FILE = "io.index";

	private final static int IO_PLAINSOCKET_READ = 1;
	private final static int IO_PLAINSOCKET_WRITE = 2;
	private final static int IO_SSL_READ = 3;
	private final static int IO_SSL_WRITE = 4;
	private final static int IO_CONNECTION = 5;

	public final static int CONNECTION_NOT_FOUND = -99;

	private static boolean sInitialized = false;
	private Object sLock;
	private static ValeraLogWriter sWriter;
	private static ValeraIOManager sDefaultManager;
	private static AtomicInteger sConnId = new AtomicInteger(0);
	private static HashMap<String, IOIndexEntryWrapper> sIOIndexHash;
	private static HashMap<Integer, IOIndexEntry> sConnMap;

	private ValeraIOManager() {
		if (!ValeraConfig.canReplayNetwork())
			return;
		try {
			sLock = new Object();
			switch (Thread.currentThread().valeraGetMode()) {
			case 1: // record
				sWriter = new ValeraLogWriter(new FileOutputStream(
						"/data/data/"
								+ Thread.currentThread().valeraPackageName()
								+ "/valera/" + LOG_FILE, false));
				break;
			case 2: // replay
				sIOIndexHash = readIOIndex("/data/data/"
						+ Thread.currentThread().valeraPackageName()
						+ "/valera/" + INDEX_FILE);
				sConnMap = new HashMap<Integer, IOIndexEntry>();
				break;
			}
		} catch (Exception e) {
			ValeraUtil.valeraAbort(e);
		}
	}

	public static boolean isInitialized() {
		return sInitialized;
	}

	public static ValeraIOManager getInstance() {
		synchronized (ValeraIOManager.class) {
			if (sDefaultManager == null) {
				sDefaultManager = new ValeraIOManager();
				sInitialized = true;
			}
			return sDefaultManager;
		}
	}

	private HashMap<String, IOIndexEntryWrapper> readIOIndex(String path)
			throws IOException, FileNotFoundException, IOException,
			ClassNotFoundException {
		HashMap<String, IOIndexEntryWrapper> hash = new HashMap<String, IOIndexEntryWrapper>();

		ValeraLogReader reader = new ValeraLogReader(new FileInputStream(path));
		int size = reader.readInt();
		for (int i = 0; i < size; i++) {
			int connId = reader.readInt();
			String method = reader.readString();
			String uri = reader.readString();
			int type = reader.readInt();
			int numRequest = reader.readInt();
			int numResponse = reader.readInt();

			IOIndexEntry entry = new IOIndexEntry(connId, method, uri, type,
					numRequest, numResponse);
			String key = method + " " + uri;
			IOIndexEntryWrapper wrapper = hash.get(key);
			if (wrapper == null) {
				wrapper = new IOIndexEntryWrapper();
				hash.put(key, wrapper);
			}
			
			wrapper.list.add(entry);
		}
		reader.close();
		
		return hash;
	}

	private void readIOConnection(IOIndexEntry index) {
		ValeraLogReader reader = null;
		ValeraUtil.valeraAssert(
				index.state == IOIndexEntry.INDEX_STATE_NO_DATA,
				"This index entry should be in NO_DATA state");

		try {
			reader = new ValeraLogReader(new FileInputStream("/data/data/"
					+ Thread.currentThread().valeraPackageName() + "/valera/"
					+ LOG_FILE + "." + index.connId));

			while (true) {
				int tag = reader.readInt();
				long relativeTime = reader.readLong();
				switch (tag) {
				case IO_PLAINSOCKET_READ: {
					ValeraUtil.valeraAssert(
							index.type == IOIndexEntry.CONN_TYPE_HTTP,
							"This should be a http connection. connId="
									+ index.connId + " type=" + index.type);
					int recordConnId = reader.readInt();
					long sleepTime = reader.readLong();
					int hasException = reader.readInt();
					Exception exception = null;
					int nread = 0;
					byte[] buf = null;
					if (hasException != 0) {
						exception = (IOException) reader.readException();
					} else {
						nread = reader.readInt();
						if (nread > 0)
							buf = reader.readByteArray();
					}
					index.addResponseEntry(new PlainSocketRead(tag,
							recordConnId, sleepTime, exception, nread, buf));
				}
					break;
				case IO_PLAINSOCKET_WRITE: {
					ValeraUtil.valeraAssert(
							index.type == IOIndexEntry.CONN_TYPE_HTTP,
							"This should be a http connection. connId="
									+ index.connId + " type=" + index.type);
					int recordConnId = reader.readInt();
					long sleepTime = reader.readLong();
					int hasException = reader.readInt();
					Exception exception = null;
					byte[] buf = null;
					if (hasException != 0) {
						exception = (IOException) reader.readException();
					} else {
						buf = reader.readByteArray();
					}
					index.addRequestEntry(new PlainSocketWrite(tag,
							recordConnId, sleepTime, exception, buf));
				}
					break;
				case IO_SSL_READ: {
					ValeraUtil.valeraAssert(
							index.type == IOIndexEntry.CONN_TYPE_HTTPS,
							"This should be a https connection. connId="
									+ index.connId + " type=" + index.type);
					int recordConnId = reader.readInt();
					long sleepTime = reader.readLong();
					int hasException = reader.readInt();
					Exception exception = null;
					int nread = 0;
					byte[] buf = null;
					if (hasException != 0) {
						exception = (IOException) reader.readException();
					} else {
						nread = reader.readInt();
						if (nread > 0)
							buf = reader.readByteArray();
					}
					index.addResponseEntry(new SSLSocketRead(tag, recordConnId,
							sleepTime, exception, nread, buf));
				}
					break;
				case IO_SSL_WRITE: {
					ValeraUtil.valeraAssert(
							index.type == IOIndexEntry.CONN_TYPE_HTTPS,
							"This should be a https connection. connId="
									+ index.connId + " type=" + index.type);
					int recordConnId = reader.readInt();
					long sleepTime = reader.readLong();
					int hasException = reader.readInt();
					Exception exception = null;
					byte[] buf = null;
					if (hasException != 0) {
						exception = (IOException) reader.readException();
					} else {
						buf = reader.readByteArray();
					}
					index.addRequestEntry(new SSLSocketWrite(tag, recordConnId,
							sleepTime, exception, buf));
				}
					break;
				}
			}
		} catch (EOFException eof) {

		} catch (Exception ex) {
			ValeraUtil.valeraAbort(ex);
		} finally {
			try {
				if (reader != null)
					reader.close();
			} catch (IOException ioe) {
				ValeraUtil.valeraAbort(ioe);
			}
			index.state = IOIndexEntry.INDEX_STATE_HAS_DATA;
		}
		
		ValeraUtil.valeraDebugPrint(String.format("readIOConnection %d %s done!", index.connId, index.uri));
	}

	public int getUniqueConnId() {
		return sConnId.incrementAndGet();
	}

	public void setConnIdForSocketIn(InputStream socketIn, int connId) {
		if (!Thread.currentThread().valeraIsEnabled())
			return;

		if (socketIn instanceof PlainSocketImpl.PlainSocketInputStream) {
			PlainSocketInputStream in = (PlainSocketInputStream) socketIn;
			in.setConnId(connId);
		} else if (socketIn instanceof OpenSSLSocketImpl.SSLInputStream) {
			SSLInputStream in = (SSLInputStream) socketIn;
			in.setConnId(connId);
		} else if (socketIn instanceof BufferedInputStream) {
			BufferedInputStream bis = (BufferedInputStream) socketIn;
			InputStream in = bis.getInputStream();
			ValeraUtil
					.valeraAssert(in instanceof PlainSocketInputStream,
							"This BufferedInputStream should wrap PlainSocketInputStream");
			((PlainSocketInputStream) in).setConnId(connId);
		} else {
			ValeraUtil.valeraAssert(false,
					"Unhandle socketIn: " + socketIn.toString());
		}
		// System.out.println("WTF: socketIn=" + socketIn.toString());
	}

	public void setConnIdForSocketOut(OutputStream socketOut, int connId) {
		if (!Thread.currentThread().valeraIsEnabled())
			return;

		if (socketOut instanceof PlainSocketImpl.PlainSocketOutputStream) {
			PlainSocketOutputStream out = (PlainSocketOutputStream) socketOut;
			out.setConnId(connId);
		} else if (socketOut instanceof OpenSSLSocketImpl.SSLOutputStream) {
			SSLOutputStream out = (SSLOutputStream) socketOut;
			out.setConnId(connId);
		} else {
			ValeraUtil.valeraAssert(false,
					"Unhandle socketOut: " + socketOut.toString());
		}
		// System.out.println("WTF: socketOut=" + socketOut.toString());
	}

	// Establish a connection relation between connId and method+uri.
	public void establishConnectionMap(int connId, String method, URI uri,
			String debug) {
		if (!ValeraConfig.canReplayNetwork())
			return;
		ValeraUtil.valeraAssert(connId > 0, "Invalid connction ID.");
		switch (Thread.currentThread().valeraGetMode()) {
		case ValeraConstant.MODE_NONE:
			break;
		case ValeraConstant.MODE_RECORD:
			synchronized (sLock) {
				try {
					sWriter.writeInt(IO_CONNECTION);
					sWriter.writeLong(System.nanoTime() / 1000000 - ValeraConfig.getConfigLoadTime());
					sWriter.writeInt(connId);
					sWriter.writeString(method);
					sWriter.writeString(uri.getPath());
					sWriter.flush();

					ValeraUtil
							.valeraDebugPrint(String
									.format("yhu009: record network connection connId=%d uri=%s debug=%s",
											connId, uri, debug));
				} catch (Exception e) {
					ValeraUtil.valeraAbort(e);
				}
			}
			break;
		case ValeraConstant.MODE_REPLAY: {
			ValeraUtil
					.valeraDebugPrint(String
							.format("yhu009: replay network connection connId=%d uri=%s debug=%s",
									connId, uri, debug));
			String key = method + " " + uri.getPath();

			IOIndexEntry entry = null;
			IOIndexEntryWrapper wrapper = sIOIndexHash.get(key);
			if (wrapper != null) {
				// ValeraUtil.valeraAssert(wrapper != null,
				// "Cannot find request: " + key);
				// ValeraUtil.valeraAssert(wrapper.curPos < wrapper.list.size(),
				// String.format("Wrapper cursor(%d) exceed list size(%d): %s",
				// wrapper.curPos, wrapper.list.size(), key));
				// entry = wrapper.list.get(wrapper.curPos);
				// wrapper.curPos++;
				entry = wrapper.list.poll();
				ValeraUtil.valeraAssert(entry != null,
						"Cannot find IOIndexEntry from wrapper");

				synchronized (sConnMap) {
					IOIndexEntry e = sConnMap.get(connId);
					if (e == null) {
						/*
						 * try { entry.reader = new ValeraLogReader(new
						 * FileInputStream("/data/data/" +
						 * Thread.currentThread().valeraPackageName() +
						 * "/valera/" + LOG_FILE + "." + entry.connId)); } catch
						 * (Exception ex) { ValeraUtil.valeraAbort(ex); }
						 */
						sConnMap.put(connId, entry);
					} else {
						ValeraUtil.valeraAssert(e == entry,
								"Replay connId mismatch with IOIndexEntry");
					}
				}
			} else {
				ValeraUtil
						.valeraDebugPrint(String
								.format("ValeraIOManager Warning: connId=%d request %s not found.",
										connId, key));
			}
		}
			break;
		}
	}

	public void recordPlainSocketRead(int connId, byte[] buffer, int offset,
			int byteCount, int nRead, long millisec, IOException exception,
			InputStream in) {
		ValeraUtil
				.valeraAssert(
						connId > 0,
						"Invalid connction ID. connId=" + connId
								+ " calling stack="
								+ ValeraUtil.getCallingStack("\n"));

		synchronized (sLock) {
			try {
				sWriter.writeInt(IO_PLAINSOCKET_READ);
				sWriter.writeLong(System.nanoTime() / 1000000 - ValeraConfig.getConfigLoadTime());
				sWriter.writeInt(connId);
				sWriter.writeLong(millisec);
				if (exception != null) {
					sWriter.writeInt(1);
					sWriter.writeException(exception);
				} else {
					sWriter.writeInt(0);
					sWriter.writeInt(nRead);
					if (nRead > 0) {
						sWriter.writeByteArray(buffer, offset, nRead);
					}
				}

				sWriter.flush();

				ValeraUtil
						.valeraDebugPrint(String
								.format("yhu009: record plainsocket read connId=%d buf.len=%d off=%d nread=%d time=%d millisecs exception=%s in=%s",
										connId, buffer.length, offset, nRead,
										millisec, exception, in.toString()));
			} catch (Exception e) {
				ValeraUtil.valeraAbort(e);
			}
		}
	}

	public int replayPlainSocketRead(int connId, byte[] buffer, int offset,
			int byteCount) throws IOException {
		ValeraUtil
				.valeraAssert(
						connId > 0,
						"Invalid connction ID. connId=" + connId
								+ " calling stack="
								+ ValeraUtil.getCallingStack("\n"));

		int ret = -1;

		// This get from hash map is safe without lock because the conn will not
		// be updated/delete.
		IOIndexEntry index = sConnMap.get(connId);
		// ValeraUtil.valeraAssert(entry != null, "Can not find connId " +
		// connId + " from index file.");
		// FIXME: <connId, entry> not found means the request is not in the log,
		// let it pass!
		if (index == null)
			return CONNECTION_NOT_FOUND;

		ValeraUtil.valeraDebugPrint(String.format(
				"replayPlainSocketRead, connId=%d, uri=%s", connId, index.uri));

		if (index.state == IOIndexEntry.INDEX_STATE_NO_DATA) {
			readIOConnection(index);
		}

		PlainSocketRead psr = (PlainSocketRead) index.responses.poll();
		ValeraUtil.valeraAssert(psr != null,
				"Not enough plain socket read data. uri=" + index.uri);

		IOException exception = null;
		long sleepTime = 0;
		try {
			int tag = psr.tag;
			ValeraUtil.valeraAssert(tag == IO_PLAINSOCKET_READ,
					"TAG mismatch. Should be IO_PLAINSOCKET_READ but it's "
							+ tag + ", uri=" + index.uri);
			sleepTime = psr.time;
			boolean hasException = psr.exception != null;
			if (hasException) {
				exception = (IOException) psr.exception;
			} else {
				int nread = psr.nread;
				byte[] buf = psr.buffer;
				if (nread > 0) {
					ValeraUtil.valeraAssert(nread == buf.length,
							"nread should equal to buf.length here: " + nread
									+ " " + buf.length);
					ValeraUtil
							.valeraAssert(
									byteCount >= buf.length,
									String.format(
											"replay buffer len=%d off=%d cnt=%d is not enough record buffer size=%d",
											buffer.length, offset, byteCount,
											buf.length));
					System.arraycopy(buf, 0, buffer, offset, nread);
				}
				ret = nread;
			}
			index.numResponse--;
			if (index.numResponse == 0) {
				index.releaseData();
				synchronized (sConnMap) {
					sConnMap.remove(connId);
				}
			}
		} catch (Exception e) {
			ValeraUtil.valeraAbort(e);
		}

		try {
			Thread.sleep(sleepTime);
		} catch (Exception e) {
		}

		if (exception != null)
			throw exception;

		return ret;
	}

	public void recordPlainSocketWrite(int connId, byte[] buffer, int offset,
			int byteCount, long millisec, IOException exception, OutputStream out) {
		ValeraUtil
				.valeraAssert(
						connId > 0,
						"Invalid connction ID. connId=" + connId
								+ " calling stack="
								+ ValeraUtil.getCallingStack("\n"));

		synchronized (sLock) {
			try {
				sWriter.writeInt(IO_PLAINSOCKET_WRITE);
				sWriter.writeLong(System.nanoTime() / 1000000 - ValeraConfig.getConfigLoadTime());
				sWriter.writeInt(connId);
				sWriter.writeLong(millisec);
				if (exception != null) {
					sWriter.writeInt(1);
					sWriter.writeException(exception);
				} else {
					sWriter.writeInt(0);
					sWriter.writeByteArray(buffer, offset, byteCount);
				}

				sWriter.flush();

				ValeraUtil
						.valeraDebugPrint(String
								.format("yhu009: record plainsocket write connId=%d buf.len=%d off=%d cnt=%d time=%d millisecs exception=%s out=%s",
										connId, buffer.length, offset,
										byteCount, millisec, exception,
										out.toString()));
			} catch (Exception e) {
				ValeraUtil.valeraAbort(e);
			}
		}
	}

	public int replayPlainSocketWrite(int connId) throws IOException {
		ValeraUtil
				.valeraAssert(
						connId > 0,
						"Invalid connction ID. connId=" + connId
								+ " calling stack="
								+ ValeraUtil.getCallingStack("\n"));

		IOIndexEntry index = sConnMap.get(connId);
		// ValeraUtil.valeraAssert(entry != null, "Can not find connId " +
		// connId + " from index file.");
		// FIXME: <connId, entry> not found means the request is not in the log,
		// let it pass!
		if (index == null)
			return CONNECTION_NOT_FOUND;

		ValeraUtil
				.valeraDebugPrint(String.format(
						"replayPlainSocketWrite, connId=%d, uri=%s", connId,
						index.uri));

		if (index.state == IOIndexEntry.INDEX_STATE_NO_DATA) {
			readIOConnection(index);
		}

		PlainSocketWrite psw = (PlainSocketWrite) index.requests.poll();
		// Number of request data sent maybe differ, let it pass.
		if (psw == null)
			return 0;

		IOException exception = null;
		long sleepTime = 0;
		try {
			int tag = psw.tag;
			ValeraUtil.valeraAssert(tag == IO_PLAINSOCKET_WRITE,
					"TAG mismatch. Should be IO_PLAINSOCKET_WRITE but it's "
							+ tag + ", uri=" + index.uri);
			int recordConnId = psw.connId;
			sleepTime = psw.time;
			boolean hasException = psw.exception != null;
			if (hasException) {
				exception = (IOException) psw.exception;
			} else {
				byte[] buf = psw.buffer;
			}
			index.numRequest--;
			// if (entry.numRequest == 0 && entry.numResponse == 0) {
			// reader.close();
			// synchronized (sConnMap) {
			// sConnMap.remove(connId);
			// }
			// }
		} catch (Exception e) {
			ValeraUtil.valeraAbort(e);
		}

		try {
			Thread.sleep(sleepTime);
		} catch (Exception e) {
		}

		if (exception != null)
			throw exception;

		return 0;
	}

	public void recordSSLSocketRead(int connId, byte[] buffer, int offset,
			int byteCount, int nRead, long millisec, IOException exception,
			InputStream in) {
		ValeraUtil
				.valeraAssert(
						connId > 0,
						"Invalid connction ID. connId=" + connId
								+ " calling stack="
								+ ValeraUtil.getCallingStack("\n"));

		synchronized (sLock) {
			try {
				sWriter.writeInt(IO_SSL_READ);
				sWriter.writeLong(System.nanoTime() / 1000000 - ValeraConfig.getConfigLoadTime());
				sWriter.writeInt(connId);
				sWriter.writeLong(millisec);
				if (exception != null) {
					sWriter.writeInt(1);
					sWriter.writeException(exception);
				} else {
					sWriter.writeInt(0);
					sWriter.writeInt(nRead);
					if (nRead > 0) {
						sWriter.writeByteArray(buffer, offset, nRead);
					}
				}

				sWriter.flush();

				ValeraUtil
						.valeraDebugPrint(String
								.format("yhu009: record sslsocket read connId=%d buf.len=%d off=%d nread=%d time=%d millisecs exception=%s in=%s",
										connId, buffer.length, offset, nRead,
										millisec, exception, in.toString()));
			} catch (Exception e) {
				ValeraUtil.valeraAbort(e);
			}
		}
	}

	public int replaySSLSocketRead(int connId, byte[] buffer, int offset,
			int byteCount) throws IOException {
		ValeraUtil
				.valeraAssert(
						connId > 0,
						"Invalid connction ID. connId=" + connId
								+ " calling stack="
								+ ValeraUtil.getCallingStack("\n"));

		int ret = -1;

		// This get from hash map is safe without lock because the conn will not
		// be updated/delete.
		IOIndexEntry index = sConnMap.get(connId);
		// ValeraUtil.valeraAssert(entry != null, "Can not find connId " +
		// connId + " from index file.");
		// FIXME: <connId, entry> not found means the request is not in the log,
		// let it pass!
		if (index == null)
			return CONNECTION_NOT_FOUND;

		ValeraUtil.valeraDebugPrint(String.format(
				"replaySSLSocketRead, connId=%d, uri=%s", connId, index.uri));

		if (index.state == IOIndexEntry.INDEX_STATE_NO_DATA) {
			readIOConnection(index);
		}

		SSLSocketRead ssr = (SSLSocketRead) index.responses.poll();
		ValeraUtil.valeraAssert(ssr != null,
				"Not enough ssl socket read data. uri=" + index.uri);

		IOException exception = null;
		long sleepTime = 0;
		try {
			int tag = ssr.tag;
			ValeraUtil.valeraAssert(tag == IO_SSL_READ,
					"TAG mismatch. Should be IO_PLAINSOCKET_READ but it's "
							+ tag + ", uri=" + index.uri);
			int recordConnId = ssr.connId;
			sleepTime = ssr.time;
			boolean hasException = ssr.exception != null;
			if (hasException) {
				exception = (IOException) ssr.exception;
			} else {
				int nread = ssr.nread;
				byte[] buf = ssr.buffer;
				if (nread > 0) {
					ValeraUtil.valeraAssert(nread == buf.length,
							"nread should equal to buf.length here: " + nread
									+ " " + buf.length);
					ValeraUtil
							.valeraAssert(
									byteCount >= buf.length,
									String.format(
											"replay buffer len=%d off=%d cnt=%d is not enough record buffer size=%d",
											buffer.length, offset, byteCount,
											buf.length));
					System.arraycopy(buf, 0, buffer, offset, nread);
				}
				ret = nread;
			}
			index.numResponse--;
			if (index.numResponse == 0) {
				index.releaseData();
				synchronized (sConnMap) {
					sConnMap.remove(connId);
				}
			}
		} catch (Exception e) {
			ValeraUtil.valeraAbort(e);
		}

		try {
			Thread.sleep(sleepTime);
		} catch (Exception e) {
		}

		if (exception != null)
			throw exception;

		return ret;
	}

	public void recordSSLSocketWrite(int connId, byte[] buffer, int offset,
			int byteCount, long millisec, IOException exception, OutputStream out) {
		ValeraUtil
				.valeraAssert(
						connId > 0,
						"Invalid connction ID. connId=" + connId
								+ " calling stack="
								+ ValeraUtil.getCallingStack("\n"));

		synchronized (sLock) {
			try {
				sWriter.writeInt(IO_SSL_WRITE);
				sWriter.writeLong(System.nanoTime() / 1000000 - ValeraConfig.getConfigLoadTime());
				sWriter.writeInt(connId);
				sWriter.writeLong(millisec);
				if (exception != null) {
					sWriter.writeInt(1);
					sWriter.writeException(exception);
				} else {
					sWriter.writeInt(0);
					sWriter.writeByteArray(buffer, offset, byteCount);
				}

				sWriter.flush();

				ValeraUtil
						.valeraDebugPrint(String
								.format("yhu009: record sslsocket write connId=%d buf.len=%d off=%d cnt=%d time=%d millisecs exception=%s out=%s",
										connId, buffer.length, offset,
										byteCount, millisec, exception,
										out.toString()));
			} catch (Exception e) {
				ValeraUtil.valeraAbort(e);
			}
		}
	}

	public int replaySSLSocketWrite(int connId) throws IOException {
		ValeraUtil
				.valeraAssert(
						connId > 0,
						"Invalid connction ID. connId=" + connId
								+ " calling stack="
								+ ValeraUtil.getCallingStack("\n"));

		IOIndexEntry index = sConnMap.get(connId);
		// ValeraUtil.valeraAssert(index != null, "Can not find connId " +
		// connId + " from index file.");
		// FIXME: <connId, entry> not found means the request is not in the log,
		// let it pass!
		if (index == null)
			return CONNECTION_NOT_FOUND;

		ValeraUtil.valeraDebugPrint(String.format(
				"replaySSLSocketWrite, connId=%d, uri=%s", connId, index.uri));

		if (index.state == IOIndexEntry.INDEX_STATE_NO_DATA) {
			readIOConnection(index);
		}

		SSLSocketWrite ssw = (SSLSocketWrite) index.requests.poll();
		// Number of request data sent maybe differ, let it pass.
		if (ssw == null)
			return 0;

		IOException exception = null;
		long sleepTime = 0;
		try {
			int tag = ssw.tag;
			ValeraUtil.valeraAssert(tag == IO_SSL_WRITE,
					"TAG mismatch. Should be IO_SSL_WRITE but it's " + tag
							+ ", uri=" + index.uri);
			int recordConnId = ssw.connId;
			sleepTime = ssw.time;
			boolean hasException = ssw.exception != null;
			if (hasException) {
				exception = (IOException) ssw.exception;
			} else {
				byte[] buf = ssw.buffer;
			}
			index.numRequest--;
			// if (entry.numRequest == 0 && entry.numResponse == 0) {
			// reader.close();
			// synchronized (sConnMap) {
			// sConnMap.remove(connId);
			// }
			// }
		} catch (Exception e) {
			ValeraUtil.valeraAbort(e);
		}

		try {
			Thread.sleep(sleepTime);
		} catch (Exception e) {
		}

		if (exception != null)
			throw exception;

		return 0;
	}

}
