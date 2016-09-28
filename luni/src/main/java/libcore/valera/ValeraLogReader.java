package libcore.valera;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.lang.reflect.Constructor;

public class ValeraLogReader {
	// The size of each primitives in byte.
	public final static byte SIZE_BYTE = 1;
	public final static byte SIZE_SHORT = 2;
	public final static byte SIZE_INT = 4;
	public final static byte SIZE_LONG = 8;
	public final static byte SIZE_FLOAT = 4;
	public final static byte SIZE_DOUBLE = 8;
	public final static byte SIZE_BOOLEAN = 1;
	public final static byte SIZE_CHAR = 2;
	
	private final static int TYPE_BYTEARRAY  = 100;
	private final static int TYPE_STRING 	 = 101;
	private final static int TYPE_EXCEPTION  = 102;
	private final static int TYPE_SHORTARRAY = 103;

	//private BufferedInputStream mReader;
	private ObjectInputStream mReader;
	//private byte[] bytes;

	public ValeraLogReader(FileInputStream fis) throws StreamCorruptedException, IOException {
		//mReader = new BufferedInputStream(fis);
		mReader = new ObjectInputStream(new BufferedInputStream(fis));
		//bytes = new byte[10];
	}

	public int readInt() throws IOException, EOFException {
		return mReader.readInt();
		//int n = mReader.read(bytes, 0, SIZE_INT);
		//if (n == -1)
		//	throw new IOException("End of stream reached.");
		//assert n == SIZE_INT;
		//return ByteBuffer.wrap(bytes, 0, SIZE_INT).getInt();
	}
	
	public long readLong() throws IOException, EOFException {
		return mReader.readLong();
		//int n = mReader.read(bytes, 0, SIZE_LONG);
		//if (n == -1)
		//	throw new IOException("End of stream reached.");
		//assert n == SIZE_LONG;
		//return ByteBuffer.wrap(bytes, 0, SIZE_LONG).getLong();
	}

	public float readFloat() throws IOException, EOFException {
		return mReader.readFloat();
		//int n = mReader.read(bytes, 0, SIZE_FLOAT);
		//if (n == -1)
		//	throw new IOException("End of stream reached.");
		//assert n == SIZE_FLOAT;
		//return ByteBuffer.wrap(bytes, 0, SIZE_FLOAT).getFloat();
	}
	
	public double readDouble() throws IOException, EOFException {
		return mReader.readDouble();
	}
	
	/*
	public Object readObject() throws OptionalDataException, ClassNotFoundException, IOException {
		return mReader.readObject();
	}
	*/
	
	public String readString() throws IOException, ClassNotFoundException {
		int tag = mReader.readInt();
		ValeraUtil.valeraAssert(tag == TYPE_STRING, "Read String failed. Type mismatch " + tag);
		return (String) mReader.readObject();
	}
	
	public Exception readException() throws IOException, ClassNotFoundException {
		int tag = mReader.readInt();
		ValeraUtil.valeraAssert(tag == TYPE_EXCEPTION, "Read Exception failed. Type mismatch " + tag);
		return (Exception) mReader.readObject();
		/*
		String className = readString();
		String msg = readString();
		Exception exception = null;
		try {
			Class clazz = Class.forName(className);
			Constructor ctor = clazz.getConstructor(new Class[]{String.class});
			exception = (Exception) ctor.newInstance(msg);
		} catch (Exception e) {
			ValeraUtil.valeraAbort(e);
		}
		return exception;
		*/
	}
	
	public byte[] readByteArray() throws IOException {
		int tag = mReader.readInt();
		ValeraUtil.valeraAssert(tag == TYPE_BYTEARRAY, "Read byte array failed. Type mismatch " + tag);
		int checkNull = mReader.readInt();
		if (checkNull == 0) {
			return null;
		} else {
			int len = mReader.readInt();
			byte[] buffer = new byte[len];
			mReader.readFully(buffer);
			return buffer;
		}
	}
	
	public short[] readShortArray() throws IOException {
		int tag = mReader.readInt();
		ValeraUtil.valeraAssert(tag == TYPE_SHORTARRAY, "Read short array failed. Type mismatch "+ tag);
		int checkNull = mReader.readInt();
		if (checkNull == 0) {
			return null;
		} else {
			int len = mReader.readInt();
			short[] buffer = new short[len];
			for (int i = 0; i < len; i++) {
				buffer[i] = mReader.readShort();
			}
			return buffer;
		}
	}
	
	public void close() throws IOException {
		mReader.close();
	}
}
