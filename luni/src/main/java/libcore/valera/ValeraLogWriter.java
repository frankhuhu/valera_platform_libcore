package libcore.valera;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class ValeraLogWriter {
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

	private ObjectOutputStream mWriter;
	//private BufferedOutputStream mWriter;
	//private byte[] bytes;

	public ValeraLogWriter(FileOutputStream fos) throws IOException {
		//mWriter = new BufferedOutputStream(fos);
		mWriter = new ObjectOutputStream(new BufferedOutputStream(fos));
		//bytes = new byte[10];
	}

	public void writeInt(int val) throws IOException {
		mWriter.writeInt(val);
		//ByteBuffer.wrap(bytes).putInt(val);
		//mWriter.write(bytes, 0, SIZE_INT);
	}
	
	public void writeLong(long val) throws IOException {
		mWriter.writeLong(val);
		//ByteBuffer.wrap(bytes).putLong(val);
		//mWriter.write(bytes, 0, SIZE_LONG);
	}

	public void writeFloat(float val) throws IOException {
		mWriter.writeFloat(val);
		//ByteBuffer.wrap(bytes).putFloat(val);
		//mWriter.write(bytes, 0, SIZE_FLOAT);
	}
	
	public void writeDouble(double val) throws IOException {
		mWriter.writeDouble(val);
	}
	
	/*
	public void writeObject(Object obj) throws IOException {
		mWriter.writeObject(obj);
	}
	*/
	
	public void writeString(String str) throws IOException {
		mWriter.writeInt(TYPE_STRING);
		mWriter.writeObject(str);
	}
	
	public void writeException(Exception e) throws IOException {
		mWriter.writeInt(TYPE_EXCEPTION);
		mWriter.writeObject(e);
		/*
		String clazz = e.getClass().getName();
		writeString(clazz);
		String msg = e.getMessage();
		writeString(msg);
		*/
	}
	
	public void writeByteArray(byte[] array, int off, int len) throws IOException {
		mWriter.writeInt(TYPE_BYTEARRAY);
		if (array == null) {
			mWriter.writeInt(0);
		} else {
			mWriter.writeInt(1);
			mWriter.writeInt(len);
			mWriter.write(array, off, len);
		}
	}
	
	public void writeShortArray(short[] array, int off, int len) throws IOException {
		mWriter.writeInt(TYPE_SHORTARRAY);
		if (array == null) {
			mWriter.writeInt(0);
		} else {
			mWriter.writeInt(1);
			mWriter.writeInt(len - off);
			for (int i = off; i < len; i++)
				mWriter.writeShort(array[i]);
		}
	}
	
	public void flush() throws IOException {
		mWriter.flush();
	}
	
	public void close() throws IOException {
		mWriter.close();
	}

}
