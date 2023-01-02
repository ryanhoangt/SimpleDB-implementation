package simpledb.file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Page {
    private ByteBuffer bb;
    public static Charset CHARSET = StandardCharsets.US_ASCII; // one byte per char

    // For creating data buffers
    public Page(int blocksize) {
        bb = ByteBuffer.allocateDirect(blocksize);
    }

    // For creating log pages
    public Page(byte[] b) {
        bb = ByteBuffer.wrap(b);
    }

    public void setInt(int offset, int n) {
        bb.putInt(offset, n);
    }

    public int getInt(int offset) {
        return bb.getInt(offset);
    }

    /**
     * Write byte array to page: length first, then data.
     * Require [b.length + Integer.BYTES] bytes.
     */
    public void setBytes(int offset, byte[] b) {
        bb.position(offset);
        bb.putInt(b.length); // length of byte array
        bb.put(b); // data
    }

    public byte[] getBytes(int offset) {
        bb.position(offset);
        int len = bb.getInt();
        byte[] b = new byte[len];
        bb.get(b, 0, len);
        return b;
    }

    public void setString(int offset, String s) {
        byte[] b = s.getBytes(CHARSET);
        setBytes(offset, b);
    }

    public String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    // utility method to determine max number of bytes required
    // to store strlen characters and one beginning number, using
    // CHARSET
    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int) bytesPerChar);
    }

    // a package-private method, needed by FileMgr
    ByteBuffer contents() {
        bb.position(0);
        return bb;
    }

}
