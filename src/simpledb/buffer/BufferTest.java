package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.Page;
import simpledb.server.SimpleDB;

public class BufferTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("buffertestdir", 400, 3);
        BufferMgr bm = db.bufferMgr();

        Buffer buffer1 = bm.pin(new BlockId("testfile", 1));
        Page p = buffer1.contents();
        int n = p.getInt(80);
        p.setInt(80, n+1);
        buffer1.setModified(1, 0); // placeholder values
        System.out.println("The new value is " + (n+1));
        bm.unpin(buffer1);

        // One of these pins will flush buff1 to disk:
        Buffer buffer2 = bm.pin(new BlockId("testfile", 2));
        Buffer buffer3 = bm.pin(new BlockId("testfile", 3));
        Buffer buffer4 = bm.pin(new BlockId("testfile", 4));

        bm.unpin(buffer2);
        buffer2 = bm.pin(new BlockId("testfile", 1));
        Page p2 = buffer2.contents();
        p2.setInt(80, 9999); // this modification won't get written to disk
        buffer2.setModified(1, 0);
    }

}
