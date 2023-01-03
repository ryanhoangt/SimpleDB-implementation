package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.server.SimpleDB;

public class BufferMgrTest {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("buffermgrtestdir", 400, 3);
        BufferMgr bm = db.bufferMgr();

        Buffer[] buffers = new Buffer[6];
        buffers[0] = bm.pin(new BlockId("testfile", 0));
        buffers[1] = bm.pin(new BlockId("testfile", 1));
        buffers[2] = bm.pin(new BlockId("testfile", 2));

        bm.unpin(buffers[1]);
        buffers[1] = null;

        buffers[3] = bm.pin(new BlockId("testfile", 0)); // block 0 pinned twice
        buffers[4] = bm.pin(new BlockId("testfile", 1)); // block 1 repinned
        System.out.println("Available buffers: " + bm.available());

        try {
            System.out.println("Attempting to pin block 3...");
            buffers[5] = bm.pin(new BlockId("testfile", 3)); // won't work, no buffers left
        } catch (NoBufferAvailableException ex) {
            System.out.println("Exception: No available buffers\n");
        }

        bm.unpin(buffers[2]);
        buffers[2] = null;
        System.out.println("Final buffer pin to blk 3");
        buffers[5] = bm.pin(new BlockId("testfile", 3));

        for (int i = 0; i < buffers.length; i++) {
            Buffer buffer = buffers[i];
            if (buffer != null)
                System.out.println("buff[" + i + "] pinned to block " + buffer.block());
        }
    }

}
