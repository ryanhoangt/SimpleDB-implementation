package simpledb.tx;

import simpledb.buffer.BufferMgr;
import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;

public class Transaction {

    private FileMgr fm;
    private LogMgr lm;
    private BufferMgr bm;

    public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
        this.fm = fm;
        this.lm = lm;
        this.bm = bm;
    }

    /* lifespan related methods */
    public void commit() {}

    public void rollback() {}

    public void recover() {}

    /* methods to access buffer */
    public void pin(BlockId blkId) {}

    public void unpin(BlockId blkId) {}

    public int getInt(BlockId blkId, int offset) {
        return -1;
    }

    public String getString(BlockId blkId, int offset) {
        return "";
    }

    public void setInt(BlockId blkId, int offset, int val, boolean okToLog) {}

    public void setString(BlockId blkId, int offset, String val, boolean okToLog) {}

    public int availableBuffs() {
        return -1;
    }

    /* methods related to file manager */
    public int size(String filename) {
        return -1;
    }

    public BlockId append(String filename) {
        return null;
    }

    public int blockSize() {
        return fm.blockSize();
    }
}
