package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;

public class Buffer {
    private FileMgr fm;
    private LogMgr lm;
    private Page contents;
    private BlockId curBlkId = null;
    private int pins = 0;
    private int txnum = -1;
    private int lsn = -1;

    public Buffer(FileMgr fm, LogMgr lm) {
        this.fm = fm;
        this.lm = lm;
        this.contents = new Page(fm.blockSize());
    }

    public Page contents() {
        return contents;
    }

    public BlockId block() {
        return curBlkId;
    }

    public void setModified(int txnum, int lsn) {
        this.txnum = txnum;
        if (lsn >= 0) {
            this.lsn = lsn;
        }
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTx() {
        return txnum;
    }

    //=== package-private methods ===
    /**
     * Write the buffer to its disk block if it is dirty.
     */
    void flush() {
        if (txnum >= 0) {
            lm.flush(lsn);
            fm.write(curBlkId, contents);
            txnum = -1;
        }
    }

    void unpin() {
        pins--;
    }

    void pin() {
        pins++;
    }

    /**
     * Reads the contents of the specified block into
     * the contents of the buffer.
     * If the buffer was dirty, then its previous contents
     * are first written to disk.
     */
    void assignToBlock(BlockId newBlkId) {
        flush();
        curBlkId = newBlkId;
        fm.read(curBlkId, contents);
        txnum = -1;
    }
}
