package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.Iterator;

class LogIterator implements Iterator<byte[]> {
    private FileMgr fm;
    private BlockId curBlockId;
    private Page curPage;
    private int boundary;
    private int currentPos;

    public LogIterator(FileMgr fm, BlockId curBlockId) {
        this.fm = fm;
        this.curBlockId = curBlockId;

        byte[] bytes = new byte[fm.blockSize()];
        this.curPage = new Page(bytes);
        moveToBlock(curBlockId);
    }

    /**
     * Moves to the specified log block
     * and positions it at the first record in that block
     * (i.e., the most recent one).
     */
    private void moveToBlock(BlockId curBlockId) {
        fm.read(curBlockId, curPage);
        this.boundary = curPage.getInt(0);
        this.currentPos = boundary;
    }

    @Override
    /**
     * Determines if the current log record
     * is the earliest record in the log file.
     * @return true if there is an earlier record
     */
    public boolean hasNext() {
        return currentPos < fm.blockSize() || curBlockId.number() > 0;
    }

    @Override
    /**
     * Moves to the next log record in the block.
     * If there are no more log records in the block,
     * then move to the previous block
     * and return the log record from there.
     * @return the next earliest log record
     */
    public byte[] next() {
        if (currentPos == fm.blockSize()) {
            curBlockId = new BlockId(curBlockId.fileName(), curBlockId.number()-1);
            moveToBlock(curBlockId);
        }
        byte[] rec = curPage.getBytes(currentPos);
        currentPos += rec.length + Integer.BYTES;
        return rec;
    }
}
