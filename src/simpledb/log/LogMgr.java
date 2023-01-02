package simpledb.log;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;

import java.util.Iterator;

public class LogMgr {
    private FileMgr fm;
    private String logfileName;
    private Page curLogPage;
    private BlockId currentBlk;
    private int latestLSN = 0;
    private int lastFlushedLSN = 0;

    public LogMgr(FileMgr fm, String logfileName) {
        this.fm = fm;
        this.logfileName = logfileName;
        // create new log page
        byte[] pageBytes = new byte[fm.blockSize()];
        this.curLogPage = new Page(pageBytes);

        int logSizeInBlocks = fm.lengthInBlocks(logfileName);
        if (logSizeInBlocks == 0) {
            this.currentBlk = appendNewBlock();
        } else { // load latest block to memory page
            this.currentBlk = new BlockId(logfileName, logSizeInBlocks-1);
            fm.read(currentBlk, curLogPage);
        }
    }

    private BlockId appendNewBlock() {
        BlockId blk = fm.append(logfileName); // create new empty block in logfile
        curLogPage.setInt(0, fm.blockSize());
        fm.write(blk, curLogPage);
        return blk;
    }

    /**
     * Appends a log record to the log buffer.
     * Log records are written right to left in the buffer.
     * The size of the record is written before the bytes.
     * The beginning of the buffer contains the location
     * of the last-written record (the "boundary").
     * Storing the records backwards makes it easy to read
     * them in reverse order.
     * @param rec record's data
     * @return the log sequence number of new log record
     */
    public synchronized int append(byte[] rec) {
        int boundary = curLogPage.getInt(0);
        int recordSize = rec.length;
        int bytesNeeded = recordSize + Integer.BYTES;

        if (boundary - bytesNeeded - Integer.BYTES < 0) { // log record doesn't fit
            flush();
            currentBlk = appendNewBlock();
            boundary = curLogPage.getInt(0);
        }

        int recPos = boundary - bytesNeeded;
        curLogPage.setBytes(recPos, rec);
        curLogPage.setInt(0, recPos); // update new boundary
        latestLSN += 1;
        return latestLSN;
    }

    // write the current buffer to log file
    private void flush() {
        // For debugging flush method
//        System.out.println("Flushing: " + currentBlk + curLogPage);
        fm.write(currentBlk, curLogPage);
        lastFlushedLSN = latestLSN;
    }

    public void flush(int lsn) {
        if (lsn >= lastFlushedLSN) {
            flush();
        }
    }

    public Iterator<byte[]> iterator() {
        flush();
        return new LogIterator(fm, currentBlk);
    }
}
