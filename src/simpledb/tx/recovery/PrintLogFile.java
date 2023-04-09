package simpledb.tx.recovery;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.file.Page;
import simpledb.log.LogMgr;
import simpledb.server.SimpleDB;

import java.util.Iterator;

/**
 * An utility class to examine log files' content.
 */
public class PrintLogFile {

    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("recoverytestdir", 400, 8);
        FileMgr fm = db.fileMgr();
        LogMgr lm = db.logMgr();
        String filename = "simpledb.log";

        int lastBlkIdx = fm.lengthInBlocks(filename) - 1;
        BlockId blockId = new BlockId(filename, lastBlkIdx);
        Page p = new Page(fm.blockSize());
        fm.read(blockId, p);
        Iterator<byte[]> iter = lm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.reconstructLogRecord(bytes);
            System.out.println(rec);
        }
    }
}
