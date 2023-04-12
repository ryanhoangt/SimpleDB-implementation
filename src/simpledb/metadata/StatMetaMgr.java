package simpledb.metadata;

import simpledb.record.Layout;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.util.HashMap;
import java.util.Map;

class StatMetaMgr {

    private TableMetaMgr tblMetaMgr;
    private Map<String, StatInfo> statsMap;
    private int numCalls;

    public StatMetaMgr(TableMetaMgr tblMetaMgr, Transaction tx) {
        this.tblMetaMgr = tblMetaMgr;
        refreshStatistics(tx);
    }

    public synchronized StatInfo getStatInfo(String tblName, Layout layout, Transaction tx) {
        numCalls++;
        if (numCalls > 100)
            refreshStatistics(tx);
        StatInfo si = statsMap.get(tblName);
        if (si == null) {
            si = calcTableStats(tblName, layout, tx);
            statsMap.put(tblName, si);
        }
        return si;
    }

    private synchronized void refreshStatistics(Transaction tx) {
        statsMap = new HashMap<>();
        numCalls = 0;
        Layout tCatLayout = tblMetaMgr.getLayout("tbl_cat", tx);
        TableScan tScan = new TableScan(tx, "tbl_cat", tCatLayout);

        while (tScan.next()) {
            String tblName = tScan.getString("tbl_name");
            Layout layout = tblMetaMgr.getLayout(tblName, tx);
            StatInfo si = calcTableStats(tblName, layout, tx);
            statsMap.put(tblName, si);
        }
        tScan.close();
    }

    private synchronized StatInfo calcTableStats(String tblName, Layout layout, Transaction tx) {
        int numRecs = 0;
        int numBlocks = 0;
        TableScan ts = new TableScan(tx, tblName, layout);

        while (ts.next()) {
            numRecs++;
            numBlocks = ts.getRID().getBlkNum() + 1;
        }
        ts.close();
        return new StatInfo(numBlocks, numRecs);
    }
}
