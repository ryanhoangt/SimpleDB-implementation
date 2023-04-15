package simpledb.query;

import simpledb.record.RID;

public class SelectScan implements UpdateScan {
    private Scan sc;
    private Predicate pred;

    public SelectScan(Scan s, Predicate pred) {
        this.sc = s;
        this.pred = pred;
    }

    // ===== Scan Methods =====
    @Override
    public void beforeFirst() {
        sc.beforeFirst();
    }

    @Override
    public boolean next() {
        while (sc.next()) {
            if (pred.isSatisfied(sc))
                return true;
        }
        return false;
    }

    @Override
    public int getInt(String fieldName) {
        return sc.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        return sc.getString(fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        return sc.getVal(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return sc.hasField(fieldName);
    }

    @Override
    public void close() {
        sc.close();
    }

    // ===== UpdateScan Methods =====
    @Override
    public void setVal(String fieldName, Constant val) {
        ((UpdateScan) sc).setVal(fieldName, val);
    }

    @Override
    public void setInt(String fieldName, int val) {
        ((UpdateScan) sc).setInt(fieldName, val);
    }

    @Override
    public void setString(String fieldName, String val) {
        ((UpdateScan) sc).setString(fieldName, val);
    }

    @Override
    public void insert() {
        ((UpdateScan) sc).insert();
    }

    @Override
    public void delete() {
        ((UpdateScan) sc).delete();
    }

    @Override
    public RID getRID() {
        return ((UpdateScan) sc).getRID();
    }

    @Override
    public void moveToRID(RID rid) {
        ((UpdateScan) sc).moveToRID(rid);
    }
}
