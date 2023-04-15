package simpledb.query;

public class ProductScan implements Scan {

    private Scan sc1, sc2;

    public ProductScan(Scan sc1, Scan sc2) {
        this.sc1 = sc1;
        this.sc2 = sc2;
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        sc1.beforeFirst();
        sc1.next();
        sc2.beforeFirst();
    }

    @Override
    public boolean next() {
        if (sc2.next())
            return true;
        else {
            sc2.beforeFirst();
            return sc2.next() && sc1.next();
        }
    }

    @Override
    public int getInt(String fieldName) {
        if (sc1.hasField(fieldName))
            return sc1.getInt(fieldName);
        else
            return sc2.getInt(fieldName);
    }

    @Override
    public String getString(String fieldName) {
        if (sc1.hasField(fieldName))
            return sc1.getString(fieldName);
        else
            return sc2.getString(fieldName);
    }

    @Override
    public Constant getVal(String fieldName) {
        if (sc1.hasField(fieldName))
            return sc1.getVal(fieldName);
        else
            return sc2.getVal(fieldName);
    }

    @Override
    public boolean hasField(String fieldName) {
        return sc1.hasField(fieldName) || sc2.hasField(fieldName);
    }

    @Override
    public void close() {
        sc1.close();
        sc2.close();
    }
}
