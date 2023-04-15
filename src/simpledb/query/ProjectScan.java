package simpledb.query;

import java.util.List;

public class ProjectScan implements Scan {

    private Scan sc;
    private List<String> fieldList;

    public ProjectScan(Scan sc, List<String> fieldList) {
        this.sc = sc;
        this.fieldList = fieldList;
    }

    @Override
    public void beforeFirst() {
        sc.beforeFirst();
    }

    @Override
    public boolean next() {
        return sc.next();
    }

    @Override
    public int getInt(String fieldName) {
        if (hasField(fieldName))
            return sc.getInt(fieldName);

        throw new RuntimeException("Field not found.");
    }

    @Override
    public String getString(String fieldName) {
        if (hasField(fieldName))
            return sc.getString(fieldName);

        throw new RuntimeException("Field not found.");
    }

    @Override
    public Constant getVal(String fieldName) {
        if (hasField(fieldName))
            return sc.getVal(fieldName);

        throw new RuntimeException("Field not found.");
    }

    @Override
    public boolean hasField(String fieldName) {
        return fieldList.contains(fieldName);
    }

    @Override
    public void close() {
        sc.close();
    }
}
