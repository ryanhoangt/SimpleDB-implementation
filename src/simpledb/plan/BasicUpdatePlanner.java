package simpledb.plan;

import simpledb.metadata.MetaDataMgr;
import simpledb.parse.*;
import simpledb.query.Constant;
import simpledb.query.UpdateScan;
import simpledb.tx.Transaction;

import java.util.Iterator;

/**
 * The basic planner for SQL update statements.
 */
public class BasicUpdatePlanner implements UpdatePlanner {

    private MetaDataMgr metaDataMgr;

    public BasicUpdatePlanner(MetaDataMgr metaDataMgr) {
        this.metaDataMgr = metaDataMgr;
    }

    @Override
    public int executeInsert(InsertData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.getTblName(), metaDataMgr);
        UpdateScan us = (UpdateScan) p.open();
        us.insert();
        Iterator<Constant> iter = data.getVals().iterator();
        for (String fldName: data.getFlds()) {
            Constant val = iter.next();
            us.setVal(fldName, val);
        }
        us.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.getTblName(), metaDataMgr);
        p = new SelectPlan(p, data.getPred());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.next()) {
            us.delete();
            count++;
        }
        us.close();
        return count;
    }

    @Override
    public int executeModify(ModifyData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.getTblName(), metaDataMgr);
        p = new SelectPlan(p, data.getPred());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.next()) {
            Constant val = data.getNewVal().evaluate(us);
            us.setVal(data.getFldName(), val);
            count++;
        }
        us.close();
        return count;
    }

    @Override
    public int executeCreateTable(CreateTableData data, Transaction tx) {
        metaDataMgr.createTable(data.getTblName(), data.getSchema(), tx);
        return 0;
    }

    @Override
    public int executeCreateView(CreateViewData data, Transaction tx) {
        metaDataMgr.createView(data.getViewName(), data.getViewDef(), tx);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndexData data, Transaction tx) {
        metaDataMgr.createIndex(data.getIdxName(), data.getTblName(), data.getFldName(), tx);
        return 0;
    }
}
