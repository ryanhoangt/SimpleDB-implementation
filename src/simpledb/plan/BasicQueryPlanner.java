package simpledb.plan;

import simpledb.metadata.MetaDataMgr;
import simpledb.parse.Parser;
import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

/**
 * The simplest, most naive query planner possible.
 */
public class BasicQueryPlanner implements QueryPlanner {

    private MetaDataMgr metaDataMgr;

    public BasicQueryPlanner(MetaDataMgr metaDataMgr) {
        this.metaDataMgr = metaDataMgr;
    }

    @Override
    public Plan createPlan(QueryData queryData, Transaction tx) {
        // Step 1: Create a plan for each mentioned table or view.
        List<Plan> plans = new ArrayList<>();
        for (String tblName: queryData.getTables()) {
            String viewDef = metaDataMgr.getViewDef(tblName, tx);
            if (viewDef != null) { // Recursively plan the view.
                Parser parser = new Parser(viewDef);
                QueryData viewData = parser.query();
                plans.add(createPlan(viewData, tx));
            } else
                plans.add(new TablePlan(tx, tblName, metaDataMgr));
        }

        // Step 2: Create the product of all table plans.
        Plan p = plans.remove(0);
        for (Plan nextP: plans)
            p = new ProductPlan(p, nextP);

        // Step 3: Add a selection plan for the predicate.
        p = new SelectPlan(p, queryData.getPred());

        // Step 4: Project on the field names.
        return new ProjectPlan(p, queryData.getFields());
    }
}
