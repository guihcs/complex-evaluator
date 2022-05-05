package sparql;


import dataset.DatasetManager;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SparqlProxy {

    public static List<Map<String, RDFNode>> query(String dataset, String query) {
        List<Map<String, RDFNode>> result = new ArrayList<>();

        try (QueryExecution queryExecution = QueryExecution.create(query, DatasetManager.getInstance().get(dataset))) {
            ResultSet resultSet = queryExecution.execSelect();
            resultSet.forEachRemaining(querySolution -> {
                Map<String, RDFNode> stringMap = new HashMap<>();
                for (String resultVar : resultSet.getResultVars()) {
                    stringMap.put(resultVar, querySolution.get(resultVar));
                }
                result.add(stringMap);
            });

        }

        return result;
    }


    public static boolean sendAskQuery(String dataset, String query) {
        try (QueryExecution queryExecution = QueryExecution.create(query, DatasetManager.getInstance().get(dataset))) {
            return queryExecution.execAsk();
        }
    }

}
