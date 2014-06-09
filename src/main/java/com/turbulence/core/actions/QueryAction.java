package com.turbulence.core.actions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.String;
import java.util.*;

import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.WebApplicationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.turbulence.core.ClusterSpaceJenaGraph;
import com.turbulence.core.TurbulenceDriver;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ComparatorType;

public class QueryAction implements Action {
    private static final Log logger = LogFactory.getLog(QueryAction.class);

    private final String query;
    /**
     * Constructs a new instance.
     */
    public QueryAction(String query)
    {
        this.query = query;
        
    }

	public StreamingOutput stream() {
        Query q = QueryFactory.create(query);
        
          


        if (q.getQueryType() != Query.QueryTypeSelect) {
            final String message = "<result><message>Cannot handle query " + query
                + ". Only SELECT supported.</message><error>"
                + TurbulenceError.QUERY_PARSE_FAILED + "</error></result>";
            return new StreamingOutput() {
                public void write(OutputStream out) throws IOException, WebApplicationException {
                    out.write(message.getBytes());
                }
            };
        }

        ClusterSpaceJenaGraph.queryMapTime = 0;
        Model model = ModelFactory.createModelForGraph(new ClusterSpaceJenaGraph());
        
      
            
                


      
        QueryExecution exec = QueryExecutionFactory.create(q, model);
        // TODO: uncomment once debugging is done
        //if (q.getResultVars().isEmpty())
        //    return null;

        final ResultSet resultSet = exec.execSelect();
       
        return new StreamingOutput() {
            public void write(OutputStream out) throws IOException, WebApplicationException {
                Set<String> rowKeys = new HashSet<String>();

                while (resultSet.hasNext()) {
                    QuerySolution row = resultSet.next();
                    
                    for (String var : resultSet.getResultVars()) {
                        try {
                            rowKeys.add(row.getResource(var).getURI());
                        } catch (ClassCastException e) {
                            rowKeys.add(row.getLiteral(var).getString());
                        }
                    }
                }
                File f1 = new File("/Users/Deepu/Documents/maven/turbulence/qm_time.txt");
                if (!f1.exists())
                    f1.createNewFile();
                BufferedWriter out2 = new BufferedWriter(new FileWriter(f1));
                out2.write("ClusterSpace operations took " + (ClusterSpaceJenaGraph.queryMapTime) + " ms");
                out2.newLine();
                out2.flush();
                out2.close();
                logger.warn("ClusterSpace operations took " + (ClusterSpaceJenaGraph.queryMapTime) + " ms");
                System.out.println("ClusterSpace operations took " + (ClusterSpaceJenaGraph.queryMapTime) + " ms");
                logger.warn("RESULT set size " + rowKeys.size());
             // check IOException in method signature
                File f = new File("/Users/Deepu/Documents/maven/turbulence/keys1.txt");
                if(f.exists())
                {
                	f.delete();
                	f.createNewFile();
                }
                if (!f.getParentFile().exists())
                    f.getParentFile().mkdirs();
                if (!f.exists())
                    f.createNewFile();

                BufferedWriter out1 = new BufferedWriter(new FileWriter(f));
                Iterator it = rowKeys.iterator(); // why capital "M"?
                while(it.hasNext()) {
                	//.warn(it.toString());
                    out1.write(it.next().toString());
                    out1.newLine();
                    out1.flush();
                }
                out1.close();
                // TODO: batch this
               for (String rowKey : rowKeys) {
                    HColumn<String, String> col = TurbulenceDriver.getInstanceDataTemplate().querySingleColumn(rowKey, "data", StringSerializer.get());
                    if (col != null && col.getValue() != null)
                        out.write(col.getValue().getBytes());
                }
            }
        };
	}

	public Result perform() {
	    throw new UnsupportedOperationException();
    }
}
