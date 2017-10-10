package fr.askjadev.xml.marklogic.query;

import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory.BasicAuthContext;
import com.marklogic.client.datamovement.QueryBatcher;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.client.query.StructuredQueryDefinition;
import javax.xml.namespace.QName;

public class SimpleElementTextQuery {
    
    // MarkLogic Server connection information
    private final String HOST;
    private final int PORT;
    private final String USER;
    private final String PASSWORD;
    private final String DOC_DIR;
    private final String ELT_NAME;
    private final String ELT_NS;
    private final String SEARCH_QUERY;
    private DatabaseClient client;
    
    public SimpleElementTextQuery(String host, String port, String user, String password, String docDir, String eltName, String eltNs, String searchQuery) {
        super();
        this.HOST = host;
        this.PORT = Integer.parseInt(port);
        this.USER = user;
        this.PASSWORD = password;
        this.DOC_DIR = docDir;
        this.ELT_NAME = eltName;
        this.ELT_NS = eltNs;
        this.SEARCH_QUERY = searchQuery;
    }
    
    // Connection
    private void connect() {
        this.client = DatabaseClientFactory.newClient(HOST, PORT, new BasicAuthContext(USER, PASSWORD));
    }
    
    // Query
    private void query() {
        // Construct the query
        QueryManager queryMgr = client.newQueryManager();
        //StringQueryDefinition query = queryMgr.newStringDefinition();
        //query.setDirectory(DOC_DIR);
        //query.setCriteria(SEARCH_QUERY);
        StructuredQueryBuilder builder = queryMgr.newStructuredQueryBuilder();
        StructuredQueryDefinition query = builder.and(
            builder.directory(true, DOC_DIR),
            builder.containerQuery(builder.element(new QName(ELT_NS, ELT_NAME)), builder.term(SEARCH_QUERY))
        );
        // Create and configure the batcher
        DataMovementManager dmm = client.newDataMovementManager();
        QueryBatcher batcher = dmm.newQueryBatcher(query);
        batcher
            .onUrisReady( batch -> {
                for (String uri : batch.getItems()) {
                    System.out.println(uri);
                }
            })
            .onQueryFailure( exception -> exception.printStackTrace() );
        
        // Start the job
        dmm.startJob(batcher);
        
        // Wait for the job to complete, and then stop it.
        batcher.awaitCompletion();
        dmm.stopJob(batcher);
    
    }

    // Main
    public static void main(String[] args) {
        SimpleElementTextQuery eltQuery = new SimpleElementTextQuery(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        eltQuery.connect();
        eltQuery.query();
    }
    
}
