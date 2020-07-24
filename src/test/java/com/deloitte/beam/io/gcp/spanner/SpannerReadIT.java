package com.deloitte.beam.io.gcp.spanner;

import java.util.Map;
import java.util.Set;

import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;

@RunWith(JUnit4.class)
public class SpannerReadIT {
    private static final int MAX_DB_NAME_LENGTH = 30;

    @Rule
    public final transient TestPipeline p = TestPipeline.create();

    /**
     * Pipeline options for this test.
     */
    public interface SpannerTestPipelineOptions extends TestPipelineOptions {
        @Description("Instance ID to Read from")
        @Default.String("beam-test")
        String getInstanceId();

        void setInstanceId(String value);

        @Description("Database ID prefix to Read from in Spanner")
        @Default.String("beam-testdb")
        String getDatabaseIdPrefix();

        void setDatabaseIdPrefix(String value);

        @Description("Table name")
        @Default.String("users")
        String getTable();

        void setTable(String value);
    }

    private Spanner spanner;

    private DatabaseAdminClient databaseAdminClient;

    private SpannerReadIT.SpannerTestPipelineOptions options;

    private String databaseName;

    private String project;

    @Before
    public void init() {
    	ProcessBuilder pb = new ProcessBuilder("/bin/sh"); // or any other program you want to run

        Map<String, String> envMap = pb.environment();

        envMap.put("GOOGLE_APPLICATION_CREDENTIALS", "/Users/marcrisney/Projects/Misc/pipeline-samples/deloitte-beam-b6b4f525fdaf.json");
        Set<String> keys = envMap.keySet();
        for(String key:keys){
            System.out.println(key+" ==> "+envMap.get(key));
        }
    }
    
    @Test
    public void testRead() throws Exception {
    	 
    	String projectId = "deloitte-beam-284202";
    	String instanceId = "test-instance";
        String databaseId = "test-db";
       
        SpannerConfig spannerConfig = SpannerConfig.create()
        		.withProjectId(projectId)
                .withInstanceId(instanceId)
                .withDatabaseId(databaseId);
        
        PCollectionView<Transaction> tx = p.apply(SpannerIO
        		.createTransaction()
        		.withSpannerConfig(spannerConfig)
        		.withTimestampBound(TimestampBound.strong()));
        PCollection<Struct> output = p.apply(SpannerIO.read()
        		.withSpannerConfig(spannerConfig)
        		.withTable("users")
        		.withColumns("name", "email")
        		.withTransaction(tx));
        
        PAssert.thatSingleton(output
        		.apply("Count rows", Count.<Struct>globally())).isEqualTo(1L);
        p.run();
    }
}