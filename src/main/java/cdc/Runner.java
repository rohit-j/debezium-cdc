package debezium;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.format.Json;
//import io.debezium.embedded.EmbeddedEngine;
// import io.debezium.config.Configuration;
// import io.debezium.config.CommonConnectorConfig;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.FileInputStream;
import java.io.IOException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.kafka.connect.source.SourceRecord;
import com.fasterxml.jackson.databind.ObjectMapper;


import io.debezium.embedded.Connect;
import io.debezium.storage.file.history.FileSchemaHistory;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.data.Struct;
import static io.debezium.data.Envelope.Operation;
import org.apache.kafka.connect.data.Field;
import org.apache.commons.lang3.tuple.Pair;
import static io.debezium.data.Envelope.FieldName.*;
import java.util.*;

import static java.util.stream.Collectors.toMap;

// import org.apache.iceberg.CatalogProperties;
// import org.apache.iceberg.Table;
// // import org.apache.iceberg.aws.s3.S3FileIOProperties;
// import org.apache.iceberg.catalog.Namespace;
// import org.apache.iceberg.catalog.TableIdentifier;
// import org.apache.iceberg.data.IcebergGenerics;
// import org.apache.iceberg.data.Record;
// import org.apache.iceberg.rest.RESTCatalog;
// import org.apache.iceberg.rest.auth.OAuth2Properties;
// import org.apache.iceberg.types.Types;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Runner {
    private static Logger logger = Logger.getLogger(Runner.class.getName());
    private Map<String, Map<String, Object>> tableEvents = new HashMap<String, Map<String, Object>> ();
    private final int EVENT_QUEUE_THRESHOLD_SIZE = 100;
    private final int EVENT_DISPATCH_THRESHOLD_TIME = 1000; 


    public static void main (String args[]) throws Exception{
        if(args.length != 2) {
            logger.info("Give src and trg properties filepaths.");
            System.exit(0);
        }
        Properties srcProps = new Properties();
        Properties trgProps = new Properties();
        try{
            srcProps.load(new FileInputStream(args[0]));
            trgProps.load(new FileInputStream(args[1]));
        }catch (IOException e) { 
            e.printStackTrace(); 
            throw e;
        }

        captureEvents(srcProps, trgProps);
    }

    private static Map<String, Object> mapFromStruct(Struct struct_record) {
        return struct_record.schema().fields().stream()
                .map(Field::name)
                .filter(fieldName -> struct_record.get(fieldName) != null)
                .map(fieldName -> Pair.of(fieldName, struct_record.get(fieldName)))
                .collect(toMap(Pair::getKey, Pair::getValue));
    }

    public static void captureEvents(Properties srcConnector, Properties trgConnector) throws Exception {
        String trgFormat = trgConnector.getProperty("format");
        logger.info("\n\nCreating DebeziumEngine.");
        try {
            // RESTCatalog catalog = null;
            // if(trgFormat != null && trgFormat.equals("iceberg")){
            //     Map<String, String> ibCatalog = new HashMap<>();
            //     for(Map.Entry<Object,Object> entry: trgConnector.entrySet()){
            //         ibCatalog.put((String) entry.getKey(), (String) entry.getValue());
            //     }
            //     catalog = new RESTCatalog();
            //     String catalogName = trgConnector.getProperty("catalogName")!=null ? trgConnector.getProperty("catalogName") : "Test";
            //     logger.info("\n\nInitializing Iceberg Catalog.");
            //     catalog.initialize(catalogName, ibCatalog);
            //     logger.info(catalog.listNamespaces().toString());
            // }
            //
            System.out.println("\nUsing following src properties to initiate Debezium: "+srcConnector.toString());
            System.out.println("\n\n");
            DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
            .using(srcConnector)
            .notifying(record -> {
                SourceRecord sourceRecord = record.record();
                Struct sourceRecordChangeValue = null;
                if (sourceRecord.value() != null){
                    sourceRecordChangeValue = (Struct)sourceRecord.value();
                    logger.info(sourceRecordChangeValue.toString());
                    // Field op = sourceRecordChangeValue.schema().field(OPERATION);
                    // if (op != null && sourceRecordChangeValue != null){
                    //     String operation = sourceRecordChangeValue.getString("op");
                    //     Struct source = (Struct) sourceRecordChangeValue.get("source");
                    //     String tablename = source.getString("table");
                    //     if(!operation.equals("r")) {
                    //         String identifier = null;
                    //         String op_record = operation.equals("d") ? "before" : "after";
                    //         Struct struct_record = (Struct) sourceRecordChangeValue.get(op_record);
                    //         if (operation.equals("c")) {
                    //             // INSERT
                    //             Map<String, Object> eventItem = mapFromStruct(struct_record);
                    //             logger.info("\nInsert "+ tablename.toString() + " " + eventItem.toString());

                    //         } 
                    //         else if (operation.equals("d")) {
                    //             // DELETE
                    //             Map<String, Object> eventItem = mapFromStruct(struct_record);
                    //             logger.info("\nDelete "+ tablename.toString() + " " + eventItem.toString());

                    //         }
                    //         else if (operation.equals("u")) {
                    //             // UPDATE
                    //             Map<String, Object> eventItem = mapFromStruct(struct_record);
                    //             logger.info("\nUpdate "+ tablename.toString() + " " + eventItem.toString());

                    //         }
                    //     } 
                    //     else {
                    //         // REPLAY
                    //         // Struct snapshot_record =  (Struct) sourceRecordChangeValue.get("after");
                    //         Map<String, Object> eventItem = mapFromStruct(sourceRecordChangeValue);
                    //         logger.info("\nReplay "+ tablename.toString() + " " + eventItem.toString());
                    //     } 
                    // }
                    // else {
                    //     // INITIAL
                    //     Struct struct_record = (Struct) sourceRecordChangeValue.get("source");
                    //     boolean isSnapshot = struct_record.get("snapshot").toString().equals("true");
                    //     Object tablename = struct_record.get("table");
                    //     if(isSnapshot && tablename!=null){
                    //         String createTable = sourceRecordChangeValue.getString("ddl");
                    //         int columnValuesInit = createTable.indexOf("(");
                    //         int columnValuesEnd = createTable.lastIndexOf(")");
                    //         if(columnValuesInit != -1 && columnValuesEnd != -1){
                    //             String columnValues = createTable.substring(columnValuesInit+1, columnValuesEnd)
                    //                                     .replaceAll("\n","")
                    //                                     .replaceAll("`", "")
                    //                                     .trim();
                    //             logger.info("\nReplay Snapshot "+ tablename.toString() + " " + columnValues);
                    //             // processCreateTable(catalog, namespace, tablename.toString(), columnValues)
                    //         }
                    //     }
                    // }
                }
            })
            .build();

            // Run the engine async
            ExecutorService executor = Executors.newSingleThreadExecutor();
            logger.info("\n\nExecuting DebeziumEngine.\n\n");
            executor.execute(engine);
            
        } catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    // public static void processCreateTable(RESTCatalog catalog, String namespace, String table, String columnValues){
    //     // id varchar(20) DEFAULT NULL,  fullname varchar(30) DEFAULT NULL,  email varchar(30) DEFAULT NULL
    //     String tablename = namespace + "." + table;
    //     Table catalogTable;
    //     if(catalog.tableExists(tablename)){
    //         catalogTable = catalog.loadTable(tablename);
    //     } else {

    //     }
    //     return catalogTable;
    // }
}