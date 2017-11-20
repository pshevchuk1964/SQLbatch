package maintest;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.asyncsql.PostgreSQLClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class MainStartClass extends AbstractVerticle {

    Logger logger = LoggerFactory.getLogger( MainStartClass.class );

    @Override
    public void start( ){
        logger.info( " -= Test SQL (batch) started. =- " );
        JsonObject postgreSQLClientConfig = new JsonObject()
                .put( "host", "192.168.122.37" )
                .put( "port", 5432 )
                .put( "maxPoolSize", 30 )
                .put( "username", "pshevchuk" )
                .put( "password", "pshevchuk" )
                .put( "database", "testdb" )
                .put( "default_schema", "test" )

                .put( "charset", "UTF-8" )
                .put( "queryTimeout", 10000 );
        SQLClient postgreSQLClient = PostgreSQLClient.createNonShared( vertx, postgreSQLClientConfig );
        logger.info( " -= DB postgreSQLClient SET . =- " );
        postgreSQLClient.getConnection( car -> {
            if (car.succeeded()) {
                SQLConnection connection = car.result();
                connection.setAutoCommit( true, res -> {
                    if (res.succeeded()) {
                        // OK!
                        final String strSQL = "INSERT INTO test.test2 ( mycomments ) VALUES (?)";
                        connection.queryWithParams( strSQL, new JsonArray().add( strSQL + " => " + str_time() ), res2 -> {
                            if (res2.succeeded()) {
                                logger.info( " ---=== INSERT OK (+1) ===--- " );
                                //   --------- 1) START ------------
                                //   batch.add(new JsonArray().add("joe"));
                                //   batch.add(new JsonArray().add("jane"));
                                logger.info( " INSERT EXECUTE. " );
//                                List<JsonArray> batch = new ArrayList<>();
//                                for (int n_count = 0; n_count < 2; n_count++) {
//                                    batch.add( new JsonArray().add( "count_" + n_count  ) );
//                                }
                                List<JsonArray> batch = new ArrayList<>();
                                for (int n_count = 0; n_count < 5; n_count++) {
                                    batch.add( new JsonArray().add( "count_" + n_count ) );
                                }
                                // batch.add(new JsonArray().add("jane"));
                                logger.info( " --==  List<JsonArray> batch => " + batch.toString() );
                                // String strSQL2 = "INSERT INTO test.test2 ( mycomments ) VALUES (?)";
                                connection.queryWithParams( strSQL, batch.get( 0 ), res5 -> {
                                    if (res5.succeeded()) {
                                        logger.info( " --==  Batch ( 0 ) inserted. ==-- " );
                                    } else {
                                        logger.info( " --==   Not Insert batch(0)  ==-- " );
                                    }

                                } );
                                connection.batchWithParams( " INSERT INTO test.test2 ( mycomments ) VALUES (?)", batch, res3 -> {
                                    logger.info( " xdthdszthdsrfthjsE. " );
                                    if (res3.succeeded()) {
                                        List<Integer> result = res3.result();
                                        logger.info( " ---=== INSERT OK ( BRANCH ) ===--- " );
//                                  connection.commit( res3.result() );
                                    } else {
                                        ;
                                        // logger.error( " ---=== INSERT ERROR : " + 345623 + " ===--- " );
                                    }
                                } );
                                //   --------- 1) END ------------
                                //   --------- 2) START ------------
//                        logger.info( " Start Branch INSERT.  " );
//                        List<String> batch = new ArrayList<>();
//                        batch.add( "INSERT INTO test.test2 ( mycomments ) value ( 'xcfbhxzsdhf' ) " );
//                        batch.add( "INSERT INTO test.test2 ( mycomments ) value ( 'xcfzdbfdzxf' ) " );
//                        logger.info( " --==  List<String> batch => "+ batch.toString() );
//
//                        logger.info( " INSERT EXECUTE. " );
//
//                        // SQLConnection connection2 = car.result();
//                        connection.batch( batch, resp -> {
//                            logger.info( " INSERT BATCH. " );
//                            if (resp.succeeded()) {
//                                // logger.info( " Branch INSERTed.  " );
//                                List<Integer> result = resp.result();
//                                connection.close();
//                                return;
//                            } else {
//                                // logger.info( " Branch NOT INSERTed.  " );
//                                connection.close();
//                                postgreSQLClient.close();
//                                return;
//                            }
//                        });
                                //   --------- 2) END ------------

                            } else {
                                logger.error( " ---=== INSERT ERROR !!! ===--- " );
                            }
                        } );
// ------
                    } else {
                        // Failed!
                    }
                } );
/*
            }
*/
            } else {
                logger.info( " postgreSQLClient error Conected ! " + car.result().toString() );
                return;
            }


        } );
        logger.info( " -= DB postgreSQLClient disconnect . =- " );
        postgreSQLClient.close();

    }

    public void close( ){
        logger.info( " -= Test SQL (batch) stoped. =- " );
    }


    private String str_time( ){
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat format1 = new SimpleDateFormat( "YYYY-MM-dd HH-mm-ss" );
        cal.add( Calendar.DATE, (0) );
        String dataNew = format1.format( cal.getTime() );
        // logger.info(" NEW TIME :" + dataNew);
        return dataNew;
    }

}
