/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;

import java.io.File;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import faasinspector.register;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.UUID;

/**
 * uwt.lambda_test::handleRequest
 *
 * @author wlloyd
 */
public class Extraction implements RequestHandler<Request, Response> {

//    private static final DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    LambdaLogger logger = null;
    public AmazonS3 s3client;
    private static final String LAMBDA_TEMP_DIRECTORY = "/tmp/";
    private static final String AWS_REGION = "us-east-1";
    private static final String FILTERED = "filtered/";
    private static final String LOADED = "loaded/";
    private static String bucketName;
    private static String dbName;
    private static String tableName;
    private static String transactionid;


    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {

        bucketName = request.getBucketname();
        dbName = request.getDbname();
        tableName = request.getTablename();
        transactionid = request.getTransactionid();

        //static path to csv file under /tmp to be loaded to db table:
        System.out.println("bucketName " + bucketName + " objectKey:  " + dbName);
        // Create logger
        logger = context.getLogger();

        //setup S3 client :
        s3client = AmazonS3ClientBuilder.standard().withRegion(AWS_REGION).build();

        // Register function
        register reg = new register(logger);

        //stamp container with uuid
        Response r = reg.StampContainer();

        setCurrentDirectory("/tmp");

        String precheckErrMsg = validateParams(request);
        if (precheckErrMsg != null) {
            setResponseObj(r, false, precheckErrMsg,  null, null, null);
            return r;
        }
        logger.log("input fileName: " + bucketName);

        // *********************************************************************
        // Implement Lambda Function Here
        // *********************************************************************
        try {
            File tmpDir = new File(dbName);

            //check if sale_xxx.db exist local or not:
            boolean exists = tmpDir.exists();
            if (!exists) {
                //if doesnt exist, go to s3 bucket at /loaded/sale_xxx.db to get it
                String dbObjectKey = LOADED + dbName;
                getDataFromS3(bucketName, dbObjectKey);
            }
            String url = "jdbc:sqlite:" + dbName;

            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection(url);

            // Detect if the table  exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT * FROM sqlite_master WHERE type='table' AND name='" + tableName + "'");

            ResultSet rs = ps.executeQuery();

            //list file for debugging:
            listFile(LAMBDA_TEMP_DIRECTORY);

            ps = con.prepareStatement("select \"Region\",\"Country\", \"Item Type\", "
                    + "avg(\"Units Sold\"), min(\"Units Sold\"),  max(\"Units Sold\"),\n"
                    + "                       sum(\"Units Sold\"), count(\"Units Sold\") from "
                    + tableName + "  WHERE  \"Item Type\" = \"Clothes\";");
//            ps = con.prepareStatement("select * from sale_table;");
            rs = ps.executeQuery();
            String timeStamp = new SimpleDateFormat("yyyyMMdd-HHmmss").format(Calendar.getInstance().getTime());


            String filesave_filtering = timeStamp + "-" + transactionid + "-filtering.csv";
//            System.out.print(" filesave_filtering is  ################" +filesave_filtering);

            saveData(rs, bucketName, filesave_filtering);

            ps = con.prepareStatement("select \"Region\",\"Country\", \"Item Type\", "
                    + "avg(\"Units Sold\"), min(\"Units Sold\"),  max(\"Units Sold\"),\n"
                    + "                       sum(\"Units Sold\"), count(\"Units Sold\") from "
                    + tableName + "  group by \"Region\",\"Country\", \"Item Type\";");
//            ps = con.prepareStatement("select * from sale_table;");
            rs = ps.executeQuery();
//            listFile(LAMBDA_TEMP_DIRECTORY);

//            System.out.println(timeStamp);
            String filesave_aggregate = timeStamp + "-" + transactionid + "-aggregate.csv";

//            System.out.print(" filesave_aggregate is  ################" +filesave_aggregate);
            saveData(rs, bucketName, filesave_aggregate);
//            String s = filesave_aggregate;
            filesave_filtering = filesave_filtering.substring(filesave_filtering.indexOf("/") + 1);
            filesave_filtering.trim();
            filesave_aggregate = filesave_aggregate.substring(filesave_aggregate.indexOf("/") + 1);
            filesave_aggregate.trim();
            //send response
            setResponseObj(r, true, null, bucketName, filesave_filtering, filesave_aggregate);
            // Delete the sample objects.
          
            setResponseObj(r, true, null, bucketName, filesave_filtering, filesave_aggregate);
        
            rs.close();
            con.close();

        } catch (SQLException sqle) {
            logger.log("DB ERROR:" + sqle.toString());

        } catch (Exception e) {
            logger.log("File Error: " + e.toString());
            setResponseObj(r, false, e.toString() ,  null, null, null);
            
        }
        // Upload three sample objects.
//        RemoveFiles(bucketName, filterd);
        return r;
    }

    //insert data from csv to table
    /**
     * Helper method
     *
     * @param directory_name
     * @return boolean
     */
    private static boolean setCurrentDirectory(String directory_name) {
        boolean result = false;  // Boolean indicating whether directory was set
        File directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs()) {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }

    private void setResponseObj(Response r, boolean success, String e,
            String dbName, String filesave_filtering, String filesave_aggregate) {
        // Set response object:
        r.setBucketname(bucketName);

        if (success) {
            r.setSuccess(true);
            r.setDbname(dbName);
            r.setFname_filtering(filesave_filtering);
            r.setFname_aggregate(filesave_aggregate);
        } else {
            r.setSuccess(false);
            r.setError(e);
        }

    }

    private void RemoveFiles( String bucketName, String keys) {


        try {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(AWS_REGION)
                .build();



        // Delete the sample objects.
        DeleteObjectsRequest multiObjectDeleteRequest = new DeleteObjectsRequest(bucketName)
                                                                .withKeys(keys)
                                                                .withQuiet(false);

        // Verify that the objects were deleted successfully.
        DeleteObjectsResult delObjRes = s3Client.deleteObjects(multiObjectDeleteRequest);
        int successfulDeletes = delObjRes.getDeletedObjects().size();
        System.out.println(successfulDeletes + " objects successfully deleted.");
    }catch(AmazonServiceException e) {
        // The call was transmitted successfully, but Amazon S3 couldn't process
        // it, so it returned an error response.
        e.printStackTrace();
    }


    }

    
    
//    SAVE DATA TO S3 --------------------------------------------
    private void saveData(ResultSet resultSet, String bucketName, String filesave) {
        String objKey  = FILTERED + filesave; //save object under: "filtered/"
        logger.log("Save data in S3: " + objKey);
        try {
//            int count = 0;
            ResultSetMetaData rsmd = resultSet.getMetaData();
//            String result1 = "";
            int columnsNumber = rsmd.getColumnCount();
            StringWriter result = new StringWriter();
            result = result.append("Region,Country,Item_Type,Average_of_Units_Sold,Min_of_Units_Sold, Max_of_Units_Sold,Sum_of_Units_Sold,Count_of_Units_Sold");
            result.append("\n");
            while (resultSet.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = resultSet.getString(i);
                    result.append(columnValue);
                    if ((i) != columnsNumber) {
                        result.append(",");
                    } else {
                        result.append("\n");
                    }
                }
            }
            byte[] bytes = result.toString().getBytes(StandardCharsets.UTF_8);
            InputStream is = new ByteArrayInputStream(bytes);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(bytes.length);
            meta.setContentType("text/plain");

            // Stream data to S3
            s3client.putObject(bucketName, objKey, is, meta);

        } catch (Exception e) {
            System.out.println("Error displayData. " + e.getMessage());
        }

    }

    /**
     * get s3 object
     *
     * @param bucketName bucket name
     * @param objectKey object key aka name of file in s3, or path of file in s3
     */
    private void getDataFromS3(String bucketName, String objectKey) {
        System.out.println("getting file from s3 for " + bucketName + " : " + objectKey);

        try {
            s3client.getObject(new GetObjectRequest(bucketName, objectKey),
                    new File(LAMBDA_TEMP_DIRECTORY + dbName));
        } catch (Exception e) {
            logger.log("Error getting object from S3. " + e.getMessage());
        }

    }

    //just do some simple validation for required fields:
    private String validateParams(Request request) {

        if (request.getTransactionid() == null || request.getTransactionid().isEmpty()) {
            return "\"transactionid\" is required in request";
        }

        if (request.getBucketname() == null || request.getBucketname().isEmpty()) {
            return "\"bucketname\" is required in request";

        }

        if (dbName == null || dbName.isEmpty()) {
            return "\"dbname\" is required in request";
        }

        if (tableName == null || tableName.isEmpty()) {
            return "\"tablename\" is required in request";
        }

        return null;
    }

    //list File under a path:
    public static void listFile(String path) {
        System.out.println("========== listFile under:================= " + path);
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles.length > 0) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    System.out.println("File " + listOfFiles[i].getName());

                } else if (listOfFiles[i].isDirectory()) {
                    System.out.println("Directory " + listOfFiles[i].getName());
                }
            }
        }

        System.out.println("=============================================");
    }

    // TODO: fix this so we can collect metrics:
    // int main enables testing function from cmd line
    public static void main(String[] args) {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };

        Extraction lt = new Extraction();

        // Create a request object
        Request req = new Request();

//         Grab the name from the cmdline from arg 0
        String bucketName = (args.length > 0 ? args[0] : "");

        // Load the name into the request object
        req.setBucketname(bucketName);

        // Grab the name from the cmdline from arg 0
        // Grab the name from the cmdline from arg 0
        String objectKey = (args.length > 0 ? args[1] : "");
//
//        // Load the name into the request object
//        // Report name to stdout
        // Grab the name from the cmdline from arg 0

//        System.out.println("cmd-line param name=" + req.getObjectKey());

        // Run the function
        Response resp = lt.handleRequest(req, c);
        try {
            Thread.sleep(100000);
        } catch (InterruptedException ie) {
            System.out.print(ie.toString());
        }
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
}
