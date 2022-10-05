package com.example.oracleconnect;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.objectstorage.transfer.UploadManager;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

/**
 * Example of using the simplified UploadManager to upload objects.
 * <p>
 * UploadManager can be configured to control how/when it does multi-part uploads,
 * and manages the underlying upload method.  Clients construct a PutObjectRequest
 * similar to what they normally would.
 */
public class UploadFile {


        public static void main(String[] args) throws Exception {
            String configurationFilePath = "~/.oci/config";
            String profile = "DEFAULT";

            Region region = Region.EU_FRANKFURT_1;
            String namespaceName = "frncljm8hgyu";
            String bucketName = "bucket-new";
            String objectName = "New.txt";
            Map<String, String> metadata = null;
            String contentType = "text/plain";
            String contentEncoding = null;
            String contentLanguage = "en";
            File body = new File("/Users/karolinajedziniak/Desktop/Pobrane/New.txt");

            // Configuring the AuthenticationDetailsProvider. It's assuming there is a default OCI config file
            // "~/.oci/config", and a profile in that config with the name "DEFAULT". Make changes to the following
            // line if needed and use ConfigFileReader.parse(CONFIG_LOCATION, CONFIG_PROFILE);

            final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault();

            final ConfigFileAuthenticationDetailsProvider provider =
                    new ConfigFileAuthenticationDetailsProvider(configFile);

            ObjectStorage client = new ObjectStorageClient(provider);
            client.setRegion(region);

            // configure upload settings as desired
            UploadConfiguration uploadConfiguration =
                    UploadConfiguration.builder()
                            .allowMultipartUploads(true)
                            .allowParallelUploads(true)
                            .build();

            UploadManager uploadManager = new UploadManager(client, uploadConfiguration);

            // create upload request
            PutObjectRequest request = createUploadRequest(bucketName, namespaceName, objectName, contentType,
                    contentLanguage, contentEncoding, metadata);

            UploadManager.UploadRequest uploadDetails =
                    UploadManager.UploadRequest.builder(body).allowOverwrite(true).build(request);

            // upload request and print result
            // if multi-part is used, and any part fails, the entire upload fails and will throw BmcException
            UploadManager.UploadResponse response = uploadManager.upload(uploadDetails);
            System.out.println(response);

            // fetch the object just uploaded
            GetObjectResponse getResponse = fetchUploadedObject(client, namespaceName, bucketName, objectName);

            // use the response's function to print the fetched object's metadata
            System.out.println(getResponse.getOpcMeta());

            // stream contents should match the file uploaded
            ObjectStorageManipulation.printFileContents(getResponse);

            client.close();
        }

        static PutObjectRequest createUploadRequest(String bucketName, String namespaceName, String objectName,
                                                    String contentType, String contentLanguage, String contentEncoding,
                                                    Map metadata) {
            return PutObjectRequest.builder()
                    .bucketName(bucketName)
                    .namespaceName(namespaceName)
                    .objectName(objectName)
                    .contentType(contentType)
                    .contentLanguage(contentLanguage)
                    .contentEncoding(contentEncoding)
                    .opcMeta(metadata)
                    .build();
        }

        static GetObjectResponse fetchUploadedObject(ObjectStorage client, String namespaceName, String bucketName, String objectName) {
            return client.getObject(
                    GetObjectRequest.builder()
                            .namespaceName(namespaceName)
                            .bucketName(bucketName)
                            .objectName(objectName)
                            .build());
        }
}
