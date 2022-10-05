package com.example.oracleconnect;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CreateBucketDetails;
import com.oracle.bmc.objectstorage.requests.CreateBucketRequest;
import com.oracle.bmc.objectstorage.requests.GetBucketRequest;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetBucketResponse;
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Example to get a bucket and its statistics.
 * <p>
 * This example first creates a bucket in the compartment corresponding to the compartment OCID passed as the first
 * argument to this program. The name of the bucket created is same as the second argument to the program. It also
 * creates an object in this bucket whose name is the third argument.
 * </p>
 * It then illustrates how we can get a bucket and its statistics (Estimated Size and Estimated Count).
 *
 *
 * args - Arguments to provide to the example. The following arguments are expected:
 *             <ul>
 *             <li>The first argument is the OCID of the compartment.</li>
 *             <li>The second is the name of bucket to create and later fetch</li>
 *             <li>The third is the name of object to create inside bucket</li>
 *             </ul>
 */
public class CreateBucketAndObject {

        public static void main(String[] args) throws Exception {
            String configurationFilePath = "~/.oci/config";
            String profile = "DEFAULT";

            Region region = Region.EU_FRANKFURT_1;


            final String compartmentId = "ocid1.compartment.oc1..aaaaaaaajsz5nfixu6yjn6ftlq6fbwk475abtxu2fmstqlmit35ls2nc7xma";
            final String bucketName = "bucket-new";
            final String objectName = "file.txt";

            // Configuring the AuthenticationDetailsProvider. It's assuming there is a default OCI config file
            // "~/.oci/config", and a profile in that config with the name "DEFAULT". Make changes to the following
            // line if needed and use ConfigFileReader.parse(CONFIG_LOCATION, CONFIG_PROFILE);

            final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault();

            final AuthenticationDetailsProvider provider =
                    new ConfigFileAuthenticationDetailsProvider(configFile);

            ObjectStorage client = new ObjectStorageClient(provider);
            client.setRegion(region);

            String namespaceName = getNamespaceName(client);

            createBucket(compartmentId, bucketName, namespaceName, client);

            createObject(namespaceName, bucketName, objectName, client);

            GetBucketRequest request = createGetBucketRequest(namespaceName, bucketName);

            GetBucketResponse response = client.getBucket(request);

            printBucketData(response);

            client.close();
        }


        static String getNamespaceName(ObjectStorage client) {
            GetNamespaceResponse namespaceResponse =
                    client.getNamespace(GetNamespaceRequest.builder().build());
            return namespaceResponse.getValue();
        }

        static void createBucket(String compartmentId, String bucketName, String namespaceName, ObjectStorage client) {
            CreateBucketDetails createSourceBucketDetails =
                    CreateBucketDetails.builder().compartmentId(compartmentId).name(bucketName).build();
            CreateBucketRequest createSourceBucketRequest =
                    CreateBucketRequest.builder()
                            .namespaceName(namespaceName)
                            .createBucketDetails(createSourceBucketDetails)
                            .build();
            client.createBucket(createSourceBucketRequest);
        }

        static void createObject(String namespaceName, String bucketName, String objectName, ObjectStorage client) {
            PutObjectRequest putObjectRequest =
                    PutObjectRequest.builder()
                            .namespaceName(namespaceName)
                            .bucketName(bucketName)
                            .objectName(objectName)
                            .contentLength(4L)
                            .putObjectBody(
                                    new ByteArrayInputStream("Let's do sth fun instead.".getBytes(StandardCharsets.UTF_8)))
                            .build();
            client.putObject(putObjectRequest);
        }

        static GetBucketRequest createGetBucketRequest(String namespaceName, String bucketName) {
            List<GetBucketRequest.Fields> fieldsList = new ArrayList<>(2);
            fieldsList.add(GetBucketRequest.Fields.ApproximateCount);
            fieldsList.add(GetBucketRequest.Fields.ApproximateSize);
            return GetBucketRequest.builder()
                            .namespaceName(namespaceName)
                            .bucketName(bucketName)
                            .fields(fieldsList)
                            .build();
        }

        static void printBucketData(GetBucketResponse response) {
            System.out.println("Bucket Name : " + response.getBucket().getName());
            System.out.println("Bucket Compartment : " + response.getBucket().getCompartmentId());
            System.out.println(
                    "The Approximate total number of objects within this bucket : "
                            + response.getBucket().getApproximateCount());
            System.out.println(
                    "The Approximate total size of objects within this bucket : "
                            + response.getBucket().getApproximateSize());
        }
}
