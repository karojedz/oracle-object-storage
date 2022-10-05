package com.example.oracleconnect;

import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorageAsync;
import com.oracle.bmc.objectstorage.ObjectStorageAsyncClient;
import com.oracle.bmc.objectstorage.model.BucketSummary;
import com.oracle.bmc.objectstorage.requests.GetNamespaceRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.ListBucketsRequest;
import com.oracle.bmc.objectstorage.responses.GetNamespaceResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.ListBucketsResponse;
import com.oracle.bmc.responses.AsyncHandler;

import java.util.concurrent.CountDownLatch;

public class DownloadFileContentAsync {

    public static void main(String[] args) throws Exception {

        String configurationFilePath = "~/.oci/config";
        String profile = "DEFAULT";

        Region region = Region.EU_FRANKFURT_1;
        String compartmentOCID = "ocid1.compartment.oc1..aaaaaaaajsz5nfixu6yjn6ftlq6fbwk475abtxu2fmstqlmit35ls2nc7xma";

        String bucketName = "bucket-new";
        String objectName = "file.txt";

        // Configuring the AuthenticationDetailsProvider. It's assuming there is a default OCI config file
        // "~/.oci/config", and a profile in that config with the name "DEFAULT". Make changes to the following
        // line if needed and use ConfigFileReader.parse(CONFIG_LOCATION, CONFIG_PROFILE);

        final ConfigFileReader.ConfigFile configFile = ConfigFileReader.parseDefault();

        final AuthenticationDetailsProvider provider =
                new ConfigFileAuthenticationDetailsProvider(configFile);

        ObjectStorageAsync client = new ObjectStorageAsyncClient(provider);
        client.setRegion(region);

        ResponseHandler<GetNamespaceRequest, GetNamespaceResponse> namespaceHandler =
                new ResponseHandler<>();
        client.getNamespace(GetNamespaceRequest.builder().build(), namespaceHandler);
        GetNamespaceResponse namespaceResponse = namespaceHandler.waitForCompletion();

        String namespaceName = namespaceResponse.getValue();
        System.out.println("Using namespace: " + namespaceName);

        ListBucketsRequest.Builder listBucketsBuilder =
                ListBucketsRequest.builder()
                        .namespaceName(namespaceName)
//                        .compartmentId(provider.getTenantId());
                        .compartmentId(compartmentOCID);

        String nextToken = null;
        do {
            listBucketsBuilder.page(nextToken);
            ResponseHandler<ListBucketsRequest, ListBucketsResponse> listBucketsHandler =
                    new ResponseHandler<>();
            client.listBuckets(listBucketsBuilder.build(), listBucketsHandler);
            ListBucketsResponse listBucketsResponse = listBucketsHandler.waitForCompletion();
            for (BucketSummary bucket : listBucketsResponse.getItems()) {
                System.out.println("Found bucket: " + bucket.getName());
            }
            nextToken = listBucketsResponse.getOpcNextPage();
        } while (nextToken != null);

        // fetch the uploaded file from object storage
        ResponseHandler<GetObjectRequest, GetObjectResponse> objectHandler =
                new ResponseHandler<>();
        GetObjectRequest getObjectRequest =
                GetObjectRequest.builder()
                        .namespaceName(namespaceName)
                        .bucketName(bucketName)
                        .objectName(objectName)
                        .build();
        client.getObject(getObjectRequest, objectHandler);
        GetObjectResponse getResponse = objectHandler.waitForCompletion();

        // stream contents should match the file uploaded
        ObjectStorageManipulation.printFileContents(getResponse);

        client.close();
    }

    private static class ResponseHandler<IN, OUT> implements AsyncHandler<IN, OUT> {
        private OUT item;
        private Throwable failed = null;
        private CountDownLatch latch = new CountDownLatch(1);

        private OUT waitForCompletion() throws Exception {
            latch.await();
            if (failed != null) {
                if (failed instanceof Exception) {
                    throw (Exception) failed;
                }
                throw (Error) failed;
            }
            return item;
        }

        @Override
        public void onSuccess(IN request, OUT response) {
            item = response;
            latch.countDown();
        }

        @Override
        public void onError(IN request, Throwable error) {
            failed = error;
            latch.countDown();
        }
    }

}
