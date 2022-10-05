package com.example.oracleconnect;

import com.oracle.bmc.objectstorage.responses.GetObjectResponse;

import java.io.InputStream;

public class ObjectStorageManipulation {

    static void printFileContents(GetObjectResponse response) throws Exception {
        try (final InputStream fileStream = response.getInputStream()) {
            // use fileStream
            byte[] bytes = new byte[fileStream.available()];
            System.out.println(fileStream.read(bytes));
            for (byte b: bytes) {
                System.out.print((char)b);
            }
            System.out.println();
        }
    }

}
