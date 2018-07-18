package com.mycompany.app;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.pubsub.v1.*;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;

public class PubSub_Send {

    private final String bucketName;
    private final String projectID;
    private final String credFile;
    private final TableId image_tableId;
    private final TableId metadata_tableId;
    private final String tmpDirPath;

    private final Storage storage;
    private final BigQuery bigquery;


    public PubSub_Send() throws IOException {
        bucketName = "z-lidc-idri";
        projectID = "dicom-206311";
        credFile = "C:\\Users\\usr\\Downloads\\IDGital-6f90c407c7fe.json";
        image_tableId = TableId.of("images_info", "image");
        metadata_tableId = TableId.of("images_info", "metadata");
        tmpDirPath = Files.createTempDirectory("DICOM").toString();

        storage = StorageOptions.newBuilder()
                .setProjectId(projectID)
                .build()
                .getService();

        bigquery = BigQueryOptions.newBuilder().
                setProjectId(projectID)
                .build()
                .getService();
    }

    public void sendMessage() throws IOException {

        PublishRequest request = PublishRequest.newBuilder().build();

        Publisher publisher;
        try {

            publisher = Publisher.newBuilder("projects/dicom-206311/topics/sem_topic")
                    .build();


            for(int i=0;i<100;i++) {
                String payload = "Hellooooo " + i;
                PubsubMessage pubsubMessage =
                        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(payload)).build();

                publisher.publish(pubsubMessage).get();
            }

            System.out.println("Sent!");
        } catch (IOException e) {
            System.out.println("Not Sended!");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }



    public static void main(String[] a) throws IOException, InterruptedException {
        new PubSub_Send().sendMessage();
    }
}
