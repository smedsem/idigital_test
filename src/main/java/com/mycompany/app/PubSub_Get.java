package com.mycompany.app;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.nio.file.Files;

public class PubSub_Get {

    private final String bucketName;
    private final String projectID;
    private final String credFile;
    private final TableId image_tableId;
    private final TableId metadata_tableId;
    private final String tmpDirPath;

    private final Storage storage;
    private final BigQuery bigquery;


    public PubSub_Get() throws IOException {
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

    public static void main(String[] a) throws IOException, InterruptedException {
        new PubSub_Get().getMessage();
    }

    public void getMessage() throws IOException, InterruptedException {

/*
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            ListTopicsRequest listTopicsRequest =
                    ListTopicsRequest.newBuilder()
                            .setProject(ProjectName.format(projectID))
                            .build();
            TopicAdminClient.ListTopicsPagedResponse response = topicAdminClient.listTopics(listTopicsRequest);
            Iterable<Topic> topics = response.iterateAll();
            for (Topic topic : topics) {
                System.out.println(topic.getName());
                // do something with the topic
            }
        }
*/

        MessageReceiver receiver =
                new MessageReceiver() {
                    @Override
                    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                        // handle incoming message, then ack/nack the received message
                        System.out.println("Id : " + message.getMessageId());
                        System.out.println("Data : " + message.getData().toStringUtf8());
                    //    consumer.ack();
                    }
                };

        String subscriptionName = "projects/dicom-206311/subscriptions/sem_sub";

        Subscriber subscriber = null;
        try {
            // create a subscriber bound to the asynchronous message receiver
            subscriber =
                    Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            // Continue to listen to messages
            while (true) {
                Thread.sleep(10);
            }
        } finally {
            if (subscriber != null) {
                subscriber.stopAsync();
            }
        }
    }
}
