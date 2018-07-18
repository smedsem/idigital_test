package com.mycompany.app;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.dcm4che3.data.Attributes;
import org.dcm4che3.tool.dcm2jpg.Dcm2Jpg;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class ImportDicomFiles {

    private final String bucketName;
    private final String projectID;
    private final String credFile;
    private final TableId image_tableId;
    private final TableId metadata_tableId;
    private final String tmpDirPath;

    private final Storage storage;
    private final BigQuery bigquery;

    public ImportDicomFiles() throws IOException {
        bucketName = "z-lidc-idri";
        projectID = "dicom-206311";
        credFile = "C:\\Users\\usr\\Downloads\\IDGital-6f90c407c7fe.json";
        image_tableId = TableId.of("images_info", "image");
        metadata_tableId = TableId.of("images_info", "metadata");
        tmpDirPath = Files.createTempDirectory("DICOM").toString();

        storage = StorageOptions.newBuilder()
                .setProjectId(projectID)
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credFile)))
                .build()
                .getService();

        bigquery = BigQueryOptions.newBuilder().
                setProjectId(projectID)
                .setCredentials(GoogleCredentials.fromStream(new FileInputStream(credFile)))
                .build()
                .getService();
    }

    public static void main(String[] a) throws IOException {
        //Storage storage = StorageOptions.newBuilder().build().getService();
        //  Blob blobJPEG = storage.get("z-lidc-idri", "000001.jpg");
        new ImportDicomFiles().importDicomObjects("LIDC-IDRI/LIDC-IDRI-0001");
    }

    private void importDicomObjects(String filesLocation) throws IOException {

        Page<Blob> blobs = storage.list(bucketName,
                Storage.BlobListOption.prefix(filesLocation));

        blobs.iterateAll().forEach(blob -> {
            if (blob.getName().endsWith(".dcm")) {
                try {
                    importDicomObject(blob);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private String downloadToLocalTmp(Blob blob) throws IOException {

        File tmpFile_DCM = new File(tmpDirPath + File.separator + blob.getName());
        tmpFile_DCM.getParentFile().mkdirs();
        Files.createFile(tmpFile_DCM.toPath());
        blob.downloadTo(tmpFile_DCM.toPath());

        return tmpFile_DCM.toString();
    }

    private String convertToJPG(String localDCMFileName) throws IOException {
        String jpgTmpFileName = localDCMFileName.replace(".dcm", ".jpg");
        Dcm2Jpg dcm2Jpg = new Dcm2Jpg();
        dcm2Jpg.initImageWriter("jpg", null, null, null, null);
        dcm2Jpg.convert(new File(localDCMFileName), new File(jpgTmpFileName));

        return jpgTmpFileName;
    }

    private Blob uploadToStorage(String tmpJPGFileName, String location) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(tmpJPGFileName);

        return storage
                .create(
                        BlobInfo.newBuilder(bucketName, location).build(),
                        fileInputStream);
    }


    private void importDicomObject(Blob blob) throws IOException {
        System.out.println("Start importing " + blob.getName());

        String tmpDCMFileName = downloadToLocalTmp(blob);

        Dicom dicom = new Dicom(tmpDCMFileName);

        String jpgTmpFileName = tmpDCMFileName.replace(".dcm", ".jpg");

        String tmpJPGFileName = dicom.convertToJPG(jpgTmpFileName);
        String newJPGStorageLocation = blob.getName().replace(".dcm", ".jpg");
        Blob jpegBlob = uploadToStorage(tmpJPGFileName, newJPGStorageLocation);

        Attributes attributes = dicom.getAttributes();


        long imageId = System.nanoTime();

        List<Integer> tags = Arrays.stream(attributes.tags()).boxed().collect(Collectors.toList());

        List<Map<String, Object>> tagsMap = new ArrayList<>();

        tags.forEach(tag -> {
            String value = attributes.getString(tag);

            System.out.println("tag = " + tag);
            System.out.println("attribute value = " + value);

            Map<String, Object> metadataRowContent = new HashMap<>();
            metadataRowContent.put("tag", tag);
            metadataRowContent.put("value", value);
            metadataRowContent.put("image_id", imageId);
            // metadataRowContent.put("create_date", new org.joda.time.DateTime(new Date()));

            tagsMap.add(metadataRowContent);

        });

        insertRows(metadata_tableId, tagsMap);

        Map<String, Object> imageRowContent = new HashMap<>();
        imageRowContent.put("dcm_image_bucket", blob.getBucket());
        imageRowContent.put("dcm_image_location", blob.getName());
        imageRowContent.put("jpg_image_bucket", jpegBlob.getBucket());
        imageRowContent.put("jpg_image_location", jpegBlob.getName());
        imageRowContent.put("id", imageId);

        insertRow(image_tableId, imageRowContent);

    }

    void insertRow(TableId tableId, Map<String, Object> metadataRowContent) {
        InsertAllResponse response =
                bigquery.insertAll(
                        InsertAllRequest.newBuilder(tableId)
                                .addRow(metadataRowContent)
                                .build());
        if (response.hasErrors()) {
            // If any of the insertions failed, this lets you inspect the errors
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                // inspect row error
            }
        }
    }

    void insertRows(TableId tableId, List<Map<String, Object>> metadataRowContent) {
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
        metadataRowContent.forEach(builder::addRow);

        InsertAllResponse response =
                bigquery.insertAll(builder.build());

        if (response.hasErrors()) {
            // If any of the insertions failed, this lets you inspect the errors
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                // inspect row error
            }
        }
    }

}
