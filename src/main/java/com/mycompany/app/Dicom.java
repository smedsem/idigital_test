package com.mycompany.app;

import org.dcm4che3.data.Attributes;
import org.dcm4che3.io.DicomInputStream;
import org.dcm4che3.tool.dcm2jpg.Dcm2Jpg;

import java.io.File;
import java.io.IOException;

public class Dicom {
    private String localFileName;

    public Dicom(String localFileName) {
        this.localFileName = localFileName;
    }

    public String convertToJPG(String destinationFile) throws IOException {
        Dcm2Jpg dcm2Jpg = new Dcm2Jpg();
        dcm2Jpg.initImageWriter("jpg", null, null, null, null);
        dcm2Jpg.convert(new File(localFileName), new File(destinationFile));

        return destinationFile;
    }

    public Attributes getAttributes() throws IOException {
        DicomInputStream din = new DicomInputStream(new File(localFileName));
        Attributes attributes = new Attributes();
        din.readAttributes(attributes, 10000, 1);

        return attributes;
    }

}
