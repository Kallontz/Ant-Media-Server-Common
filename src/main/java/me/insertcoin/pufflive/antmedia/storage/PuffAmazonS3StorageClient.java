package me.insertcoin.pufflive.antmedia.storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import io.antmedia.storage.StorageClient;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.file.Files;

public class PuffAmazonS3StorageClient extends StorageClient {

    private AmazonS3 amazonS3;
    private String storagePrefix;

    protected static Logger logger = LoggerFactory.getLogger(io.antmedia.storage.AmazonS3StorageClient.class);

    private AmazonS3 getAmazonS3() {
        if (amazonS3 == null) {
            final BasicAWSCredentials awsCredentials = new BasicAWSCredentials(getAccessKey(), getSecretKey());

            final AmazonS3ClientBuilder awsClientBuilder =
                    AmazonS3Client.builder().withCredentials(new AWSStaticCredentialsProvider(awsCredentials));

            awsClientBuilder.setRegion(getRegion());

            amazonS3 = awsClientBuilder.build();
        }
        return amazonS3;
    }


    public void delete(String fileName, FileType type) {
        AmazonS3 s3 = getAmazonS3();
        s3.deleteObject(getStorageName(), type.getValue() + "/" + fileName);

    }

    public boolean fileExist(String fileName, FileType type) {
        AmazonS3 s3 = getAmazonS3();
        return s3.doesObjectExist(getStorageName(), type.getValue() + "/" + fileName);
    }

    public void save(final File file, FileType type) {
        String decodedPath;

        try {
            decodedPath = URLDecoder.decode(file.getPath(), "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            decodedPath = file.getPath();
        }

        final String filePath = decodedPath;
        final String decodedFileName = decodedPath.substring(decodedPath.lastIndexOf(File.separator) + File.separator.length());
        final int pathPrefixEndIdx = filePath.indexOf("/streams/") + 9;
        final int pathSuffixStartIdx = filePath.indexOf(decodedFileName);

        final String relativePath = filePath.substring(pathPrefixEndIdx, pathSuffixStartIdx);
        final String s3Path = (storagePrefix != null ? storagePrefix : type.getValue()) + File.separator + relativePath;

        final AmazonS3 s3 = getAmazonS3();
        final PutObjectRequest putRequest = new PutObjectRequest(getStorageName(), s3Path + decodedFileName, file);

        putRequest.setCannedAcl(CannedAccessControlList.PublicRead);
        putRequest.setGeneralProgressListener(event -> {
            if (event.getEventType() == ProgressEventType.TRANSFER_FAILED_EVENT) {
                logger.error("S3 - Error: Upload failed for {}", file.getName());
            } else if (event.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    logger.error(ExceptionUtils.getStackTrace(e));
                }
                logger.info("File {} uploaded to S3", s3Path + file.getName());
            }
        });
        s3.putObject(putRequest);
    }

    public void setStoragePrefix(String storagePrefix) {
        this.storagePrefix = storagePrefix;
    }
}

