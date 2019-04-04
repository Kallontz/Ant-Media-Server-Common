package me.insertcoin.pufflive.antmedia.storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.gson.JsonObject;
import io.antmedia.storage.StorageClient;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PuffAmazonS3StorageClient extends StorageClient {
    protected static Logger logger = LoggerFactory.getLogger(PuffAmazonS3StorageClient.class);

    // Suppress warning to maintain consistency with JavaScript regular expression at PuffStreamsAPI
    @SuppressWarnings("RegExpRedundantEscape")
    private static final Pattern PATTERN_STREAM_KEY_NAME
            = Pattern.compile("^(\\b[0-9A-Fa-f]{4}\\b-\\d{4}-\\d{2}-\\d{2}\\/\\d{10}_.{6})_\\d{3}\\.mp4$");

    private static final long DEFAULT_FILE_PART_SIZE = 50 * 1024 * 1024; // 50MB
    private static long FILE_PART_SIZE = DEFAULT_FILE_PART_SIZE;

    private CloseableHttpClient client;
    private AmazonS3 s3Client;
    private BasicAWSCredentials awsCredentials;

    private String storagePrefix;
    private String apiBaseUri;
    private String apiArchiveCompleteUri;
    private String serverKey;

    public PuffAmazonS3StorageClient() {
        client = HttpClients.createMinimal();
    }

    private AmazonS3 getAmazonS3() {
        if (s3Client == null) {
            this.awsCredentials = new BasicAWSCredentials(getAccessKey(), getSecretKey());

            final AmazonS3ClientBuilder awsClientBuilder =
                    AmazonS3Client.builder().withCredentials(new AWSStaticCredentialsProvider(awsCredentials));

            awsClientBuilder.setRegion(getRegion());

            this.s3Client = awsClientBuilder.build();
        }
        return s3Client;
    }

    private TransferManager getTransperManager() {
        if (s3Client == null) {
            getAmazonS3();
        }

        return TransferManagerBuilder.standard()
                .withS3Client(s3Client)
//                .withMultipartUploadThreshold(FILE_PART_SIZE)
                .build();
    }


    public void delete(String fileName, FileType type) {
        AmazonS3 s3 = getAmazonS3();
        s3.deleteObject(getStorageName(), type.getValue() + "/" + fileName);

    }

    public boolean fileExist(String fileName, FileType type) {
        AmazonS3 s3 = getAmazonS3();
        return s3.doesObjectExist(getStorageName(), type.getValue() + "/" + fileName);
    }

    // TODO: Make fail-safe algorithm
    public void save(final File file, FileType type) {
        final String appName = getAppName(file.getName());

        if (appName != null) {
            String decodedPath;

            try {
                decodedPath = URLDecoder.decode(file.getPath(), StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                decodedPath = file.getPath();
            }

            final String bucketName = getStorageName();
            final String filePath = decodedPath;
            final String decodedFileName = decodedPath.substring(decodedPath.lastIndexOf(File.separator) + File.separator.length());
            final int pathPrefixEndIdx = filePath.indexOf("/streams/") + 9;
            final int pathSuffixStartIdx = filePath.indexOf(decodedFileName);


            final String relativePath = filePath.substring(pathPrefixEndIdx, pathSuffixStartIdx);
            final String s3Path = (storagePrefix != null ? storagePrefix : type.getValue()) + File.separator + relativePath;
            final String s3FileName = s3Path + decodedFileName;


            final TransferManager transferManager = getTransperManager();


            logger.info("-=-=-=> StandBy upload to BucketName {} Path {} name {} size {}",
                    bucketName, s3FileName, file.getName(), file.length());

//            Upload upload = transferManager.upload(bucketName, s3FileName, file);
            PutObjectRequest request = new PutObjectRequest(bucketName, s3FileName, file);

            // To receive notifications when bytes are transferred, add a
            // ProgressListener to your request.
            request.setGeneralProgressListener(event -> {
                if (event.getEventType() == ProgressEventType.TRANSFER_FAILED_EVENT) {
                    logger.error("-=-=-=> [{}] S3 Error: Upload failed for {}", decodedFileName, file.getName());
                } else if (event.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
                    logger.info("-=-=-=> File {}{} uploaded to S3", s3Path, file.getName());
                    if (putArchiveComplete(apiBaseUri, apiArchiveCompleteUri, getAppName(file.getName()))) {
                        try {
                            Files.delete(file.toPath());
                        } catch (IOException e) {
                            logger.error("Delete Failed... " + file.getName(), e);
                        }
                    }
                }
            });
            Upload upload = transferManager.upload(request);

            logger.info("-=-=-=> Started upload to BucketName {} Path {}", bucketName, s3FileName);
            try {
                upload.waitForCompletion();
//                if (putArchiveComplete(apiBaseUri, apiArchiveCompleteUri, getAppName(file.getName()))) {
//                    Files.delete(file.toPath());
//                }
                logger.info("-=-=-=> upload Complete BucketName {} Path {}", bucketName, s3FileName);
            } catch (InterruptedException e) {
                logger.error("-=-=-=> Unable to put object as multipart to Amazon S3 for file " + file.getName(), e);
//                e.printStackTrace();
            }

            /*
            List<PartETag> partETags = new ArrayList<PartETag>();
            List<MultiPartFileUploader> uploaders = new ArrayList<MultiPartFileUploader>();
            final TransferManager transferManager = getTransperManager();
//            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, s3Path + decodedFileName);
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, s3FileName);
            InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
            long contentLength = file.length();

            try {
                // Step 2: Upload parts.
                long filePosition = 0;
                long partSize = 0;
                for (int i = 1; filePosition < contentLength; i++) {
                    // Last part can be less than part size. Adjust part size.
                    partSize = Math.min(FILE_PART_SIZE, (contentLength - filePosition));

                    // Create request to upload a part.
                    UploadPartRequest uploadRequest =
                            new UploadPartRequest().
                                    withBucketName(bucketName).withKey(s3FileName).
                                    withUploadId(initResponse.getUploadId()).withPartNumber(i).
                                    withFileOffset(filePosition).
                                    withFile(file).
                                    withPartSize(partSize);

                    uploadRequest.setGeneralProgressListener(event -> {
                        if (event.getEventType() == ProgressEventType.TRANSFER_FAILED_EVENT) {
                            logger.error("[{}] S3 Error: Upload failed for {}", decodedFileName, file.getName());
                        } else if (event.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
                            logger.info("File {}{} uploaded to S3", s3Path, file.getName());
                        }
                    });

                    // Upload part and add response to our list.
                    MultiPartFileUploader uploader = new MultiPartFileUploader(s3Client, uploadRequest);
                    uploaders.add(uploader);
                    uploader.upload();

                    filePosition += partSize;
                }

                for (MultiPartFileUploader uploader : uploaders) {
                    uploader.join();
                    partETags.add(uploader.getPartETag());
                }
                // Step 3: complete.
                CompleteMultipartUploadRequest compRequest =
                        new CompleteMultipartUploadRequest(bucketName,
                                s3FileName,
                                initResponse.getUploadId(),
                                partETags);

                logger.info("[{}] partETags Size {} ", decodedFileName, partETags.size());
                logger.info("[{}] S3 Upload complete for {} bucket {} {}", decodedFileName, file.getName(), bucketName, s3FileName);
                s3Client.completeMultipartUpload(compRequest);

                if (putArchiveComplete(apiBaseUri, apiArchiveCompleteUri, getAppName(file.getName()))) {
                    logger.info("[{}] Called API complete for {}", decodedFileName, file.getName());
                    Files.delete(file.toPath());
                }
            }
            catch (Throwable t) {
                logger.error("Unable to put object as multipart to Amazon S3 for file " + file.getName(), t);
                s3Client.abortMultipartUpload(
                        new AbortMultipartUploadRequest(
                                bucketName, file.getName(), initResponse.getUploadId()));
            }
            */




            /*
            final AmazonS3 s3 = getAmazonS3();
            final PutObjectRequest putRequest = new PutObjectRequest(getStorageName(), s3Path + decodedFileName, file);

            putRequest.setCannedAcl(CannedAccessControlList.PublicRead);
            putRequest.setGeneralProgressListener(event -> {
                if (event.getEventType() == ProgressEventType.TRANSFER_FAILED_EVENT) {
                    logger.error("[{}] S3 Error: Upload failed for {}", decodedFileName, file.getName());
                } else if (event.getEventType() == ProgressEventType.TRANSFER_COMPLETED_EVENT) {
                    try {
                        if (putArchiveComplete(apiBaseUri, apiArchiveCompleteUri, getAppName(file.getName())))
                            Files.delete(file.toPath());
                    } catch (IOException e) {
                        logger.error(ExceptionUtils.getStackTrace(e));
                    }
                    logger.info("File {}{} uploaded to S3", s3Path, file.getName());
                }
            });
            s3.putObject(putRequest);
            */
        } else {
            logger.error("Parsing appName from [{}] has failed! S3 upload canceled!", file.getName());
        }
    }

    private static List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<PartETag>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }

    public void setStoragePrefix(String storagePrefix) {
        this.storagePrefix = storagePrefix;
    }

    public void setApiBaseUri(String apiBaseUri) {
        this.apiBaseUri = apiBaseUri;
    }

    public void setApiArchiveCompleteUri(String apiArchiveCompleteUri) {
        this.apiArchiveCompleteUri = apiArchiveCompleteUri;
    }

    public void setServerKey(String serverKey) {
        this.serverKey = serverKey;
    }

    private String getAppName(final String fileName) {
        try {
            final Matcher matcher = PATTERN_STREAM_KEY_NAME.matcher(URLDecoder.decode(fileName, StandardCharsets.UTF_8.name()));

            return matcher.find() ? matcher.group(1) : null;
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    private boolean putArchiveComplete(final String baseUri, final String endpointUri, final String streamKey) {
        try {
            final JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("app_name", streamKey);
            jsonObject.addProperty("server_key", serverKey);

            final HttpPut httpPut = new HttpPut(baseUri + endpointUri);
            httpPut.setEntity(new StringEntity(jsonObject.toString(), ContentType.APPLICATION_JSON));

            final CloseableHttpResponse response = client.execute(httpPut);
            final String responseContentType = response.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue();
            final ContentType contentType = ContentType.parse(responseContentType);

            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                final String responseApi = EntityUtils.toString(response.getEntity(), contentType.getCharset());
                logger.info("streamKey [{}] API success: {}", streamKey, responseApi);
                return true;
            } else {
                final String responseError = EntityUtils.toString(response.getEntity(), contentType.getCharset());
                logger.error("streamKey [{}] API error: {}", streamKey, responseError);
            }
        } catch (IOException e) {
            logger.error("streamKey [{}] IOException: {}", streamKey, e.getLocalizedMessage());
        }

        return false;
    }


    private static class MultiPartFileUploader extends Thread {

        private AmazonS3 s3Client;
        private UploadPartRequest uploadRequest;
        private PartETag partETag;

        MultiPartFileUploader(AmazonS3 s3Client, UploadPartRequest uploadRequest) {
            this.s3Client = s3Client;
            this.uploadRequest = uploadRequest;
        }

        @Override
        public void run() {
            partETag = s3Client.uploadPart(uploadRequest).getPartETag();
        }

        private PartETag getPartETag() {
            return partETag;
        }

        private void upload() {
            start();
        }
    }
}

