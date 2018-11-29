package me.insertcoin.pufflive.antmedia.storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PuffAmazonS3StorageClient extends StorageClient {
    protected static Logger logger = LoggerFactory.getLogger(PuffAmazonS3StorageClient.class);

    // Suppress warning to maintain consistency with JavaScript regular expression at PuffStreamsAPI
    @SuppressWarnings("RegExpRedundantEscape")
    private static final Pattern PATTERN_STREAM_KEY_NAME
            = Pattern.compile("^(\\b[0-9A-Fa-f]{4}\\b-\\d{4}-\\d{2}-\\d{2}\\/\\d{10}_.{6})_\\d{3}\\.mp4$");

    private CloseableHttpClient client;
    private AmazonS3 amazonS3;

    private String storagePrefix;
    private String apiBaseUri;
    private String apiArchiveCompleteUri;
    private String serverKey;

    public PuffAmazonS3StorageClient() {
        client = HttpClients.createMinimal();
    }

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
        } else {
            logger.error("Parsing appName from [{}] has failed! S3 upload canceled!", file.getName());
        }
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
}

