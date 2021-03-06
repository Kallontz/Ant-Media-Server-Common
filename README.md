Ant Media Server Common
==================

Please open issues on main repository https://github.com/ant-media/Ant-Media-Server/issues

## Customization for PUFF

`PuffAmazonS3StorageClient` class added in `me.insertcoin.pufflive.antmedia.storage` package.  
This storage detect subdirectories in stream key and use when upload files to AWS S3.  

New param, `storagePrefix`, in `PuffAmazonS3StorageClient` Java bean is indicating parent directories in AWS S3 bucket.

Sample setting is as follows.

```
<bean id="app.storageClient" class="me.insertcoin.pufflive.antmedia.storage.PuffAmazonS3StorageClient">
    <property name="accessKey" value="Enter your S3_ACCESS_KEY" />
    <property name="secretKey" value="Enter your S3_SECRET_KEY" />
    <property name="region" value="ap-northeast-2" />
    <property name="storageName" value="puff-development-media" />
    <property name="storagePrefix" value="live" />
</bean>
```

Location of configuration file can be found in follow link.

[Reference: Amazon (AWS) S3 Integration](https://github.com/ant-media/Ant-Media-Server/wiki/Amazon-(AWS)-S3-Integration#enable-aws-s3-in-your-streaming-app)
