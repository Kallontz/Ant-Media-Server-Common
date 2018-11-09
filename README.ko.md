Ant Media Server Common
==================

Issue는 주 repository에 생성하십시오. https://github.com/ant-media/Ant-Media-Server/issues

## PUFF 특화 기능

`PuffAmazonS3StorageClient` 클래스가 `me.insertcoin.pufflive.antmedia.storage` 패키지 안에 추가되었습니다.

이 스토리지는 스트림 키의 폴더구조를 인식하여 AWS S3에 파일을 업로드할 때 사용합니다.

`PuffAmazonS3StorageClient` Java bean의 `storagePrefix`라는 새로운 매개변수는 AWS S3 버켓의 상위 폴더들을 설정합니다.

샘플 설정은 다음과 같습니다.

```
<bean id="app.storageClient" class="me.insertcoin.pufflive.antmedia.storage.PuffAmazonS3StorageClient">
    <property name="accessKey" value="Enter your S3_ACCESS_KEY" />
    <property name="secretKey" value="Enter your S3_SECRET_KEY" />
    <property name="region" value="ap-northeast-2" />
    <property name="storageName" value="puff-development-media" />
    <property name="storagePrefix" value="live" />
</bean>
```

설정 파일의 위치는 다음의 링크를 참조하십시오.

[Reference: Amazon (AWS) S3 Integration](https://github.com/ant-media/Ant-Media-Server/wiki/Amazon-(AWS)-S3-Integration#enable-aws-s3-in-your-streaming-app)
