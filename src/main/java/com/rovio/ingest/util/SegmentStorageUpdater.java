/*
 * Copyright 2021 Rovio Entertainment Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.rovio.ingest.util;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.rovio.ingest.WriterContext;
import com.rovio.ingest.util.azure.LocalAzureAccountConfig;
import com.rovio.ingest.util.azure.LocalAzureClientFactory;
import com.rovio.ingest.util.azure.LocalAzureCloudBlobIterableFactory;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentKiller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.storage.azure.AzureDataSegmentConfig;
import org.apache.druid.storage.azure.AzureDataSegmentKiller;
import org.apache.druid.storage.azure.AzureDataSegmentPusher;
import org.apache.druid.storage.azure.AzureInputDataConfig;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.hdfs.HdfsDataSegmentKiller;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusher;
import org.apache.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import org.apache.druid.storage.hdfs.HdfsKerberosConfig;
import org.apache.druid.storage.hdfs.HdfsStorageAuthentication;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3DataSegmentKiller;
import org.apache.druid.storage.s3.S3DataSegmentPusher;
import org.apache.druid.storage.s3.S3DataSegmentPusherConfig;
import org.apache.druid.storage.s3.S3InputDataConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;

public class SegmentStorageUpdater {

    public static DataSegmentPusher createPusher(WriterContext param) {
        Preconditions.checkNotNull(param);
        if (param.isLocalDeepStorage()) {
            return new LocalDataSegmentPusher(getLocalConfig(param.getLocalDir()));
        } else if (param.isHdfsDeepStorage()) {
            return new HdfsDataSegmentPusher(
                    getHdfsConfig(
                            param.getHdfsDir(),
                            param.getHdfsSecurityKerberosPrincipal(),
                            param.getHdfsSecurityKerberosKeytab(),
                            getHdfsHadoopConfiguration(param.getHdfsCoreSitePath(), param.getHdfsHdfsSitePath(), param.getHdfsDefaultFS())
                    ),
                    getHdfsHadoopConfiguration(param.getHdfsCoreSitePath(), param.getHdfsHdfsSitePath(), param.getHdfsDefaultFS()),
                    MAPPER
            );
        } else if (param.isAzureDeepStorage()) {
            LocalAzureAccountConfig azureAccountConfig = new LocalAzureAccountConfig();
            azureAccountConfig.setAccount(param.getAzureAccount());
            if (param.getAzureKey() != null && !param.getAzureKey().isEmpty()) {
                azureAccountConfig.setKey(param.getAzureKey());
            }
            if (param.getAzureSharedAccessStorageToken() != null && !param.getAzureSharedAccessStorageToken().isEmpty()) {
                azureAccountConfig.setSharedAccessStorageToken(param.getAzureSharedAccessStorageToken());
            }
            if (param.getAzureEndpointSuffix() != null && !param.getAzureEndpointSuffix().isEmpty()) {
                azureAccountConfig.setEndpointSuffix(param.getAzureEndpointSuffix());
            }
            if (param.getAzureManagedIdentityClientId() != null && !param.getAzureManagedIdentityClientId().isEmpty()) {
                azureAccountConfig.setManagedIdentityClientId(param.getAzureManagedIdentityClientId());
            }
            azureAccountConfig.setUseAzureCredentialsChain(param.getAzureUseAzureCredentialsChain());
            azureAccountConfig.setProtocol(param.getAzureProtocol());
            azureAccountConfig.setMaxTries(param.getAzureMaxTries());
            LocalAzureClientFactory azureClientFactory = new LocalAzureClientFactory(azureAccountConfig);
            AzureStorage azureStorage = new AzureStorage(azureClientFactory);
            AzureDataSegmentConfig azureDataSegmentConfig = new AzureDataSegmentConfig();
            azureDataSegmentConfig.setContainer(param.getAzureContainer());
            azureDataSegmentConfig.setPrefix(param.getAzurePrefix());
            return new AzureDataSegmentPusher(azureStorage, azureAccountConfig, azureDataSegmentConfig);
        } else {
            ServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3 = getAmazonS3().get();
            S3DataSegmentPusherConfig s3Config = new S3DataSegmentPusherConfig();
            s3Config.setBucket(param.getS3Bucket());
            s3Config.setBaseKey(param.getS3BaseKey());
            s3Config.setUseS3aSchema(true);
            s3Config.setDisableAcl(param.isS3DisableAcl());
            return new S3DataSegmentPusher(serverSideEncryptingAmazonS3, s3Config);
        }
    }

    public static DataSegmentKiller createKiller(WriterContext param) {
        Preconditions.checkNotNull(param);
        if (param.isLocalDeepStorage()) {
            return new LocalDataSegmentKiller(getLocalConfig(param.getLocalDir()));
        } else if (param.isHdfsDeepStorage()) {
            return new HdfsDataSegmentKiller(
                    getHdfsHadoopConfiguration(param.getHdfsCoreSitePath(), param.getHdfsHdfsSitePath(), param.getHdfsDefaultFS()),
                    getHdfsConfig(
                            param.getHdfsDir(),
                            param.getHdfsSecurityKerberosPrincipal(),
                            param.getHdfsSecurityKerberosKeytab(),
                            getHdfsHadoopConfiguration(param.getHdfsCoreSitePath(), param.getHdfsHdfsSitePath(), param.getHdfsDefaultFS())
                    )
            );
        } else if (param.isAzureDeepStorage()) {

            LocalAzureAccountConfig azureAccountConfig = new LocalAzureAccountConfig();
            azureAccountConfig.setAccount(param.getAzureAccount());
            if (param.getAzureKey() != null && !param.getAzureKey().isEmpty()) {
                azureAccountConfig.setKey(param.getAzureKey());
            }
            if (param.getAzureSharedAccessStorageToken() != null && !param.getAzureSharedAccessStorageToken().isEmpty()) {
                azureAccountConfig.setSharedAccessStorageToken(param.getAzureSharedAccessStorageToken());
            }
            if (param.getAzureEndpointSuffix() != null && !param.getAzureEndpointSuffix().isEmpty()) {
                azureAccountConfig.setEndpointSuffix(param.getAzureEndpointSuffix());
            }
            if (param.getAzureManagedIdentityClientId() != null && !param.getAzureManagedIdentityClientId().isEmpty()) {
                azureAccountConfig.setManagedIdentityClientId(param.getAzureManagedIdentityClientId());
            }
            azureAccountConfig.setUseAzureCredentialsChain(param.getAzureUseAzureCredentialsChain());
            azureAccountConfig.setProtocol(param.getAzureProtocol());
            azureAccountConfig.setMaxTries(param.getAzureMaxTries());
            LocalAzureClientFactory azureClientFactory = new LocalAzureClientFactory(azureAccountConfig);
            AzureStorage azureStorage = new AzureStorage(azureClientFactory);
            AzureDataSegmentConfig azureDataSegmentConfig = new AzureDataSegmentConfig();
            azureDataSegmentConfig.setContainer(param.getAzureContainer());
            azureDataSegmentConfig.setPrefix(param.getAzurePrefix());
            AzureInputDataConfig azureInputDataConfig = new AzureInputDataConfig();
            azureInputDataConfig.setMaxListingLength(param.getAzureMaxListingLength());
            LocalAzureCloudBlobIterableFactory azureFactory = new LocalAzureCloudBlobIterableFactory();
            return new AzureDataSegmentKiller(azureDataSegmentConfig, azureInputDataConfig, azureAccountConfig, azureStorage, azureFactory);
        } else {
            Supplier<ServerSideEncryptingAmazonS3> serverSideEncryptingAmazonS3 = getAmazonS3();
            S3DataSegmentPusherConfig s3Config = new S3DataSegmentPusherConfig();
            s3Config.setBucket(param.getS3Bucket());
            s3Config.setBaseKey(param.getS3BaseKey());
            s3Config.setUseS3aSchema(true);
            s3Config.setDisableAcl(param.isS3DisableAcl());
            return new S3DataSegmentKiller(serverSideEncryptingAmazonS3, s3Config, new S3InputDataConfig());
        }
    }

    private static Supplier<ServerSideEncryptingAmazonS3> getAmazonS3() {
        return Suppliers.memoize(() -> new ServerSideEncryptingAmazonS3(
                AmazonS3ClientBuilder.defaultClient(),
                new NoopServerSideEncryption()));
    }

    private static LocalDataSegmentPusherConfig getLocalConfig(String localDir) {
        return Suppliers.memoize(() -> {
            LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
            if (localDir != null) {
                config.storageDirectory = new File(localDir);
            }
            return config;
        }).get();
    }

    private static HdfsDataSegmentPusherConfig getHdfsConfig(String hdfsDir, String kerberosPrincipal, String kerberosKeytab, Configuration conf) {
        return Suppliers.memoize(() -> {
            HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
            if (hdfsDir != null) {
                config.setStorageDirectory(hdfsDir);
            }
            if (kerberosPrincipal != null && kerberosKeytab != null) {
                HdfsKerberosConfig hdfsKerberosConfig = new HdfsKerberosConfig(kerberosPrincipal, kerberosKeytab);
                HdfsStorageAuthentication hdfsAuth = new HdfsStorageAuthentication(hdfsKerberosConfig, conf);
                hdfsAuth.authenticate();
            }
            return config;
        }).get();
    }

    private static Configuration getHdfsHadoopConfiguration(String hdfsCoreSitePath, String hdfsHdfsSitePath, String defaultFS) {
        return Suppliers.memoize(() -> {
            if (hdfsCoreSitePath == null || hdfsHdfsSitePath == null) {
                throw new UnsupportedOperationException("Custom hdfs site configuration is not implemented");
            } else {
                Configuration configuration = new Configuration(true);
                if (defaultFS != null) {
                    configuration.set("fs.defaultFS", defaultFS);
                }
                return configuration;
            }
        }).get();
    }
}
