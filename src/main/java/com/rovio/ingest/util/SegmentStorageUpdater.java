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
import com.google.common.base.Suppliers;
import com.rovio.ingest.WriterContext;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentKiller;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3DataSegmentKiller;
import org.apache.druid.storage.s3.S3DataSegmentPusher;
import org.apache.druid.storage.s3.S3DataSegmentPusherConfig;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import java.io.File;

import static com.rovio.ingest.DataSegmentCommitMessage.MAPPER;

public class SegmentStorageUpdater {

    public static DataSegmentPusher createPusher(WriterContext param) {
        Preconditions.checkNotNull(param);
        if (param.isLocalDeepStorage()) {
            return new LocalDataSegmentPusher(getLocalConfig(param.getLocalDir()), MAPPER);
        } else {
            ServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3 = getAmazonS3();
            S3DataSegmentPusherConfig s3Config = new S3DataSegmentPusherConfig();
            s3Config.setBucket(param.getS3Bucket());
            s3Config.setBaseKey(param.getS3BaseKey());
            s3Config.setUseS3aSchema(true);
            return new S3DataSegmentPusher(serverSideEncryptingAmazonS3, s3Config, MAPPER);
        }
    }

    public static DataSegmentKiller createKiller(WriterContext param) {
        Preconditions.checkNotNull(param);
        if (param.isLocalDeepStorage()) {
            return new LocalDataSegmentKiller(getLocalConfig(param.getLocalDir()));
        } else {
            ServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3 = getAmazonS3();
            return new S3DataSegmentKiller(serverSideEncryptingAmazonS3);
        }
    }

    private static ServerSideEncryptingAmazonS3 getAmazonS3() {
        return Suppliers.memoize(() -> new ServerSideEncryptingAmazonS3(
                AmazonS3ClientBuilder.defaultClient(),
                new NoopServerSideEncryption())).get();
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
}
