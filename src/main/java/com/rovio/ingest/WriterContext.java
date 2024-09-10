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
package com.rovio.ingest;

import com.google.common.base.Preconditions;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;

public class WriterContext implements Serializable {
    private static final long serialVersionUID = 1L;

    // Default values
    private static final String DEFAULT_QUERY_GRANULARITY = "DAY";
    private static final String DEFAULT_DRUID_METADATA_DB_TABLE_BASE = "druid";
    private static final String DEFAULT_DRUID_METADATA_DB_TYPE = "mysql";
    private static final int DEFAULT_MAX_ROWS_PER_SEGMENT = 5_000_000;
    private static final String DEFAULT_SEGMENT_GRANULARITY = "DAY";
    private static final int DEFAULT_MAX_ROWS_IN_MEMORY = 75000;
    public static final String DEFAULT_DRUID_DEEP_STORAGE_TYPE = "s3";
    // concise was the default until now, next release it is going to change to roaring bitmapserde
    public static final String DEFAULT_BITMAP_FACTORY = "concise";

    private final String dataSource;
    private final String timeColumn;
    private final String segmentGranularity;
    private final String queryGranularity;
    private final String bitmapFactory;
    private final List<String> excludedDimensions;
    private final int segmentMaxRows;
    private final int maxRowsInMemory;
    private final String metadataDbType;
    private final String metadataDbUri;
    private final String metadataDbUser;
    private final String metadataDbPass;
    private final String metadataDbTableBase;
    private final String s3Bucket;
    private final String s3BaseKey;
    private final boolean s3DisableAcl;
    private final String localDir;
    private final String hdfsDir;
    private final String hdfsCoreSitePath;
    private final String hdfsHdfsSitePath;
    private final String hdfsDefaultFS;
    private final String hdfsSecurityKerberosPrincipal;
    private final String hdfsSecurityKerberosKeytab;
    private final String azureAccount;
    private final String azureKey;
    private final String azureSharedAccessStorageToken;
    private final Boolean azureUseAzureCredentialsChain;
    private final String azureContainer;
    private final String azurePrefix;
    private final String azureManagedIdentityClientId;
    private final String azureProtocol;
    private final int azureMaxTries;
    private final int azureMaxListingLength;
    private final String azureEndpointSuffix;
    private final String deepStorageType;
    private final boolean initDataSource;
    private final String version;
    private final boolean rollup;
    private final boolean useDefaultValueForNull;
    private final boolean useThreeValueLogicForNativeFilters;
    private final String dimensionsSpec;
    private final String metricsSpec;
    private final String transformSpec;

    private WriterContext(CaseInsensitiveStringMap options, String version) {
        this.dataSource = getOrThrow(options, ConfKeys.DATA_SOURCE);
        this.timeColumn = getOrThrow(options, ConfKeys.TIME_COLUMN);
        this.segmentGranularity = options.getOrDefault(ConfKeys.SEGMENT_GRANULARITY, DEFAULT_SEGMENT_GRANULARITY);
        this.queryGranularity = options.getOrDefault(ConfKeys.QUERY_GRANULARITY, DEFAULT_QUERY_GRANULARITY);
        this.bitmapFactory = options.getOrDefault(ConfKeys.BITMAP_FACTORY, DEFAULT_BITMAP_FACTORY);
        this.excludedDimensions = Arrays.asList(options.getOrDefault(ConfKeys.EXCLUDED_DIMENSIONS, "").split(","));

        int segmentMaxRows = options.getInt(ConfKeys.SEGMENT_MAX_ROWS, DEFAULT_MAX_ROWS_PER_SEGMENT);
        if (segmentMaxRows > DEFAULT_MAX_ROWS_PER_SEGMENT || segmentMaxRows <= 0) {
            // Clamp max rows per segment.
            segmentMaxRows = DEFAULT_MAX_ROWS_PER_SEGMENT;
        }
        this.segmentMaxRows = segmentMaxRows;

        int maxRowsInMemory = options.getInt(ConfKeys.MAX_ROWS_IN_MEMORY, DEFAULT_MAX_ROWS_IN_MEMORY);
        if (maxRowsInMemory <= 0) {
            maxRowsInMemory = DEFAULT_MAX_ROWS_IN_MEMORY;
        }
        this.maxRowsInMemory = maxRowsInMemory;

        this.metadataDbType = options.getOrDefault(ConfKeys.METADATA_DB_TYPE, DEFAULT_DRUID_METADATA_DB_TYPE);
        this.metadataDbUri = getOrThrow(options, ConfKeys.METADATA_DB_URI);
        this.metadataDbUser = getOrThrow(options, ConfKeys.METADATA_DB_USERNAME);
        this.metadataDbPass = getOrThrow(options, ConfKeys.METADATA_DB_PASSWORD);
        this.metadataDbTableBase = options.getOrDefault(ConfKeys.METADATA_DB_TABLE_BASE, DEFAULT_DRUID_METADATA_DB_TABLE_BASE);

        this.s3Bucket = options.getOrDefault(ConfKeys.DEEP_STORAGE_S3_BUCKET, null);
        this.s3BaseKey = options.getOrDefault(ConfKeys.DEEP_STORAGE_S3_BASE_KEY, null);
        this.s3DisableAcl = options.getBoolean(ConfKeys.DEEP_STORAGE_S3_DISABLE_ACL, false);
        this.localDir = options.getOrDefault(ConfKeys.DEEP_STORAGE_LOCAL_DIRECTORY, null);
        this.hdfsDir = options.getOrDefault(ConfKeys.DEEP_STORAGE_HDFS_STORAGE_DIRECTORY, null);
        this.hdfsCoreSitePath = options.getOrDefault(ConfKeys.DEEP_STORAGE_HDFS_CORE_SITE_PATH, null);
        this.hdfsHdfsSitePath = options.getOrDefault(ConfKeys.DEEP_STORAGE_HDFS_HDFS_SITE_PATH, null);
        this.hdfsDefaultFS = options.getOrDefault(ConfKeys.DEEP_STORAGE_HDFS_DEFAULT_FS, null);
        this.hdfsSecurityKerberosPrincipal = options.getOrDefault(ConfKeys.DEEP_STORAGE_HDFS_SECURITY_KERBEROS_PRINCIPAL, null);
        this.hdfsSecurityKerberosKeytab = options.getOrDefault(ConfKeys.DEEP_STORAGE_HDFS_SECURITY_KERBEROS_KEYTAB, null);
        this.azureAccount = options.getOrDefault(ConfKeys.DEEP_STORAGE_AZURE_ACCOUNT, null);
        this.azureKey = options.getOrDefault(ConfKeys.DEEP_STORAGE_AZURE_KEY, null);
        this.azureSharedAccessStorageToken = options.getOrDefault(ConfKeys.DEEP_STORAGE_AZURE_SHAREDACCESSSTORAGETOKEN, null);
        this.azureUseAzureCredentialsChain = options.getBoolean(ConfKeys.DEEP_STORAGE_AZURE_USEAZURECRENDENTIALSCHAIN, false);
        this.azureContainer = options.getOrDefault(ConfKeys.DEEP_STORAGE_AZURE_CONTAINER, null);
        this.azurePrefix = options.getOrDefault(ConfKeys.DEEP_STORAGE_AZURE_PREFIX, "");
        this.azureProtocol = options.getOrDefault(ConfKeys.DEEP_STORAGE_AZURE_PROTOCOL, "https");
        this.azureMaxTries = options.getInt(ConfKeys.DEEP_STORAGE_AZURE_MAXTRIES, 3);
        this.azureMaxListingLength = options.getInt(ConfKeys.DEEP_STORAGE_AZURE_MAXLISTINGLENGTH, 1024);
        this.azureEndpointSuffix = options.getOrDefault(ConfKeys.DEEP_STORAGE_AZURE_ENDPOINTSUFFIX, "core.windows.net");
        this.azureManagedIdentityClientId = options.getOrDefault(ConfKeys.DEEP_STORAGE_AZURE_MANAGEDIDENTITYCLIENTID, null);

        this.deepStorageType = options.getOrDefault(ConfKeys.DEEP_STORAGE_TYPE, DEFAULT_DRUID_DEEP_STORAGE_TYPE);
        Preconditions.checkArgument(Arrays.asList("s3", "local", "hdfs", "azure").contains(this.deepStorageType),
                String.format("Invalid %s: %s", ConfKeys.DEEP_STORAGE_TYPE, this.deepStorageType));

        this.initDataSource = options.getBoolean(ConfKeys.DATASOURCE_INIT, false);
        this.rollup = options.getBoolean(ConfKeys.SEGMENT_ROLLUP, true);
        this.useDefaultValueForNull = options.getBoolean(ConfKeys.USE_DEFAULT_VALUES_FOR_NULL, true);
        this.useThreeValueLogicForNativeFilters = options.getBoolean(ConfKeys.USE_THREE_VALUE_LOGIC_FOR_NATIVE_FILTERS, true);
        this.dimensionsSpec = options.getOrDefault(ConfKeys.DIMENSIONS_SPEC, null);
        this.metricsSpec = options.getOrDefault(ConfKeys.METRICS_SPEC, null);
        this.transformSpec = options.getOrDefault(ConfKeys.TRANSFORM_SPEC, null);

        this.version = version;
    }

    public static WriterContext from(CaseInsensitiveStringMap options, String version) {
        return new WriterContext(options, version);
    }

    private static String getOrThrow(CaseInsensitiveStringMap options, String key) {
        if (options.containsKey(key)) {
            return options.get(key);
        }

        throw new IllegalArgumentException(format("Missing mandatory \"%s\" option", key));
    }

    public String getDataSource() {
        return dataSource;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    public String getSegmentGranularity() {
        return segmentGranularity;
    }

    public String getQueryGranularity() {
        return queryGranularity;
    }

    public String getBitmapFactory() {
        return bitmapFactory;
    }

    public List<String> getExcludedDimensions() {
        return excludedDimensions;
    }

    public int getSegmentMaxRows() {
        return segmentMaxRows;
    }

    public int getMaxRowsInMemory() {
        return maxRowsInMemory;
    }

    public String getMetadataDbType() {
        return metadataDbType;
    }

    public String getMetadataDbUri() {
        return metadataDbUri;
    }

    public String getMetadataDbUser() {
        return metadataDbUser;
    }

    public String getMetadataDbPass() {
        return metadataDbPass;
    }

    public String getMetadataDbTableBase() {
        return metadataDbTableBase;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3BaseKey() {
        return s3BaseKey;
    }

    public boolean isS3DisableAcl() {
        return s3DisableAcl;
    }

    public String getLocalDir() {
        return localDir;
    }

    public String getHdfsDir() {
        return hdfsDir;
    }

    public String getHdfsCoreSitePath() {
        return hdfsCoreSitePath;
    }

    public String getHdfsHdfsSitePath() {
        return hdfsHdfsSitePath;
    }

    public String getHdfsDefaultFS() {
        return hdfsDefaultFS;
    }

    public String getHdfsSecurityKerberosPrincipal() {
        return hdfsSecurityKerberosPrincipal;
    }

    public String getHdfsSecurityKerberosKeytab() {
        return hdfsSecurityKerberosKeytab;
    }

    public String getAzureAccount() {
        return azureAccount;
    }

    public String getAzureKey() {
        return azureKey;
    }

    public String getAzureSharedAccessStorageToken() {
        return azureSharedAccessStorageToken;
    }

    public Boolean getAzureUseAzureCredentialsChain() {
        return azureUseAzureCredentialsChain;
    }

    public String getAzureContainer() {
        return azureContainer;
    }

    public String getAzurePrefix() {
        return azurePrefix;
    }

    public String getAzureProtocol() {
        return azureProtocol;
    }

    public int getAzureMaxTries() {
        return azureMaxTries;
    }

    public int getAzureMaxListingLength() {
        return azureMaxListingLength;
    }

    public String getAzureEndpointSuffix() {
        return azureEndpointSuffix;
    }

    public String getAzureManagedIdentityClientId() {
        return azureManagedIdentityClientId;
    }

    public boolean isInitDataSource() {
        return initDataSource;
    }

    public String getVersion() {
        return version;
    }

    public boolean isLocalDeepStorage() {
        return "local".equals(deepStorageType);
    }

    public boolean isHdfsDeepStorage() {
        return "hdfs".equals(deepStorageType);
    }

    public boolean isAzureDeepStorage() {
        return "azure".equals(deepStorageType);
    }

    public boolean isRollup() {
        return rollup;
    }

    public boolean isUseDefaultValueForNull() {
        return useDefaultValueForNull;
    }

    public boolean isUseThreeValueLogicForNativeFilters() {
        return useThreeValueLogicForNativeFilters;
    }

    public String getDimensionsSpec() {
        return dimensionsSpec;
    }

    public String getMetricsSpec() {
        return metricsSpec;
    }

    public String getTransformSpec() {
        return transformSpec;
    }

    public static class ConfKeys {
        public static final String DATASOURCE_INIT = "druid.datasource.init";
        // Segment config
        public static final String DATA_SOURCE = "druid.datasource";
        public static final String TIME_COLUMN = "druid.time_column";
        public static final String DIMENSIONS_SPEC = "druid.dimensions_spec";
        public static final String METRICS_SPEC = "druid.metrics_spec";
        public static final String TRANSFORM_SPEC = "druid.transform_spec";
        public static final String SEGMENT_GRANULARITY = "druid.segment_granularity";
        public static final String QUERY_GRANULARITY = "druid.query_granularity";
        public static final String BITMAP_FACTORY = "druid.bitmap_factory";
        public static final String EXCLUDED_DIMENSIONS = "druid.exclude_dimensions";
        public static final String SEGMENT_MAX_ROWS = "druid.segment.max_rows";
        public static final String MAX_ROWS_IN_MEMORY = "druid.memory.max_rows";
        public static final String SEGMENT_ROLLUP = "druid.segment.rollup";
        public static final String USE_DEFAULT_VALUES_FOR_NULL = "druid.use_default_values_for_null";
        public static final String USE_THREE_VALUE_LOGIC_FOR_NATIVE_FILTERS = "druid.use_three_value_logic_for_native_filters";
        // Metadata config
        public static final String METADATA_DB_TYPE = "druid.metastore.db.type";
        public static final String METADATA_DB_URI = "druid.metastore.db.uri";
        public static final String METADATA_DB_USERNAME = "druid.metastore.db.username";
        public static final String METADATA_DB_PASSWORD = "druid.metastore.db.password";
        public static final String METADATA_DB_TABLE_BASE = "druid.metastore.db.table.base";
        // Deep Storage config
        public static final String DEEP_STORAGE_TYPE = "druid.segment_storage.type";
        // S3 config
        public static final String DEEP_STORAGE_S3_BUCKET = "druid.segment_storage.s3.bucket";
        public static final String DEEP_STORAGE_S3_BASE_KEY = "druid.segment_storage.s3.basekey";
        public static final String DEEP_STORAGE_S3_DISABLE_ACL = "druid.segment_storage.s3.disableacl";
        // Local config (only for testing)
        public static final String DEEP_STORAGE_LOCAL_DIRECTORY = "druid.segment_storage.local.dir";
        // HDFS config
        public static final String DEEP_STORAGE_HDFS_STORAGE_DIRECTORY = "druid.segment_storage.hdfs.dir";
        public static final String DEEP_STORAGE_HDFS_CORE_SITE_PATH = "druid.segment_storage.hdfs.core.site.path";
        public static final String DEEP_STORAGE_HDFS_HDFS_SITE_PATH = "druid.segment_storage.hdfs.hdfs.site.path";
        public static final String DEEP_STORAGE_HDFS_DEFAULT_FS = "druid.segment_storage.hdfs.default.fs";
        public static final String DEEP_STORAGE_HDFS_SECURITY_KERBEROS_PRINCIPAL = "druid.segment_storage.hdfs.security.kerberos.principal";
        public static final String DEEP_STORAGE_HDFS_SECURITY_KERBEROS_KEYTAB = "druid.segment_storage.hdfs.security.kerberos.keytab";
        // Azure config
        public static final String DEEP_STORAGE_AZURE_ACCOUNT = "druid.azure.account";
        public static final String DEEP_STORAGE_AZURE_KEY = "druid.azure.key";
        public static final String DEEP_STORAGE_AZURE_SHAREDACCESSSTORAGETOKEN = "druid.azure.sharedAccessStorageToken";
        public static final String DEEP_STORAGE_AZURE_USEAZURECRENDENTIALSCHAIN = "druid.azure.useAzureCredentialsChain";
        public static final String DEEP_STORAGE_AZURE_CONTAINER = "druid.azure.container";
        public static final String DEEP_STORAGE_AZURE_PREFIX = "druid.azure.prefix";
        public static final String DEEP_STORAGE_AZURE_PROTOCOL = "druid.azure.protocol";
        public static final String DEEP_STORAGE_AZURE_MAXTRIES = "druid.azure.maxTries";
        public static final String DEEP_STORAGE_AZURE_MAXLISTINGLENGTH = "druid.azure.maxListingLength";
        public static final String DEEP_STORAGE_AZURE_ENDPOINTSUFFIX = "druid.azure.endpointSuffix";
        public static final String DEEP_STORAGE_AZURE_MANAGEDIDENTITYCLIENTID = "druid.azure.managedIdentityClientId";
    }
}
