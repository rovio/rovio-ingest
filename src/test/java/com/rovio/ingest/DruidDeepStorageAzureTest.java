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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

@Testcontainers
public class DruidDeepStorageAzureTest {
    public static String azureAccount = "user";
    public static String azureKey = "key";
    public static String azureContainer = "container";
    public static String azurePrefix = "prefix";
    public static String azureProtocol = "http";
    public static Integer azurePort = 10000;
    public static final DockerImageName AZURE_IMAGE = DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:latest");
    @Container
    public static GenericContainer<?> AZURE = getAzureContainer();

    public static GenericContainer<?> getAzureContainer() {
        return new GenericContainer<>(AZURE_IMAGE)
                .withEnv("AZURITE_ACCOUNTS", azureAccount + ":" + azureKey)
                .withExposedPorts(azurePort);
    }

    public static Integer getAzureMappedPort() {
        return AZURE.getMappedPort(azurePort);
    }

    public static String getAzureHost() {
        return AZURE.getHost();
    }

    public static String getAzureEndPointSuffix() {
        return getAzureHost() + ":" + getAzureMappedPort();
    }

    public static Map<String, String> getAzureOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(WriterContext.ConfKeys.DEEP_STORAGE_AZURE_ACCOUNT, azureAccount);
        options.put(WriterContext.ConfKeys.DEEP_STORAGE_AZURE_KEY, azureKey);
        options.put(WriterContext.ConfKeys.DEEP_STORAGE_AZURE_CONTAINER, azureContainer);
        options.put(WriterContext.ConfKeys.DEEP_STORAGE_AZURE_PREFIX, azurePrefix);
        options.put(WriterContext.ConfKeys.DEEP_STORAGE_AZURE_PROTOCOL, azureProtocol);
        options.put(WriterContext.ConfKeys.DEEP_STORAGE_AZURE_ENDPOINTSUFFIX, getAzureEndPointSuffix());
        return options;
    }
}
