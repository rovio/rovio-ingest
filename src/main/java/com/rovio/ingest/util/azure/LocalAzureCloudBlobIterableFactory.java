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
package com.rovio.ingest.util.azure;

import org.apache.druid.storage.azure.AzureCloudBlobIterable;
import org.apache.druid.storage.azure.AzureCloudBlobIterableFactory;
import org.apache.druid.storage.azure.AzureCloudBlobIteratorFactory;

import java.net.URI;

public class LocalAzureCloudBlobIterableFactory implements AzureCloudBlobIterableFactory {
    AzureCloudBlobIteratorFactory azureCloudBlobIteratorFactory;

    @Override
    public AzureCloudBlobIterable create(Iterable<URI> prefixes, int maxListingLength) {
        return new AzureCloudBlobIterable(azureCloudBlobIteratorFactory, prefixes, maxListingLength);
    }
}
