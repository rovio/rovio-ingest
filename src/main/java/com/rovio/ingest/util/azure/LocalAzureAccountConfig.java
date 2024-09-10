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

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.druid.storage.azure.AzureAccountConfig;

public class LocalAzureAccountConfig extends AzureAccountConfig {
    @JsonProperty
    private String managedIdentityClientId;

    @SuppressWarnings("unused") // Used by Jackson deserialization?
    public void setManagedIdentityClientId(String managedIdentityClientId) {
        this.managedIdentityClientId = managedIdentityClientId;
    }

    public String getManagedIdentityClientId() {
        return managedIdentityClientId;
    }
}
