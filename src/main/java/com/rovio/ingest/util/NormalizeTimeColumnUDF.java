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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.granularity.Granularity;

import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.spark.sql.api.java.UDF2;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class NormalizeTimeColumnUDF implements UDF2<Long, String, Long> {

    private static final long serialVersionUID = 1L;

    /**
     * @param instant epoch milliseconds
     * @param granularityString {@link Granularity} type name
     * @return epoch milliseconds which is the start interval given instant for {@link Granularity} specified by granularityString
     */
    @Override
    public Long call(Long instant, String granularityString) {
        Preconditions.checkArgument(instant != null, "instant cannot be null");
        return normalize(instant, granularityString);
    }

    public static long normalize(long instant, String granularityString) {
        Preconditions.checkArgument(granularityString != null, "granularityString cannot be null");
        Granularity granularity = Granularity.fromString(granularityString);
        Preconditions.checkArgument(granularity instanceof PeriodGranularity, "Only PeriodGranularity type supported");
        return granularity.bucket(new DateTime(instant, DateTimeZone.UTC)).getStartMillis();
    }
}
