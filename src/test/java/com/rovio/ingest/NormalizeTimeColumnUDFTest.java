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

import com.rovio.ingest.util.NormalizeTimeColumnUDF;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class NormalizeTimeColumnUDFTest {
    private final NormalizeTimeColumnUDF udf = new NormalizeTimeColumnUDF();
    private final Long instant = 1569961771384L;

    @Test
    public void shouldFailForInvalidGranularity() {
        assertThrows(IllegalArgumentException.class, () -> udf.call(instant, "dummy"));
    }

    @Test
    public void shouldFailForNoneGranularity() {
        assertThrows(IllegalArgumentException.class, () -> udf.call(instant, "NONE"));
    }

    @Test
    public void shouldFailForAllGranularity() {
        assertThrows(IllegalArgumentException.class, () -> udf.call(instant, "ALL"));
    }

    @Test
    public void shouldFailForPeriodGranularity() {
        assertThrows(IllegalArgumentException.class, () -> udf.call(instant, "PT5M"));
    }

    @Test
    public void shouldPassForDurationGranulaity() {
        DateTime dateTime = new DateTime(instant, DateTimeZone.UTC);
        assertEquals(dateTime.withTimeAtStartOfDay().getMillis(), (long) udf.call(instant, "DAY"));
        assertEquals(dateTime.withDayOfMonth(1).withTimeAtStartOfDay().getMillis(),
                (long) udf.call(instant, "MONTH"));
        assertEquals(dateTime.withMonthOfYear(1).withDayOfMonth(1).withTimeAtStartOfDay().getMillis(),
                (long) udf.call(instant, "YEAR"));
    }
}
