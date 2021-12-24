/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.vertx;

import org.junit.Test;

import java.util.Arrays;
import java.util.stream.IntStream;

public final class UnspecifiedTypeListTupleTest {
    
    @Test
    public void assertGetLocalDateTimeNoExceptionOccurs() {
        UnspecifiedTypeListTuple tuple = new UnspecifiedTypeListTuple(Arrays.asList(
                "20211012 2323",
                "20211012 232323",
                "2021-10-12 23:23:23",
                "2021-10-12 23:23:23.1",
                "2021-10-12 23:23:23.12",
                "2021-10-12 23:23:23.123",
                "2021-10-12 23:23:23.123+08",
                "2021-10-12T23:23:23.123+08",
                "2021-10-12 23:23:23.12345",
                "2021-10-12 23:23:23.12345+0800",
                "20211012 23:23:23.12345+0800",
                "2021-10-12 23:23:23.12345+08:00:00",
                "211012 23:23:23.12345+08:00:00",
                "10/12/21 23:23:23.12345+08:00:00",
                "2021-10-12 23:23:23.123456",
                "2021-10-12 23:23:23.123456 +08:00",
                "2021-10-12 23:23:23.123456 -08:00"));
        IntStream.range(0, tuple.size()).forEach(tuple::getLocalDateTime);
    }
}
