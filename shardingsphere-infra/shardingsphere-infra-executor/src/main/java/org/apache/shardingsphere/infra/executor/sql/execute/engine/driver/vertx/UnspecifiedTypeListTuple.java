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

import io.vertx.sqlclient.impl.ListTuple;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

/**
 * List tuple supports unspecified type in String.
 */
public final class UnspecifiedTypeListTuple extends ListTuple {
    
    private static final DateTimeFormatter LOCAL_DATE_TIME_FORMATTER;
    
    static {
        LOCAL_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern(
                "[yyyy-MM-dd][yyyy_MM_dd][MM/dd/yy][yyyyMMdd][yyMMdd]" +
                        "['T'][ ]" +
                        "[HH:mm:ss][HHmmss][HH:mm][HHmm]" +
                        "[.SSSSSS][.SSSSS][.SSSS][.SSS][.SS][.S]" +
                        "[ ]" +
                        "[XXXXX][XXXX][XXX][XX][X]"
        );
    }
    
    public UnspecifiedTypeListTuple(final List<Object> list) {
        super((list instanceof RandomAccess) ? list : new ArrayList<>(list));
    }
    
    @Override
    public LocalDateTime getLocalDateTime(final int pos) {
        Object val = getValue(pos);
        if (val == null) {
            return null;
        } else if (val instanceof OffsetDateTime) {
            return ((OffsetDateTime) val).toLocalDateTime();
        } else if (val instanceof String) {
            return LocalDateTime.from(LOCAL_DATE_TIME_FORMATTER.parse((CharSequence) val));
        } else {
            return (LocalDateTime) val;
        }
    }
}
