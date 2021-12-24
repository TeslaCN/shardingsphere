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
import java.util.List;

public class UnspecifiedTypeListTuple extends ListTuple {
    
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.S[S[S[S[S[S]]]]]][' '][XXX[XX]]");
    
    public UnspecifiedTypeListTuple(final List<Object> list) {
        super(list);
    }
    
    @Override
    public LocalDateTime getLocalDateTime(final int pos) {
        Object val = getValue(pos);
        if (val == null) {
            return null;
        } else if (val instanceof OffsetDateTime) {
            return ((OffsetDateTime) val).toLocalDateTime();
        } else if (val instanceof String) {
            return LocalDateTime.from(dateTimeFormatter.parse((CharSequence) val));
        } else {
            return (LocalDateTime) val;
        }
    }
}
