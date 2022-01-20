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

package io.vertx.pgclient.impl.codec;

import io.vertx.sqlclient.desc.ColumnDescriptor;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;

import java.sql.SQLException;
import java.util.List;

/**
 * Query result meta data for Vert.x PostgreSQL.
 */
@RequiredArgsConstructor
public final class VertxPostgreSQLQueryResultMetaData implements QueryResultMetaData {
    
    private final List<ColumnDescriptor> columns;
    
    @Override
    public int getColumnCount() throws SQLException {
        return columns.size();
    }
    
    @Override
    public String getTableName(final int columnIndex) throws SQLException {
        return null;
    }
    
    @Override
    public String getColumnName(final int columnIndex) throws SQLException {
        return columns.get(columnIndex - 1).name();
    }
    
    @Override
    public String getColumnLabel(final int columnIndex) throws SQLException {
        return columns.get(columnIndex - 1).name();
    }
    
    @Override
    public int getColumnType(final int columnIndex) throws SQLException {
        return columns.get(columnIndex - 1).jdbcType().getVendorTypeNumber();
    }
    
    @Override
    public String getColumnTypeName(final int columnIndex) throws SQLException {
        return columns.get(columnIndex - 1).typeName();
    }
    
    @Override
    public int getColumnLength(final int columnIndex) throws SQLException {
        // TODO PgColumnDesc is package-private
        return ((PgColumnDesc) columns.get(columnIndex - 1)).length;
    }
    
    @Override
    public int getDecimals(final int columnIndex) throws SQLException {
        return 0;
    }
    
    @Override
    public boolean isSigned(final int columnIndex) throws SQLException {
        return false;
    }
    
    @Override
    public boolean isNotNull(final int columnIndex) throws SQLException {
        return false;
    }
    
    @Override
    public boolean isAutoIncrement(final int columnIndex) throws SQLException {
        return false;
    }
}
