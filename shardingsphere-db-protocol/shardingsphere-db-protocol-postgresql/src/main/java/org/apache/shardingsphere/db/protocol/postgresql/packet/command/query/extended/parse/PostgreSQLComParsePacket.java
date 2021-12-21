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

package org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.parse;

import lombok.Getter;
import lombok.ToString;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.PostgreSQLCommandPacket;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.PostgreSQLCommandPacketType;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLColumnType;
import org.apache.shardingsphere.db.protocol.postgresql.packet.identifier.PostgreSQLIdentifierTag;
import org.apache.shardingsphere.db.protocol.postgresql.payload.PostgreSQLPacketPayload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Command parse packet for PostgreSQL.
 */
@Getter
@ToString
public final class PostgreSQLComParsePacket extends PostgreSQLCommandPacket {
    
    private final String statementId;
    
    private final String sql;
    
    private final List<PostgreSQLColumnType> columnTypes;
    
    public PostgreSQLComParsePacket(final PostgreSQLPacketPayload payload) {
        payload.readInt4();
        statementId = payload.readStringNul();
        sql = payload.readStringNul();
        columnTypes = sql.isEmpty() ? Collections.emptyList() : getParameterTypes(payload);
    }
    
    private List<PostgreSQLColumnType> getParameterTypes(final PostgreSQLPacketPayload payload) {
        int parameterCount = payload.readInt2();
        List<PostgreSQLColumnType> result = new ArrayList<>(parameterCount);
        for (int i = 0; i < parameterCount; i++) {
            result.add(PostgreSQLColumnType.valueOf(payload.readInt4()));
        }
        return result;
    }
    
    @Override
    public void write(final PostgreSQLPacketPayload payload) {
    }
    
    @Override
    public PostgreSQLIdentifierTag getIdentifier() {
        return PostgreSQLCommandPacketType.PARSE_COMMAND;
    }
}
