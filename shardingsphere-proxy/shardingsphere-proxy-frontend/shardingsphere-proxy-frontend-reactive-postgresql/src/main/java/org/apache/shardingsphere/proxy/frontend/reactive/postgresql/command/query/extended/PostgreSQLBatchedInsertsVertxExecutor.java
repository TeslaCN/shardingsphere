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

package org.apache.shardingsphere.proxy.frontend.reactive.postgresql.command.query.extended;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import lombok.SneakyThrows;
import org.apache.shardingsphere.db.protocol.postgresql.packet.command.query.extended.PostgreSQLPreparedStatement;
import org.apache.shardingsphere.infra.binder.LogicSQL;
import org.apache.shardingsphere.infra.binder.SQLStatementContextFactory;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.config.props.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.context.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.executor.check.SQLCheckEngine;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutionGroupContext;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionContext;
import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.vertx.UnspecifiedTypeListTuple;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.vertx.VertxExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.DriverExecutionPrepareEngine;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.vertx.VertxExecutionContext;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.proxy.backend.communication.vertx.VertxBackendConnection;
import org.apache.shardingsphere.proxy.backend.context.BackendExecutorContext;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.session.ConnectionSession;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Batched inserts executor for PostgreSQL.
 */
public final class PostgreSQLBatchedInsertsVertxExecutor {
    
    private final KernelProcessor kernelProcessor = new KernelProcessor();
    
    private final ConnectionSession connectionSession;
    
    private final MetaDataContexts metaDataContexts;
    
    private final PostgreSQLPreparedStatement preparedStatement;
    
    private final Map<ExecutionUnit, List<Tuple>> executionUnitParameters;
    
    private ExecutionContext anyExecutionContext;
    
    private ExecutionGroupContext<VertxExecutionUnit> executionGroupContext;
    
    public PostgreSQLBatchedInsertsVertxExecutor(final ConnectionSession connectionSession, final PostgreSQLPreparedStatement preparedStatement, final List<List<Object>> parameterSets) {
        this.connectionSession = connectionSession;
        metaDataContexts = ProxyContext.getInstance().getContextManager().getMetaDataContexts();
        this.preparedStatement = preparedStatement;
        executionUnitParameters = new HashMap<>();
        for (List<Object> eachGroupParameter : parameterSets) {
            ExecutionContext executionContext = createExecutionContext(createLogicSQL(eachGroupParameter));
            if (null == anyExecutionContext) {
                anyExecutionContext = executionContext;
            }
            for (ExecutionUnit each : executionContext.getExecutionUnits()) {
                executionUnitParameters.computeIfAbsent(each, unused -> new LinkedList<>()).add(new UnspecifiedTypeListTuple(each.getSqlUnit().getParameters()));
            }
        }
    }
    
    private LogicSQL createLogicSQL(final List<Object> parameters) {
        SQLStatementContext<?> sqlStatementContext = SQLStatementContextFactory.newInstance(
                metaDataContexts.getMetaDataMap(), parameters, preparedStatement.getSqlStatement(), connectionSession.getSchemaName());
        return new LogicSQL(sqlStatementContext, preparedStatement.getSql(), parameters);
    }
    
    private ExecutionContext createExecutionContext(final LogicSQL logicSQL) {
        SQLCheckEngine.check(logicSQL.getSqlStatementContext().getSqlStatement(), logicSQL.getParameters(),
                metaDataContexts.getMetaData(connectionSession.getSchemaName()).getRuleMetaData().getRules(), connectionSession.getSchemaName(), metaDataContexts.getMetaDataMap(), null);
        return kernelProcessor.generateExecutionContext(logicSQL, metaDataContexts.getMetaData(connectionSession.getSchemaName()), metaDataContexts.getProps());
    }
    
    /**
     * Execute batch.
     *
     * @return inserted rows
     */
    @SneakyThrows(SQLException.class)
    public Future<Integer> executeBatch() {
        addBatchedParametersToPreparedStatements();
        return executeBatchedPreparedStatements();
    }
    
    private void addBatchedParametersToPreparedStatements() throws SQLException {
        Collection<ShardingSphereRule> rules = metaDataContexts.getMetaData(connectionSession.getSchemaName()).getRuleMetaData().getRules();
        DriverExecutionPrepareEngine<VertxExecutionUnit, Future<? extends SqlClient>> prepareEngine = new DriverExecutionPrepareEngine<>(
                "Vert.x", metaDataContexts.getProps().<Integer>getValue(ConfigurationPropertyKey.MAX_CONNECTIONS_SIZE_PER_QUERY),
                (VertxBackendConnection) connectionSession.getBackendConnection(), new VertxExecutionContext(), rules);
        executionGroupContext = prepareEngine.prepare(anyExecutionContext.getRouteContext(), executionUnitParameters.keySet());
    }
    
    private Future<Integer> executeBatchedPreparedStatements() throws SQLException {
        ExecutorCallback<VertxExecutionUnit, Future<RowSet<Row>>> callback = new BatchedInsertsVertxExecutorCallback();
        List futures = BackendExecutorContext.getInstance().getExecutorEngine().execute(executionGroupContext, null, callback, true);
        return CompositeFuture.all(futures).compose(rowSets -> {
            int result = 0;
            for (Object o : rowSets.list()) {
                RowSet<Row> each = (RowSet<Row>) o;
                result += each.rowCount();
            }
            return Future.succeededFuture(result);
        });
    }
    
    private class BatchedInsertsVertxExecutorCallback implements ExecutorCallback<VertxExecutionUnit, Future<RowSet<Row>>> {
        
        @Override
        public Collection<Future<RowSet<Row>>> execute(final Collection<VertxExecutionUnit> inputs, final boolean isTrunkThread, final Map<String, Object> dataMap) throws SQLException {
            List<Future<RowSet<Row>>> result = new ArrayList<>(inputs.size());
            for (VertxExecutionUnit unit : inputs) {
                List<Tuple> tuples = executionUnitParameters.get(unit.getExecutionUnit());
                result.add(unit.getStorageResource().compose(pq -> pq.executeBatch(tuples)));
            }
            return result;
        }
    }
}
