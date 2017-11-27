/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingjdbc.orchestration.internal.state.datasource;

import io.shardingjdbc.core.exception.ShardingJdbcException;
import io.shardingjdbc.core.jdbc.core.datasource.MasterSlaveDataSource;
import io.shardingjdbc.core.jdbc.core.datasource.ShardingDataSource;
import io.shardingjdbc.orchestration.api.config.OrchestrationConfiguration;
import io.shardingjdbc.orchestration.internal.config.ConfigurationService;
import io.shardingjdbc.orchestration.internal.listener.ListenerManager;
import io.shardingjdbc.orchestration.internal.state.StateNode;
import io.shardingjdbc.orchestration.reg.listener.DataChangedEvent;
import io.shardingjdbc.orchestration.reg.listener.EventListener;
import io.shardingjdbc.orchestration.reg.api.CoordinatorRegistryCenter;

import java.sql.SQLException;

/**
 * Data source listener manager.
 *
 * @author caohao
 */
public final class DataSourceListenerManager implements ListenerManager {
    
    private final StateNode stateNode;
    
    private final CoordinatorRegistryCenter regCenter;
    
    private final ConfigurationService configurationService;
    
    private final DataSourceService dataSourceService;
    
    public DataSourceListenerManager(final OrchestrationConfiguration config, final CoordinatorRegistryCenter regCenter) {
        stateNode = new StateNode(config.getName());
        this.regCenter = regCenter;
        configurationService = new ConfigurationService(config, regCenter);
        dataSourceService = new DataSourceService(config, regCenter);
    }
    
    @Override
    public void start(final ShardingDataSource shardingDataSource) {
        regCenter.watch(stateNode.getDataSourcesNodeFullPath(), new EventListener() {
            
            @Override
            public void onChange(final DataChangedEvent event) {
                if (DataChangedEvent.Type.UPDATED == event.getEventType() || DataChangedEvent.Type.DELETED == event.getEventType()) {
                    try {
                        shardingDataSource.renew(dataSourceService.getAvailableShardingRule(), configurationService.loadShardingProperties());
                    } catch (final SQLException ex) {
                        throw new ShardingJdbcException(ex);
                    }
                }
            }
        });
    }
    
    @Override
    public void start(final MasterSlaveDataSource masterSlaveDataSource) {
        regCenter.watch(stateNode.getDataSourcesNodeFullPath(), new EventListener() {
            
            @Override
            public void onChange(final DataChangedEvent event) {
                if (DataChangedEvent.Type.UPDATED == event.getEventType() || DataChangedEvent.Type.DELETED == event.getEventType()) {
                    masterSlaveDataSource.renew(dataSourceService.getAvailableMasterSlaveRule());
                }
            }
        });
    }
}
