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

package org.apache.shardingsphere.sharding.cache;

import com.google.common.collect.Range;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.DeleteStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.UpdateStatementContext;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingCondition;
import org.apache.shardingsphere.sharding.route.engine.condition.engine.impl.WhereClauseShardingConditionEngine;
import org.apache.shardingsphere.sharding.route.engine.condition.value.ListShardingConditionValue;
import org.apache.shardingsphere.sharding.route.engine.condition.value.RangeShardingConditionValue;
import org.apache.shardingsphere.sharding.route.engine.condition.value.ShardingConditionValue;
import org.apache.shardingsphere.sharding.rule.ShardingRule;

import java.lang.ref.SoftReference;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sharding cache.
 */
public final class ShardingCache {
    
    private SoftReference<Map<SQLStatementContext<?>, ShardingCacheContext>> cacheReference = new SoftReference<>(new ConcurrentHashMap<>());
    
    /**
     * Is reusable.
     *
     * @param shardingRule sharding rule
     * @param database database
     * @param sqlStatementContext SQL statement context
     * @param parameters parameters
     * @return is reusable
     */
    public boolean isReusable(final ShardingRule shardingRule, final ShardingSphereDatabase database, final SQLStatementContext<?> sqlStatementContext, final List<Object> parameters) {
        if (!(sqlStatementContext instanceof SelectStatementContext || sqlStatementContext instanceof UpdateStatementContext || sqlStatementContext instanceof DeleteStatementContext)) {
            return false;
        }
        Map<SQLStatementContext<?>, ShardingCacheContext> cacheMap = getCacheMap();
        ShardingCacheContext shardingCacheContext = cacheMap.get(sqlStatementContext);
        if (null != shardingCacheContext) {
            return shardingCacheContext.isReusable(parameters);
        }
        List<ShardingCondition> shardingConditions = new WhereClauseShardingConditionEngine(shardingRule, database).createShardingConditions(sqlStatementContext, parameters);
        Set<Integer> parameterMarkerIndexes = new HashSet<>();
        for (ShardingCondition each : shardingConditions) {
            for (ShardingConditionValue conditionValue : each.getValues()) {
                if (!isConditionValueCacheable(conditionValue)) {
                    return false;
                }
                parameterMarkerIndexes.addAll(conditionValue.getParameterMarkerIndexes());
            }
        }
        cacheMap.put(sqlStatementContext, new ShardingCacheContext(parameters, parameterMarkerIndexes));
        return false;
    }
    
    private Map<SQLStatementContext<?>, ShardingCacheContext> getCacheMap() {
        Map<SQLStatementContext<?>, ShardingCacheContext> result = cacheReference.get();
        if (null == result) {
            synchronized (this) {
                if (null == (result = cacheReference.get())) {
                    result = new ConcurrentHashMap<>();
                    cacheReference = new SoftReference<>(result);
                }
            }
        }
        return result;
    }
    
    private static boolean isConditionValueCacheable(final ShardingConditionValue conditionValue) {
        if (conditionValue instanceof ListShardingConditionValue<?>) {
            for (Comparable<?> eachValue : ((ListShardingConditionValue<?>) conditionValue).getValues()) {
                if (!(eachValue instanceof Number)) {
                    return false;
                }
            }
        }
        if (conditionValue instanceof RangeShardingConditionValue<?>) {
            Range<?> range = ((RangeShardingConditionValue<?>) conditionValue).getValueRange();
            return range.lowerEndpoint() instanceof Number && range.upperEndpoint() instanceof Number;
        }
        return true;
    }
}
