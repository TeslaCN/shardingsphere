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

package org.apache.shardingsphere.sharding.strategy.algorithm.sharding.inline;

import com.google.common.base.Preconditions;
import groovy.lang.Closure;
import groovy.util.Expando;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;

import java.util.Collection;
import java.util.Properties;

/**
 * Inline sharding algorithm.
 */
public final class InlineShardingAlgorithm implements StandardShardingAlgorithm<Comparable<?>> {
    
    private static final String ALGORITHM_EXPRESSION = "algorithm.expression";
    
    private static final String ALLOW_RANGE_QUERY = "allow.range.query.with.inline.sharding";
    
    private Closure<?> closure;
    
    @Getter
    @Setter
    private Properties props = new Properties();
    
    @Override
    public void init() {
        Preconditions.checkNotNull(props.get(ALGORITHM_EXPRESSION), "Inline sharding algorithm expression cannot be null.");
        String algorithmExpression = InlineExpressionParser.handlePlaceHolder(props.get(ALGORITHM_EXPRESSION).toString().trim());
        Closure<?> closure = new InlineExpressionParser(algorithmExpression).evaluateClosure();
        this.closure = closure.rehydrate(new Expando(), null, null);
        this.closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    }
    
    @Override
    public String doSharding(final Collection<String> availableTargetNames, final PreciseShardingValue<Comparable<?>> shardingValue) {
        closure.setProperty(shardingValue.getColumnName(), shardingValue.getValue());
        return closure.call().toString();
    }
    
    @Override
    public Collection<String> doSharding(final Collection<String> availableTargetNames, final RangeShardingValue<Comparable<?>> shardingValue) {
        if (isAllowRangeQuery()) {
            return availableTargetNames;
        }
        throw new UnsupportedOperationException("Since the property of `allow.range.query.with.inline.sharding` is false, inline sharding algorithm can not tackle with range query.");
    }
    
    private boolean isAllowRangeQuery() {
        return null != props.get(ALLOW_RANGE_QUERY) && Boolean.parseBoolean(props.get(ALLOW_RANGE_QUERY).toString());
    }
    
    @Override
    public String getType() {
        return "INLINE";
    }
}
