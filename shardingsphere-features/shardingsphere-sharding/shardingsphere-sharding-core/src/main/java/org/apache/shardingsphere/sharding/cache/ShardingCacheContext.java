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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.Set;

/**
 * Sharding cache context.
 */
public final class ShardingCacheContext {
    
    private final Set<Integer> parameterMarkerIndexes;
    
    private List<Object> previousParameters;
    
    public ShardingCacheContext(final List<Object> parameters, final Set<Integer> parameterMarkerIndexes) {
        this.parameterMarkerIndexes = parameterMarkerIndexes;
        previousParameters = ensureRandomAccess(parameters);
    }
    
    /**
     * Is reusable.
     *
     * @param currentParameters current parameters
     * @return is reusable
     */
    public boolean isReusable(final List<Object> currentParameters) {
        if (previousParameters.size() != currentParameters.size()) {
            previousParameters = ensureRandomAccess(currentParameters);
            return false;
        }
        List<Object> randomAccessCurrentParameters = ensureRandomAccess(currentParameters);
        boolean result = allShardingConditionsAreSame(randomAccessCurrentParameters);
        previousParameters = randomAccessCurrentParameters;
        return result;
    }
    
    private boolean allShardingConditionsAreSame(final List<Object> currentParameters) {
        for (int each : parameterMarkerIndexes) {
            if (each >= currentParameters.size() || !Objects.equals(previousParameters.get(each), currentParameters.get(each))) {
                return false;
            }
        }
        return true;
    }
    
    private static List<Object> ensureRandomAccess(final List<Object> parameters) {
        return parameters instanceof RandomAccess ? parameters : new ArrayList<>(parameters);
    }
}
