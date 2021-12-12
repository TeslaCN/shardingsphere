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

package org.apache.shardingsphere.proxy.backend.communication;

import io.vertx.core.Future;
import org.apache.shardingsphere.infra.binder.LogicSQL;
import org.apache.shardingsphere.infra.context.kernel.KernelProcessor;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.proxy.backend.response.header.ResponseHeader;
import org.apache.shardingsphere.proxy.backend.response.header.query.impl.QueryHeader;

import java.util.List;

public final class ReactiveDatabaseCommunicationEngine {
    
    private final KernelProcessor kernelProcessor = new KernelProcessor();
    
    private final LogicSQL logicSQL;
    
//    private final ShardingSphereMetaData metaData;
    
//    private final MetaDataRefreshEngine metadataRefreshEngine;
    
    private List<QueryHeader> queryHeaders;
    
    private MergedResult mergedResult;
    
    public ReactiveDatabaseCommunicationEngine(final LogicSQL logicSQL) {
        this.logicSQL = logicSQL;
    }
    
    /**
     * Execute future.
     *
     * @return future of response header
     */
    public Future<ResponseHeader> executeFuture() {
        throw new UnsupportedOperationException();
    }
}
