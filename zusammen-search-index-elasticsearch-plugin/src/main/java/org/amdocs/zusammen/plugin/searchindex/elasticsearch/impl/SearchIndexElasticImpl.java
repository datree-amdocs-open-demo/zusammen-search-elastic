/*
 * Copyright © 2016-2017 European Support Limited
 *
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
 */


package org.amdocs.zusammen.plugin.searchindex.elasticsearch.impl;


import org.amdocs.zusammen.commons.health.data.HealthInfo;
import org.amdocs.zusammen.commons.health.data.HealthStatus;
import org.amdocs.zusammen.commons.log.ZusammenLogger;
import org.amdocs.zusammen.commons.log.ZusammenLoggerFactory;
import org.amdocs.zusammen.datatypes.SessionContext;
import org.amdocs.zusammen.datatypes.response.Response;
import org.amdocs.zusammen.datatypes.response.ZusammenException;
import org.amdocs.zusammen.datatypes.searchindex.SearchCriteria;
import org.amdocs.zusammen.datatypes.searchindex.SearchResult;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.ElementSearchIndex;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.HealthHelper;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.SearchIndexServices;
import org.amdocs.zusammen.sdk.searchindex.types.SearchIndexElement;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;

public class SearchIndexElasticImpl implements org.amdocs.zusammen.sdk.searchindex.SearchIndex {

    private ElementSearchIndex elementSearchIndex;
    private SearchIndexServices searchIndexServices;
    private static final ZusammenLogger logger = ZusammenLoggerFactory.getLogger(SearchIndexElasticImpl.class.getSimpleName());

    @Override
    public Response<Void> createElement(SessionContext sessionContext, SearchIndexElement element) {
        getElementSearchIndex().createElement(sessionContext, element);
        return new Response(Void.TYPE);
    }

    @Override
    public Response<Void> updateElement(SessionContext sessionContext, SearchIndexElement element) {
        getElementSearchIndex().updateElement(sessionContext, element);
        return new Response(Void.TYPE);
    }

    @Override
    public Response<Void> deleteElement(SessionContext sessionContext, SearchIndexElement element) {
        getElementSearchIndex().deleteElement(sessionContext, element);
        return new Response(Void.TYPE);
    }

    @Override
    public Response<SearchResult> search(SessionContext sessionContext, SearchCriteria
            searchCriteria) {
        return new Response(getSearchIndexServices().search(sessionContext, searchCriteria));
    }

    @Override
    public Response<HealthInfo> checkHealth(SessionContext sessionContext) {
        HealthInfo retVal;
        try {
            ClusterHealthResponse clusterHealthResponse = getElementSearchIndex().checkHealth(sessionContext);
            ClusterHealthStatus status = clusterHealthResponse.getStatus();
            switch (status) {
                case RED:
                case YELLOW:
                    logger.error("Health Check failed ", clusterHealthResponse.toString());
                    retVal = new HealthInfo( HealthHelper.MODULE_NAME, HealthStatus.DOWN, "Cluster status is "+status);
                    break;

                case GREEN:
                    logger.error("Health Check failed ", clusterHealthResponse.toString());
                    retVal = new HealthInfo(HealthHelper.MODULE_NAME, HealthStatus.UP, "");
                    break;

                default:
                    throw new RuntimeException("Unexpected value");
            }
        } catch (Throwable e) {
            logger.error("Health Check failed ", e);
            retVal = new HealthInfo( HealthHelper.MODULE_NAME, HealthStatus.DOWN, e.getMessage());
        }

        return new Response<>(retVal);
    }

    private ElementSearchIndex getElementSearchIndex() {
        if (elementSearchIndex == null) {
            elementSearchIndex = new ElementSearchIndex();
        }
        return elementSearchIndex;
    }

    private SearchIndexServices getSearchIndexServices() {
        if (elementSearchIndex == null) {
            searchIndexServices = new SearchIndexServices();
        }
        return searchIndexServices;
    }

}