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

package org.amdocs.zusammen.plugin.searchindex.elasticsearch.dao.impl;

import org.amdocs.zusammen.datatypes.SessionContext;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.EsClientServices;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.EsConfig;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.dao.ElasticSearchDao;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.datatypes.EsSearchCriteria;
import org.amdocs.zusammen.utils.fileutils.json.JsonUtil;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Objects;

public class ElasticSearchDaoImpl implements ElasticSearchDao{

  @Override
  public IndexResponse create(SessionContext sessionContext, String index, String type,
                              String source, String id) {
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      return transportClient.prepareIndex(index, type, id).setSource(source).get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (Objects.nonNull(transportClient)) {
        clientServices.stop(sessionContext, transportClient);
      }
    }
  }

  @Override
  public GetResponse get(SessionContext sessionContext, String index, String type,
                         String id) throws IndexNotFoundException {
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      return transportClient.prepareGet(index, type, id).get();

    } catch (IndexNotFoundException indexNotFoundExc) {
      throw indexNotFoundExc;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (Objects.nonNull(transportClient)) {
        clientServices.stop(sessionContext, transportClient);
      }
    }
  }


  //Update by merging documents
  @Override
  public UpdateResponse update(SessionContext sessionContext, String index, String type,
                               String source, String id) {
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      return transportClient.prepareUpdate(index, type, id).setDoc(source).get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (Objects.nonNull(transportClient)) {
        clientServices.stop(sessionContext, transportClient);
      }
    }
  }

  @Override
  public DeleteResponse delete(SessionContext sessionContext, String index, String type,
                               String id) {
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      return transportClient.prepareDelete(index, type, id).get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (Objects.nonNull(transportClient)) {
        clientServices.stop(sessionContext, transportClient);
      }
    }

  }

  @Override
  public SearchResponse search(SessionContext sessionContext, String index,
                               EsSearchCriteria searchCriteria) {
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index);
      searchRequestBuilder.setSearchType(SearchType.DEFAULT);
      searchRequestBuilder.setExplain(false);

      if (Objects.nonNull(searchCriteria.getQuery())) {
        searchRequestBuilder.setQuery(
            QueryBuilders.wrapperQuery(JsonUtil.inputStream2Json(searchCriteria.getQuery())));
      }
      if (Objects.nonNull(searchCriteria.getFromPage())) {
        searchRequestBuilder.setFrom(searchCriteria.getFromPage());
      }
      if (Objects.nonNull(searchCriteria.getPageSize())) {
        searchRequestBuilder.setSize(searchCriteria.getPageSize());
      }

      if (Objects.nonNull(searchCriteria.getTypes()) && searchCriteria.getTypes().size() > 0) {
        searchRequestBuilder.setTypes(searchCriteria.getTypes().toArray(new String[searchCriteria
            .getTypes().size()]));
      }

      return searchRequestBuilder.get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (Objects.nonNull(transportClient)) {
        clientServices.stop(sessionContext, transportClient);
      }
    }

  }


}
