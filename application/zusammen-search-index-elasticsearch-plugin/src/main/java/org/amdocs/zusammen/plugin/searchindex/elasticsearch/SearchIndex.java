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


package org.amdocs.zusammen.plugin.searchindex.elasticsearch;


import io.netty.util.internal.StringUtil;
import org.amdocs.zusammen.datatypes.SessionContext;
import org.amdocs.zusammen.datatypes.Space;
import org.amdocs.zusammen.datatypes.searchindex.SearchCriteria;
import org.amdocs.zusammen.datatypes.searchindex.SearchResult;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.dao.ElasticSearchDao;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.dao.ElasticSearchDaoFactory;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.datatypes.EsEnrichmentData;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.datatypes.EsSearchCriteria;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.datatypes.EsSearchResult;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.amdocs.zusammen.sdk.types.searchindex.ElementSearchableData;
import org.amdocs.zusammen.utils.fileutils.FileUtils;
import org.amdocs.zusammen.utils.fileutils.json.JsonUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.index.IndexNotFoundException;

import java.io.ByteArrayInputStream;
import java.util.Objects;

public class SearchIndex {

  private ElasticSearchDao elasticSearchDao;

  public SearchResult search(SessionContext sessionContext, SearchCriteria searchCriteria) {
    checkSearchCriteriaInstance(searchCriteria);
    String index = getEsIndex(sessionContext);
    EsSearchResult searchResult = new EsSearchResult();

    searchResult.setSearchResponse(getElasticSearchDao(sessionContext)
        .search(sessionContext, index, (EsSearchCriteria) searchCriteria));
    return searchResult;
  }

  ElasticSearchDao getElasticSearchDao(SessionContext context) {
    if (Objects.isNull(elasticSearchDao)) {
      elasticSearchDao = ElasticSearchDaoFactory.getInstance().createInterface(context);
    }
    return elasticSearchDao;
  }

  EsSearchableData getEsSearchableData(ElementSearchableData elementSearchableData) {
    if (Objects.isNull(elementSearchableData.getSearchableData())) {
      EsSearchableData esSearchableData = new EsSearchableData();
      esSearchableData.setType(EsConstants.ELEMENT_DATA_DEFAULT_TYPE);
      return esSearchableData;
    }
    return JsonUtil.json2Object(elementSearchableData.getSearchableData(), EsSearchableData.class);
  }

  String getEsSource(EsSearchableData searchableData) {
    return new String(FileUtils.toByteArray(searchableData.getData()));
  }

  EsSearchableData createEnrichedElasticSearchableData(SessionContext sessionContext,
                                                       ElementSearchableData elementSearchableData) {
    EsSearchableData esSearchableData = getEsSearchableData(elementSearchableData);
    String searchableDataJson = null;
    if (Objects.nonNull(esSearchableData.getData())) {
      searchableDataJson = JsonUtil.inputStream2Json(esSearchableData.getData());
    }
    EsEnrichmentData enrichmentData = createEsEnrichmentData(sessionContext, elementSearchableData);
    String enrichmentDataJson = JsonUtil.object2Json(enrichmentData);

    String enrichedSearchableData =
        getEnrichedSearchableData(searchableDataJson, enrichmentDataJson);

    EsSearchableData enrichmentSearchable = new EsSearchableData();
    enrichmentSearchable.setData(new ByteArrayInputStream(enrichedSearchableData.getBytes()));
    enrichmentSearchable.setType(esSearchableData.getType());
    return enrichmentSearchable;
  }

  private String getEnrichedSearchableData(String searchableDataJson, String enrichmentDataJson) {
    if( Objects.nonNull(searchableDataJson)) {
      searchableDataJson = searchableDataJson.substring(0, searchableDataJson.length() - 1) + ",";
      enrichmentDataJson = enrichmentDataJson.substring(1);
      return searchableDataJson + enrichmentDataJson;
    }
    return enrichmentDataJson;
  }

  String createSearchableDataId(SessionContext sessionContext,
                                ElementSearchableData elementSearchableData) {
    String elasticSpace =
        getElasticSpaceForCreAndUpd(sessionContext, elementSearchableData.getSpace());
    StringBuffer searchableId = new StringBuffer();
    searchableId.append(elasticSpace).append("_");
    searchableId.append(elementSearchableData.getItemId()).append("_");
    searchableId.append(elementSearchableData.getVersionId()).append("_");
    searchableId.append(elementSearchableData.getElementId());
    return searchableId.toString();
  }

  EsEnrichmentData createEsEnrichmentData(SessionContext sessionContext,
                                          ElementSearchableData elementSearchableData) {
    EsEnrichmentData enrichmentData = new EsEnrichmentData();
    enrichmentData
        .setSpace(getElasticSpaceForCreAndUpd(sessionContext, elementSearchableData.getSpace()));
    enrichmentData.setItemId(elementSearchableData.getItemId());
    enrichmentData.setVersionId(elementSearchableData.getVersionId());
    enrichmentData.setElementId(elementSearchableData.getElementId());
    return enrichmentData;
  }

  String getElasticSpaceForCreAndUpd(SessionContext sessionContext, Space space) {
    if (space.equals(Space.PRIVATE)) {
      return sessionContext.getUser().getUserName().toLowerCase().replaceAll("\\ ", "");
    } else if (space.equals(Space.PUBLIC)) {
      return Space.PUBLIC.name().toLowerCase();
    } else {
      throw new RuntimeException(
          Space.BOTH.name() + " is invalid Space value for Elastic"
              + " create and update actions");
    }
  }

  String getEsIndex(SessionContext sessionContext) {
    String tenant = sessionContext.getTenant();
    if (StringUtil.isNullOrEmpty(tenant)) {
      throw new RuntimeException("Mandatory Data is missing - Tenant value in session context");
    }
    return tenant.replaceAll("\\ |\\\"|\\*|\\/|\\<|\\||\\|\\,|\\>|\\\\|\\?|\\,", "").toLowerCase();
  }


  void validation(ElementSearchableData elementSearchableData) {
    if (Objects.isNull(elementSearchableData)) {
      throw new RuntimeException(
          "Mandatory Data is missing - Element searchable data");
    }
    StringBuffer errorMsg = new StringBuffer();
    dataValidation(elementSearchableData, errorMsg);
    if (Objects.isNull(elementSearchableData.getSpace())) {
      errorMsg.append("Mandatory Data is missing - Space").append("\n");
    }
    if (Objects.isNull(elementSearchableData.getItemId())) {
      errorMsg.append("Mandatory Data is missing - Item Id").append("\n");
    }
    if (Objects.isNull(elementSearchableData.getVersionId())) {
      errorMsg.append("Mandatory Data is missing - Version Id").append("\n");
    }
    if (Objects.isNull(elementSearchableData.getElementId())) {
      errorMsg.append("Mandatory Data is missing - Element Id").append("\n");
    }

    if (errorMsg.length() > 0) {
      throw new RuntimeException(errorMsg.toString());
    }
  }

  void dataValidation(ElementSearchableData elementSearchableData, StringBuffer errorMsg) {
    if (Objects.nonNull(elementSearchableData.getSearchableData())) {
      try {
        getEsSearchableData(elementSearchableData);
      } catch (Exception exc) {
        errorMsg.append("Invalid searchableData, EsSearchableData object is expected").append("\n");
      }
    }
  }

  void checkIfSearchableDataExist(SessionContext sessionContext, String index,
                                  String type, String id)
      throws IndexNotFoundException {
    GetResponse getResponse =
        getElasticSearchDao(sessionContext).get(sessionContext, index, type, id);
    if (!getResponse.isExists()) {
      String notFoundErrorMsg = "Searchable data for tenant - '" + sessionContext.getTenant()
          + "', type - '" + type + "' and id - '" + id.toString() + "' was not found.";
      throw new RuntimeException(notFoundErrorMsg);
    }
  }

  void checkSearchCriteriaInstance(SearchCriteria searchCriteria) {
    if (!(searchCriteria instanceof EsSearchCriteria)) {
      throw new RuntimeException(
          "Invalid SearchCriteria, EsSearchCriteria object is expected");
    }
  }
}
