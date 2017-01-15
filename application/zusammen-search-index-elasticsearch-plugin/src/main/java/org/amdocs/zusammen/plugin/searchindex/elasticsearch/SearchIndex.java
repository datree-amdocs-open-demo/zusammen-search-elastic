/*
 * 				Copyright takedown notice
 *
 * If you believe your copyright protected work was posted on Amdocs account in Github without authorization,
 * you may submit a copyright infringement notification. Before doing so, please consider whether fair use,
 * fair dealing, or a similar exception to copyright applies. These requests should only be submitted by the
 * copyright owner or an agent authorized to act on the owner’s behalf.
 *
 * Please bear in mind that requesting the removal of content by submitting an infringement notification means
 * initiating a legal process.
 *
 * Do not make false claims. Misuse of this process may result legal consequences.
 *
 * You can submit an alleged copyright infringement by sending an email to amdocsfossfp@amdocs.com and specifying
 * the following information (copyright takedown notifications must include the following elements.
 * Without this information, we will be unable to take action on your request):
 *
 * 1. Your contact information
 * 	You’ll need to provide information that will allow us to contact you regarding your complaint, such as an email address, physical address or telephone number.
 *
 * 2. A description of your work that you believe has been infringed
 * 	In your complaint, please describe the copyrighted content you want to protect.
 *
 * 3. You must agree to and include the following statement:
 * 	“I believe that the use of the material is not authorized by the copyright owner, its agent, or the law.”
 *
 * 4. And the following statement:
 * 	"The information in this notification is accurate, and I am the owner, or an agent authorized to act on behalf of the owner”
 *
 * 5. Your signature
 * 	Please make sure to sign at the bottom of your complaint.
 *
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
