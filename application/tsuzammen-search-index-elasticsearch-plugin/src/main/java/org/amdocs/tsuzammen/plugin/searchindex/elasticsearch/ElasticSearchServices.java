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

package org.amdocs.tsuzammen.plugin.searchindex.elasticsearch;


import io.netty.util.internal.StringUtil;
import org.amdocs.tsuzammen.datatypes.Id;
import org.amdocs.tsuzammen.datatypes.SessionContext;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchIndexContext;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchIndexSpace;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchContext;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchCriteria;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.amdocs.tsuzammen.utils.fileutils.json.JsonUtil;
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

public class ElasticSearchServices {

  public IndexResponse create(SessionContext sessionContext, SearchIndexContext searchIndexContext,
                              EsSearchableData searchableData, Id id) {
    inputValidation(searchableData, id, true);
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      String index = getEsIndex(sessionContext);
      String type = searchableData.getType();
      String source = getEsSource(sessionContext, searchIndexContext, searchableData);
      return transportClient.prepareIndex(index, type, id.toString()).setSource(source).get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (Objects.nonNull(transportClient)) {
        clientServices.stop(sessionContext, transportClient);
      }
    }
  }

  public GetResponse get(SessionContext sessionContext, EsSearchableData searchableData, Id id)
      throws IndexNotFoundException {
    inputValidation(searchableData, id, false);
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      String index = getEsIndex(sessionContext);
      String type = searchableData.getType();
      return transportClient.prepareGet(index, type, id.toString()).get();

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

  /*
  Update by merging documents
   */
  public UpdateResponse update(SessionContext sessionContext, SearchIndexContext searchIndexContext,
                               EsSearchableData searchableData, Id id) {
    inputValidation(searchableData, id, true);
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      String index = getEsIndex(sessionContext);
      String type = searchableData.getType();
      String source = getEsSource(sessionContext, searchIndexContext, searchableData);
      return transportClient.prepareUpdate(index, type, id.toString()).setDoc(source).get();


    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (Objects.nonNull(transportClient)) {
        clientServices.stop(sessionContext, transportClient);
      }
    }
  }

  public SearchResponse search(SessionContext sessionContext, SearchIndexContext searchIndexContext,
                               EsSearchCriteria searchCriteria) {
    TransportClient transportClient = null;
    EsClientServices clientServices = new EsClientServices();
    EsConfig config = new EsConfig();
    try {
      transportClient = clientServices.start(sessionContext, config);
      String index = getEsIndex(sessionContext);
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


  private void inputValidation(EsSearchableData searchableData, Id id, boolean isDataRequired) {
    StringBuffer errorMsg = new StringBuffer();

    if (Objects.isNull(id)) {
      errorMsg.append("Id object is null").append("\n");
    }
    if (Objects.isNull(searchableData)) {
      errorMsg.append("SearchableData object is null");
      throw new RuntimeException(errorMsg.toString());
    }
    if (StringUtil.isNullOrEmpty(searchableData.getType())) {
      errorMsg.append("Empty type in the searchableData object").append("\n");
    }

    if (isDataRequired) {
      dataValidation(searchableData, errorMsg);
    }

    if (errorMsg.length() > 0) {
      throw new RuntimeException(errorMsg.toString());
    }
  }

  private void dataValidation(EsSearchableData searchableData, StringBuffer errorMsg) {
    if (Objects.isNull(searchableData.getData())) {
      errorMsg.append("Empty data in the searchableData object").append("\n");
    } else {
      try {
        JsonUtil.inputStream2Json(searchableData.getData());
        searchableData.getData().reset();
      } catch (Exception e) {
        errorMsg.append("SearchableData object include invalid JSON data");
      }
    }
  }

  private String getEsSource(SessionContext sessionContext,
                                       SearchIndexContext searchIndexContext,
                                       EsSearchableData searchableData) {
    if (Objects.nonNull(searchableData) && Objects.nonNull(searchableData.getData())) {
      EsSearchContext elasticSearchContext =
          getElasticSearchContext(sessionContext, searchIndexContext);
      String elasticSearchContextJson = JsonUtil.object2Json(elasticSearchContext);
      String searchableDataJson = JsonUtil.inputStream2Json(searchableData.getData());
      searchableDataJson = searchableDataJson.substring(0, searchableDataJson.length() - 1) + ",";
      elasticSearchContextJson = elasticSearchContextJson.substring(1);
      return searchableDataJson + elasticSearchContextJson;
    }
    return "";
  }

  private EsSearchContext getElasticSearchContext(SessionContext sessionContext,
                                                  SearchIndexContext searchIndexContext) {
    EsSearchContext elasticSearchContext = new EsSearchContext();
    elasticSearchContext.setItemId(searchIndexContext.getItemId());
    elasticSearchContext.setVersionId(searchIndexContext.getVersionId());
    elasticSearchContext.setSpace(getElasticSpace(sessionContext, searchIndexContext));
    return elasticSearchContext;
  }

  private String getElasticSpace(SessionContext sessionContext, SearchIndexContext searchContext) {
    if (Objects.nonNull(searchContext.getSpace())) {
      if (searchContext.getSpace().equals(SearchIndexSpace.PRIVATE)) {
        return sessionContext.getUser().getUserName();
      } else if (searchContext.getSpace().equals(SearchIndexSpace.PUBLIC)) {
        return SearchIndexSpace.PUBLIC.name().toLowerCase();
      } else {
        throw new RuntimeException(
            SearchIndexSpace.BOTH.name() + "is invalid Search Space value for "
                + "create and update actions");
      }
    }
    throw new RuntimeException("Missing search space value in search context");
  }

  private String getEsIndex(SessionContext sessionContext) {
    String tenant = sessionContext.getTenant();
    if (StringUtil.isNullOrEmpty(tenant)) {
      throw new RuntimeException("Missing tenant value in session context, tenant is mandatory");
    }
    return tenant;
  }


}
