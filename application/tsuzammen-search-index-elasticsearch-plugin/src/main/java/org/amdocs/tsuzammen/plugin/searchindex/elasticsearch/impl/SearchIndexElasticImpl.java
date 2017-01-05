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

package org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.impl;


import io.netty.util.internal.StringUtil;
import org.amdocs.tsuzammen.datatypes.Id;
import org.amdocs.tsuzammen.datatypes.SessionContext;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchContext;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchCriteria;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchResult;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchableData;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.EsClientService;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.EsConfig;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.amdocs.tsuzammen.sdk.SearchIndex;
import org.amdocs.tsuzammen.utils.fileutils.json.JsonUtil;
import org.elasticsearch.client.transport.TransportClient;

import java.util.Objects;

public class SearchIndexElasticImpl implements SearchIndex {


  @Override
  public void create(SessionContext sessionContext, SearchContext searchContext,
                     SearchableData searchableData, Id id) {
    searchableDataValidation(searchableData);
    TransportClient transportClient = null;
    EsClientService esClientService = new EsClientService();
    EsConfig config = new EsConfig();
    try {
      transportClient = esClientService.start(sessionContext, config);
      String index = getEsIndex(sessionContext);
      String type = ((EsSearchableData) searchableData).getType();
      String source = getEsSource(searchContext, (EsSearchableData) searchableData);
      transportClient.prepareIndex(index, type).setSource(source).get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (Objects.nonNull(transportClient)) {
        esClientService.stop(sessionContext, transportClient);
      }
    }


  }

  private void searchableDataValidation(SearchableData searchableData) {
    StringBuffer errorMsg = new StringBuffer();

    if (Objects.isNull(searchableData)) {
      errorMsg.append("SearchableData object is null");
      throw new RuntimeException(errorMsg.toString());
    }
    if (!(searchableData instanceof EsSearchableData)) {
      errorMsg.append("Invalid instance of SearchableData, EsSearchableData object is expected");
      throw new RuntimeException(errorMsg.toString());
    }
    if (StringUtil.isNullOrEmpty(((EsSearchableData) searchableData).getType())) {
      errorMsg.append("Empty type in the searchableData object").append("\n");
    }
    if (Objects.isNull(((EsSearchableData) searchableData).getData())) {
      errorMsg.append("Empty data in the searchableData object").append("\n");
    } else {
      try {
        JsonUtil.inputStream2Json(((EsSearchableData) searchableData).getData());
        ((EsSearchableData) searchableData).getData().reset();
      } catch (Exception e) {
        errorMsg.append("SearchableData object include invalid JSON data");
      }
    }

    if (errorMsg.length() > 0) {
      throw new RuntimeException(errorMsg.toString());
    }
  }

  private String getEsSource(SearchContext searchContext, EsSearchableData searchableData) {
    if (Objects.nonNull(searchableData.getData())) {
      String searchContextJson = JsonUtil.object2Json(searchContext);
      String searchableDataJson = JsonUtil.inputStream2Json(searchableData.getData());
      searchableDataJson = searchableDataJson.substring(0, searchableDataJson.length() - 1) + ",";
      searchContextJson = searchContextJson.substring(1);
      return searchableDataJson + searchContextJson;
    }
    return "";
  }

  private String getEsIndex(SessionContext sessionContext) {
    return sessionContext.getTenant();
  }

  @Override
  public void update(SessionContext sessionContext, SearchContext searchContext,
                     SearchableData searchableData, Id id) {

  }

  @Override
  public SearchResult search(SessionContext sessionContext, SearchContext searchContext,
                             SearchCriteria searchCriteria) {
    return null;
  }

  @Override
  public void delete(SessionContext sessionContext, SearchContext searchContext,
                     SearchCriteria searchCriteria, Id id) {

  }
}
