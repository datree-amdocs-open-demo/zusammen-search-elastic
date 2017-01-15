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
