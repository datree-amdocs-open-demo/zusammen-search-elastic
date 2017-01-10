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

import org.amdocs.tsuzammen.datatypes.Id;
import org.amdocs.tsuzammen.datatypes.SessionContext;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchIndexContext;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchIndexSpace;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchCriteria;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class ElasticSearchServicesTest {

  private Id searchableId = null;
  private String type;
  private String tenant;
  private String user;
  private String createName;
  private String updateName;

  private void initSearchData() {
    tenant = "elasticsearchservicestest";
    type = "type1";
    user = "user";
    createName = "createName";
    updateName = "updateName";
    if (Objects.isNull(searchableId)) {
      searchableId = new Id();
    }
  }

  @Test(groups = "create")
  public void testCreate() {
    initSearchData();
    String message = "create es data test";
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");
    tags.add("c");

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PRIVATE);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, createName, message, tags);
    IndexResponse response = new ElasticSearchServices()
        .create(sessionContext, searchContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "created");

    searchableData = EsTestUtils.createSearchableData("type2", updateName, message, tags);
    response = new ElasticSearchServices()
        .create(sessionContext, searchContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "created");
  }

  @Test
  public void testCreateWithFixInvalidIndex() {
    initSearchData();

    SessionContext sessionContext = EsTestUtils.createSessionContext(" one \" OO * * ee * \"\" " +
            "** sDs \\\\ ww \\ w < P<A tt << EE | s|sd ,w ,q >> ksjk> d / // ww /p ?? q? s    ",
        user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PRIVATE);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData("type2", updateName, null, null);
    IndexResponse response = new ElasticSearchServices()
        .create(sessionContext, searchContext, searchableData, new Id());
    Assert.assertEquals(response.getResult().getLowercase(), "created");
    Assert.assertEquals(response.getIndex(), "oneooeesdswwwpatteessdwqksjkdwwpqs");
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Empty type in the searchableData object\n"
          + "Empty data in the searchableData object\n")
  public void testCreateWithNoSearchableDataAndType() {
    String tenant = "tenant1";

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);
    EsSearchableData searchableData = new EsSearchableData();
    new ElasticSearchServices().create(sessionContext, searchContext, searchableData, new Id());

  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "SearchableData object is null")
  public void testCreateWithNullSearchableData() {
    String tenant = "tenant1";

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);

    new ElasticSearchServices().create(sessionContext, searchContext, null, new Id());
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Searchable Data Id object is null.*")
  public void testCreateWithNullId() {
    String tenant = "tenant1";

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);

    new ElasticSearchServices().create(sessionContext, searchContext, null, null);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "SearchableData object include invalid JSON data")
  public void testCreateWithInvalidData() {
    initSearchData();
    String tenant = "tenant1";

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);

    new ElasticSearchServices()
        .create(sessionContext, searchContext, EsTestUtils.createInvalidSearchableData(), new Id());
  }


  @Test(groups = "create", dependsOnMethods = {"testCreate"})
  public void testUpdate() {
    initSearchData();
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, updateName, null, tags);
    UpdateResponse response = new ElasticSearchServices()
        .update(sessionContext, searchContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "updated");
  }

  @Test(groups = "create", dependsOnMethods = {"testCreate"})
  public void testGet() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData = EsTestUtils.createSearchableData(type, null, null, null);
    GetResponse response =
        new ElasticSearchServices().get(sessionContext, searchableData, searchableId);
    Assert.assertEquals(response.isExists(), true);
  }

  @Test
  public void testGetIdNotFound() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData = EsTestUtils.createSearchableData(type, null, null, null);
    GetResponse response =
        new ElasticSearchServices().get(sessionContext, searchableData, new Id());
    Assert.assertEquals(response.isExists(), false);
  }

  @Test(groups = "create", dependsOnMethods = {"testCreate"})
  public void testGetTypeNotFound() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData = EsTestUtils.createSearchableData("notFoundType", null,
        null, null);
    GetResponse response =
        new ElasticSearchServices().get(sessionContext, searchableData, searchableId);
    Assert.assertEquals(response.isExists(), false);
  }

  @Test(expectedExceptions = {IndexNotFoundException.class})
  public void testGetIndexNotFound() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext("notFoundIndex", user);
    EsSearchableData searchableData = EsTestUtils.createSearchableData(type, null, null, null);
    new ElasticSearchServices().get(sessionContext, searchableData, searchableId);
  }

  @Test(groups = "search", dependsOnGroups = {"create"})
  public void testSearchFullParameters() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);

    Optional<String> json = EsTestUtils.getJson(updateName, null, null);
    Assert.assertEquals(json.isPresent(), true);
    if (json.isPresent()) {
      String jsonQuery = EsTestUtils.wrapperTermQuery(json.get().toLowerCase());
      List<String> types = new ArrayList<>();
      types.add(type);
      types.add("type2");
      EsSearchCriteria searchCriteria = EsTestUtils.createSearchCriteria(types, 0, 1, jsonQuery);
      SearchResponse response =
          new ElasticSearchServices().search(sessionContext, searchContext, searchCriteria);
      Assert.assertEquals(response.getHits().getTotalHits(), 2);
      Assert.assertEquals(response.getHits().getHits().length, 1);
    }
  }

  @Test(groups = "search", dependsOnGroups = {"create"})
  public void testSearchNoQueryParam() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);

    List<String> types = new ArrayList<>();
    types.add(type);
    EsSearchCriteria searchCriteria = EsTestUtils.createSearchCriteria(types, 0, 20, null);
    SearchResponse response =
        new ElasticSearchServices().search(sessionContext, searchContext, searchCriteria);
    Assert.assertEquals(response.getHits().getTotalHits(), 1);
  }

  @Test(groups = "search", dependsOnGroups = {"create"})
  public void testSearchNoFromPageParam() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);

    Optional<String> json = EsTestUtils.getJson(updateName, null, null);
    Assert.assertEquals(json.isPresent(), true);
    if (json.isPresent()) {
      String jsonQuery = EsTestUtils.wrapperTermQuery(json.get().toLowerCase());

      List<String> types = new ArrayList<>();
      types.add(type);
      EsSearchCriteria searchCriteria =
          EsTestUtils.createSearchCriteria(types, null, 20, jsonQuery);
      SearchResponse response =
          new ElasticSearchServices().search(sessionContext, searchContext, searchCriteria);
      Assert.assertEquals(response.getHits().getTotalHits(), 1);
    }
  }

  @Test(groups = "search", dependsOnGroups = {"create"})
  public void testSearchNoPageSizeParam() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);
    Optional<String> json = EsTestUtils.getJson(updateName, null, null);
    Assert.assertEquals(json.isPresent(), true);
    if (json.isPresent()) {
      String jsonQuery = EsTestUtils.wrapperTermQuery(json.get().toLowerCase());

      List<String> types = new ArrayList<>();
      types.add(type);
      EsSearchCriteria searchCriteria = EsTestUtils.createSearchCriteria(types, 0, null, jsonQuery);
      SearchResponse response =
          new ElasticSearchServices().search(sessionContext, searchContext, searchCriteria);
      Assert.assertEquals(response.getHits().getTotalHits(), 1);
    }
  }

  @Test(groups = "search", dependsOnGroups = {"create"})
  public void testSearchNoTypeParam() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);
    Optional<String> json = EsTestUtils.getJson(updateName, null, null);
    Assert.assertEquals(json.isPresent(), true);
    if (json.isPresent()) {
      String jsonQuery = EsTestUtils.wrapperTermQuery(json.get().toLowerCase());

      EsSearchCriteria searchCriteria = EsTestUtils.createSearchCriteria(null, 0, 20, jsonQuery);
      SearchResponse response =
          new ElasticSearchServices().search(sessionContext, searchContext, searchCriteria);
      Assert.assertEquals(response.getHits().getTotalHits(), 2);
    }
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = ".*Missing tenant value in session context, tenant is " +
          "mandatory")
  public void testMissingTenant() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(null, user);
    SearchIndexContext searchContext =
        EsTestUtils.createSearchIndexContext(SearchIndexSpace.PUBLIC);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, createName, null, null);
    new ElasticSearchServices().create(sessionContext, searchContext, searchableData, new Id());
  }

  @Test(dependsOnGroups = {"create", "search"})
  public void testDelete() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, null, null, null);
    DeleteResponse response =
        new ElasticSearchServices().delete(sessionContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "deleted");
    searchableData.setType("type2");
    response =
        new ElasticSearchServices().delete(sessionContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "deleted");
  }

  @Test
  public void testDeleteIdNotFound() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, null, null, null);
    DeleteResponse response =
        new ElasticSearchServices().delete(sessionContext, searchableData, new Id());
    Assert.assertEquals(response.getResult().getLowercase(), "not_found");
  }

  @Test
  public void testDeleteTypeNotFound() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData("notFoundType", null, null, null);
    DeleteResponse response =
        new ElasticSearchServices().delete(sessionContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "not_found");
  }

  @Test
  public void testDeleteIndexNotFound() {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext("notfoundtenant", user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, null, null, null);
    DeleteResponse response =
        new ElasticSearchServices().delete(sessionContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "not_found");
  }

}