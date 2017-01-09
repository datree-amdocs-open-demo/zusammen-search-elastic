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
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


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

  @Test
  public void testCreate() throws Exception {
    initSearchData();
    String spaceName = "public";
    String message = "create es data test";
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");
    tags.add("c");

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext = EsTestUtils.createSearchContext(SearchIndexSpace.PUBLIC);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, createName, message, tags);
    IndexResponse response = new ElasticSearchServices()
        .create(sessionContext, searchContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "created");

    searchableData =
        EsTestUtils.createSearchableData("type2", updateName, message, tags);
    response = new ElasticSearchServices()
        .create(sessionContext, searchContext, searchableData, new Id());
    Assert.assertEquals(response.getResult().getLowercase(), "created");
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Empty type in the searchableData object\n"
          + "Empty data in the searchableData object\n")
  public void testCreateFailed() throws Exception {
    String tenant = "tenant1";

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext = EsTestUtils.createSearchContext(SearchIndexSpace.PUBLIC);
    EsSearchableData searchableData = new EsSearchableData();
    new ElasticSearchServices().create(sessionContext, searchContext, searchableData, new Id());

  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "SearchableData object is null")
  public void testCreateWithNullSearchableData() throws Exception {
    String spaceName = "/public/abc";
    String tenant = "tenant1";

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext = EsTestUtils.createSearchContext(SearchIndexSpace.PUBLIC);

    new ElasticSearchServices().create(sessionContext, searchContext, null, new Id());
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "SearchableData object include invalid JSON data")
  public void testCreateWithInvalidData() throws Exception {
    initSearchData();
    String spaceName = "/public/abc";
    String tenant = "tenant1";

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext = EsTestUtils.createSearchContext(SearchIndexSpace.PUBLIC);

    new ElasticSearchServices()
        .create(sessionContext, searchContext, EsTestUtils.createInvalidSearchableData(), new Id());
  }


  @Test(dependsOnMethods = {"testCreate"})
  public void testUpdate() throws Exception {
    initSearchData();
    String spaceName = "updateUser";
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext = EsTestUtils.createSearchContext(SearchIndexSpace.PUBLIC);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, updateName, null, tags);
    UpdateResponse response = new ElasticSearchServices()
        .update(sessionContext, searchContext, searchableData, searchableId);
    Assert.assertEquals(response.getResult().getLowercase(), "updated");
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testGet() throws Exception {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData = EsTestUtils.createSearchableData(type, null, null, null);
    GetResponse response =
        new ElasticSearchServices().get(sessionContext, searchableData, searchableId);
    Assert.assertEquals(response.isExists(), true);
  }

  @Test
  public void testGetIdNotFound() throws Exception {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData = EsTestUtils.createSearchableData(type, null, null, null);
    GetResponse response =
        new ElasticSearchServices().get(sessionContext, searchableData, new Id());
    Assert.assertEquals(response.isExists(), false);
  }

  @Test(dependsOnMethods = {"testCreate"})
  public void testGetTypeNotFound() throws Exception {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData = EsTestUtils.createSearchableData("notFoundType", null,
        null, null);
    GetResponse response =
        new ElasticSearchServices().get(sessionContext, searchableData, searchableId);
    Assert.assertEquals(response.isExists(), false);
  }

  @Test(expectedExceptions = {IndexNotFoundException.class})
  public void testGetIndexNotFound() throws Exception {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext("notFoundIndex", user);
    EsSearchableData searchableData = EsTestUtils.createSearchableData(type, null, null, null);
    new ElasticSearchServices().get(sessionContext, searchableData, searchableId);
  }

  /*@Test(dependsOnMethods = {"testCreate", "testUpdate"})
  public void testSearchFullParameters() throws Exception {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext = EsTestUtils.createSearchContext(SearchIndexSpace.PUBLIC);

    String jsonQuery =
        EsTestUtils.wrapperTermQuery(EsTestUtils.getJson(updateName, null, null)
            .get().toLowerCase());

    List<String> types = new ArrayList<>();
    types.add(type);
    types.add("type2");
    EsSearchCriteria searchCriteria = EsTestUtils.createSearchCriteria(types, 0, 20, jsonQuery);
    SearchResponse response =
        new ElasticSearchServices().search(sessionContext, searchContext, searchCriteria);
    Assert.assertEquals(response.getHits().getTotalHits(), 2);
  }

  @Test(dependsOnMethods = {"testSearchFullParameters"})
  public void testSearchNoQueryParam() throws Exception {
    initSearchData();
    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    SearchIndexContext searchContext = EsTestUtils.createSearchContext(SearchIndexSpace.PUBLIC);

    String jsonQuery =
        EsTestUtils.wrapperTermQuery(EsTestUtils.getJson(updateName, null, null)
            .get().toLowerCase());

    List<String> types = new ArrayList<>();
    types.add(type);
    types.add("type2");
    EsSearchCriteria searchCriteria = EsTestUtils.createSearchCriteria(types, 0, 20, jsonQuery);
    SearchResponse response =
        new ElasticSearchServices().search(sessionContext, searchContext, searchCriteria);
    Assert.assertEquals(response.getHits().getTotalHits(), 2);
  }*/
}