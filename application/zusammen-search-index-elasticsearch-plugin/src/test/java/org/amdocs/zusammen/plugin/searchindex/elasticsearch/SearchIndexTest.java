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

import org.amdocs.zusammen.datatypes.Id;
import org.amdocs.zusammen.datatypes.SessionContext;
import org.amdocs.zusammen.datatypes.Space;
import org.amdocs.zusammen.datatypes.searchindex.SearchCriteria;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.dao.ElasticSearchDao;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.datatypes.EsSearchCriteria;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.amdocs.zusammen.sdk.types.searchindex.ElementSearchableData;
import org.amdocs.zusammen.utils.fileutils.json.JsonUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;


public class SearchIndexTest {

  @Mock
  ElasticSearchDao elasticSearchDaoMock;
  @InjectMocks
  SearchIndex searchIndex;

  @BeforeMethod(alwaysRun = true)
  public void injectDoubles() {
    MockitoAnnotations.initMocks(this);
    Mockito.when(elasticSearchDaoMock.search(anyObject(), anyString(), anyObject()))
        .thenReturn(new SearchResponse());

  }

  @Test
  public void testSearch() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "user");
    Optional<String> json = EsTestUtils.getJson("myname", null, null);
    Assert.assertEquals(json.isPresent(), true);
    if (json.isPresent()) {
      String jsonQuery = EsTestUtils.wrapperTermQuery(json.get().toLowerCase());

      List<String> types = new ArrayList<>();
      types.add("type1");
      EsSearchCriteria searchCriteria = EsTestUtils.createSearchCriteria(types, 0, 1, jsonQuery);
      searchIndex.search(sessionContext, searchCriteria);
    }
  }

  @Test
  public void testGetElasticSearchDao() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "myUser");
    ElasticSearchDao elasticSearchDao =
        new SearchIndex().getElasticSearchDao(sessionContext);
    Assert.assertNotNull(elasticSearchDao);
  }

  @Test
  public void testGetEsSearchableData() throws Exception {
    EsSearchableData searchableData = EsTestUtils.createSearchableData("myType", null, null, null);
    ElementSearchableData elementSearchableData =
        EsTestUtils.createElementSearchableData(searchableData,
            Space.PUBLIC, new Id(), new Id(), new Id());
    EsSearchableData data = new SearchIndex().getEsSearchableData(elementSearchableData);
    Assert.assertNotNull(data);
  }

  @Test
  public void testGetEsSource() throws Exception {
    EsSearchableData searchableData = EsTestUtils.createSearchableData("nyType", "nmyname",
        "msg", null);
    String source = new SearchIndex().getEsSource(searchableData);
    Assert.assertNotNull(source);
  }

  @Test
  public void testCreateEnrichedElasticSearchableData() throws Exception {
    String user = "myUser";
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", user);
    String type = "type";
    String name = "createName";
    String message = "message";
    EsSearchableData searchableData = EsTestUtils.createSearchableData(type, name, message, null);
    Id itemId = new Id();
    Id versionId = new Id();
    Id elementId = new Id();
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PRIVATE, itemId, versionId, elementId);

    EsSearchableData enrichedElasticSearchableData = new SearchIndex()
        .createEnrichedElasticSearchableData(sessionContext, elementSearchableData);
    Map map = JsonUtil.json2Object(enrichedElasticSearchableData.getData(), Map.class);
    Assert.assertEquals(map.size(), 6);
    Assert.assertEquals(map.get("name"), name);
    Assert.assertEquals(map.get("message"), message);
    Assert.assertEquals(((Map) map.get("itemId")).get("value").toString(), itemId.toString());
    Assert
        .assertEquals(((Map) map.get("elementId")).get("value").toString(), elementId.toString());
    Assert
        .assertEquals(((Map) map.get("versionId")).get("value").toString(), versionId.toString());
    Assert.assertEquals(map.get("space"), user.toLowerCase());
  }

  @Test
  public void testCreateSearchableDataId() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "myUser");
    EsSearchableData searchableData = EsTestUtils.createSearchableData("myType", "nmyname",
        "msg", null);
    Id itemId = new Id();
    Id versionId = new Id();
    Id elementId = new Id();
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PUBLIC, itemId, versionId, elementId);
    String id = new SearchIndex().createSearchableDataId(sessionContext, elementSearchableData);
    Assert.assertNotNull(id);
    Assert.assertEquals(id,
        Space.PUBLIC.toString().toLowerCase() + "_" + itemId.toString() + "_" +
            versionId.toString() + "_" + elementId.toString());
  }

  @Test
  public void testGetElasticSpaceForCreAndUpdPrivate() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "My user");
    String elasticSpaceForCreAndUpd =
        new SearchIndex().getElasticSpaceForCreAndUpd(sessionContext, Space.PRIVATE);
    Assert.assertEquals(elasticSpaceForCreAndUpd, "myuser");
  }

  @Test
  public void testGetElasticSpaceForCreAndUpdPublic() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "My user");
    String elasticSpaceForCreAndUpd =
        new SearchIndex().getElasticSpaceForCreAndUpd(sessionContext, Space.PUBLIC);
    Assert.assertEquals(elasticSpaceForCreAndUpd, "public");
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "BOTH is invalid Space value for Elastic create and " +
          "update actions")
  public void testGetElasticSpaceForCreAndUpdBoth() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "My user");
    new SearchIndex().getElasticSpaceForCreAndUpd(sessionContext, Space.BOTH);
  }

  @Test
  public void testGetIndex() {
    SessionContext sessionContext = EsTestUtils.createSessionContext(" one \" OO * * ee * \"\" "
            + "** sDs \\\\ ww \\ w < P<A tt << EE | s|sd ,w ,q >> ksjk> d / // ww /p ?? q? s    ",
        "myUser");

    String esIndex = new SearchIndex().getEsIndex(sessionContext);
    Assert.assertEquals(esIndex, "oneooeesdswwwpatteessdwqksjkdwwpqs");
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Tenant value in session context")
  public void testGetIndexWithNoTenant() {
    SessionContext sessionContext = EsTestUtils.createSessionContext(null, "myUser");
    new SearchIndex().getEsIndex(sessionContext);
  }

  @Test
  public void testValidationNoError() throws Exception {
    EsSearchableData searchableData = EsTestUtils.createSearchableData("myType", "nmyname",
        "msg", null);
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PUBLIC, new Id(), new Id(), new Id());
    new SearchIndex().validation(elementSearchableData, true);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Element searchable data")
  public void testValidationMissingElementSearchableData() throws Exception {
    new SearchIndex().validation(null, true);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Searchable data.*")
  public void testValidationMissingSearchableData() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PUBLIC, new Id(), new Id(), new Id());
    new SearchIndex().validation(elementSearchableData, true);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Invalid searchableData, EsSearchableData object is expected.*")
  public void testValidationInvalidSearchableData() throws Exception {
    ElementSearchableData elementSearchableData = new ElementSearchableData();
    elementSearchableData.setSearchableData(new ByteArrayInputStream("kfkf".getBytes()));
    new SearchIndex().validation(elementSearchableData, true);
  }

  @Test
  public void testValidationSearchableDataNoRequired() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PUBLIC, new Id(), new Id(), new Id());
    new SearchIndex().validation(elementSearchableData, false);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Space.*")
  public void testValidationMissingSpace() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, null, new Id(), new Id(), new Id());
    new SearchIndex().validation(elementSearchableData, false);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = ".*Mandatory Data is missing - Item Id.*")
  public void testValidationMissingItemId() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, null, null, new Id(), new Id());
    new SearchIndex().validation(elementSearchableData, false);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Version Id.*")
  public void testValidationMissingVersionId() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PRIVATE, new Id(), null, new Id());
    new SearchIndex().validation(elementSearchableData, false);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Element Id.*")
  public void testValidationMissingElementId() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PRIVATE, new Id(), new Id(), null);
    new SearchIndex().validation(elementSearchableData, false);
  }

  @Test
  public void testCheckSearchCriteriaInstance() throws Exception {
    SearchCriteria searchCriteria = new EsSearchCriteria();
    new SearchIndex().checkSearchCriteriaInstance(searchCriteria);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Invalid SearchCriteria, EsSearchCriteria object is expected")
  public void testCheckSearchCriteriaInstanceInvalid() throws Exception {
    SearchCriteria searchCriteria = null;
    new SearchIndex().checkSearchCriteriaInstance(searchCriteria);
  }

}