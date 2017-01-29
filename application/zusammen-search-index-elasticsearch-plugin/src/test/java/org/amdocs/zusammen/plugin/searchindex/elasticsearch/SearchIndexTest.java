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
  SearchIndexServices searchIndexServices;

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
      searchIndexServices.search(sessionContext, searchCriteria);
    }
  }

  @Test
  public void testGetElasticSearchDao() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "myUser");
    ElasticSearchDao elasticSearchDao =
        new SearchIndexServices().getElasticSearchDao(sessionContext);
    Assert.assertNotNull(elasticSearchDao);
  }

  @Test
  public void testGetEsSearchableData() throws Exception {
    EsSearchableData searchableData = EsTestUtils.createSearchableData("myType", "myname", null,
        null);
    ElementSearchableData elementSearchableData =
        EsTestUtils.createElementSearchableData(searchableData,
            Space.PUBLIC, new Id(), new Id(), new Id());
    EsSearchableData data = new SearchIndexServices().getEsSearchableData(elementSearchableData);
    Assert.assertNotNull(data);
    Assert.assertNotNull(data.getType());
    Assert.assertNotNull(data.getData());
  }

  @Test
  public void testGetEsSearchableDataWithNoData() throws Exception {
    ElementSearchableData elementSearchableData =
        EsTestUtils.createElementSearchableData(null,
            Space.PUBLIC, new Id(), new Id(), new Id());
    EsSearchableData data = new SearchIndexServices().getEsSearchableData(elementSearchableData);
    Assert.assertNotNull(data);
    Assert.assertNotNull(data.getType());
    Assert.assertNull(data.getData());
  }

  @Test
  public void testGetEsSource() throws Exception {
    EsSearchableData searchableData = EsTestUtils.createSearchableData("nyType", "nmyname",
        "msg", null);
    String source = new SearchIndexServices().getEsSource(searchableData);
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

    EsSearchableData enrichedElasticSearchableData = new SearchIndexServices()
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
  public void testCreateOnlyEnrichedElasticSearchableData() throws Exception {
    String user = "myUser";
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", user);
    Id itemId = new Id();
    Id versionId = new Id();
    Id elementId = new Id();
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PRIVATE, itemId, versionId, elementId);

    EsSearchableData enrichedElasticSearchableData = new SearchIndexServices()
        .createEnrichedElasticSearchableData(sessionContext, elementSearchableData);
    Map map = JsonUtil.json2Object(enrichedElasticSearchableData.getData(), Map.class);
    Assert.assertEquals(map.size(), 4);
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
    String id = new SearchIndexServices().createSearchableDataId(sessionContext, elementSearchableData);
    Assert.assertNotNull(id);
    Assert.assertEquals(id,
        Space.PUBLIC.toString().toLowerCase() + "_" + itemId.toString() + "_" +
            versionId.toString() + "_" + elementId.toString());
  }

  @Test
  public void testGetElasticSpaceForCreAndUpdPrivate() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "My user");
    String elasticSpaceForCreAndUpd =
        new SearchIndexServices().getElasticSpaceForCreAndUpd(sessionContext, Space.PRIVATE);
    Assert.assertEquals(elasticSpaceForCreAndUpd, "myuser");
  }

  @Test
  public void testGetElasticSpaceForCreAndUpdPublic() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "My user");
    String elasticSpaceForCreAndUpd =
        new SearchIndexServices().getElasticSpaceForCreAndUpd(sessionContext, Space.PUBLIC);
    Assert.assertEquals(elasticSpaceForCreAndUpd, "public");
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "BOTH is invalid Space value for Elastic create and " +
          "update actions")
  public void testGetElasticSpaceForCreAndUpdBoth() throws Exception {
    SessionContext sessionContext = EsTestUtils.createSessionContext("tenant", "My user");
    new SearchIndexServices().getElasticSpaceForCreAndUpd(sessionContext, Space.BOTH);
  }

  @Test
  public void testGetIndex() {
    SessionContext sessionContext = EsTestUtils.createSessionContext(" one \" OO * * ee * \"\" "
            + "** sDs \\\\ ww \\ w < P<A tt << EE | s|sd ,w ,q >> ksjk> d / // ww /p ?? q? s    ",
        "myUser");

    String esIndex = new SearchIndexServices().getEsIndex(sessionContext);
    Assert.assertEquals(esIndex, "oneooeesdswwwpatteessdwqksjkdwwpqs");
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Tenant value in session context")
  public void testGetIndexWithNoTenant() {
    SessionContext sessionContext = EsTestUtils.createSessionContext(null, "myUser");
    new SearchIndexServices().getEsIndex(sessionContext);
  }

  @Test
  public void testValidationNoError() throws Exception {
    EsSearchableData searchableData = EsTestUtils.createSearchableData("myType", "nmyname",
        "msg", null);
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PUBLIC, new Id(), new Id(), new Id());
    new SearchIndexServices().validation(elementSearchableData);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Element searchable data")
  public void testValidationMissingElementSearchableData() throws Exception {
    new SearchIndexServices().validation(null);
  }

  @Test
  public void testValidationMissingSearchableData() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PUBLIC, new Id(), new Id(), new Id());
    new SearchIndexServices().validation(elementSearchableData);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Invalid searchableData, EsSearchableData object is expected.*")
  public void testValidationInvalidSearchableData() throws Exception {
    ElementSearchableData elementSearchableData = new ElementSearchableData();
    elementSearchableData.setSearchableData(new ByteArrayInputStream("kfkf".getBytes()));
    new SearchIndexServices().validation(elementSearchableData);
  }

  @Test
  public void testValidationSearchableDataNoRequired() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PUBLIC, new Id(), new Id(), new Id());
    new SearchIndexServices().validation(elementSearchableData);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Space.*")
  public void testValidationMissingSpace() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, null, new Id(), new Id(), new Id());
    new SearchIndexServices().validation(elementSearchableData);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = ".*Mandatory Data is missing - Item Id.*")
  public void testValidationMissingItemId() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, null, null, new Id(), new Id());
    new SearchIndexServices().validation(elementSearchableData);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Version Id.*")
  public void testValidationMissingVersionId() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PRIVATE, new Id(), null, new Id());
    new SearchIndexServices().validation(elementSearchableData);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Mandatory Data is missing - Element Id.*")
  public void testValidationMissingElementId() throws Exception {
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(null, Space.PRIVATE, new Id(), new Id(), null);
    new SearchIndexServices().validation(elementSearchableData);
  }

  @Test
  public void testCheckSearchCriteriaInstance() throws Exception {
    SearchCriteria searchCriteria = new EsSearchCriteria();
    new SearchIndexServices().checkSearchCriteriaInstance(searchCriteria);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Invalid SearchCriteria, EsSearchCriteria object is expected")
  public void testCheckSearchCriteriaInstanceInvalid() throws Exception {
    SearchCriteria searchCriteria = null;
    new SearchIndexServices().checkSearchCriteriaInstance(searchCriteria);
  }

}