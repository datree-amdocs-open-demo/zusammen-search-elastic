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
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.dao.ElasticSearchDao;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.amdocs.zusammen.sdk.types.searchindex.ElementSearchableData;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.IndexNotFoundException;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;

public class ElementSearchIndexTest {

  @Mock
  ElasticSearchDao elasticSearchDaoMock;
  @Mock
  GetResponse getResponseMock;
  @Mock
  DeleteResponse deleteResponseMock;
  @InjectMocks
  ElementSearchIndex elementSearchIndex;

  @BeforeMethod(alwaysRun = true)
  public void injectDoubles() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(elasticSearchDaoMock
        .create(anyObject(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(new IndexResponse());
    Mockito.when(elasticSearchDaoMock.get(anyObject(), anyString(), anyString(), anyString()))
        .thenReturn(getResponseMock);
    Mockito.when(elasticSearchDaoMock.delete(anyObject(), anyString(), anyString(), anyString()))
        .thenReturn(deleteResponseMock);
  }

  private Id itemId = null;
  private Id versionId = null;
  private Id elementId = null;
  private String type;
  private String tenant;
  private String user;
  private String createName;
  private String updateName;


  private void initSearchData() {
    tenant = "elementsearchindextest";
    type = "type1";
    user = "user";
    createName = "createName";
    updateName = "updateName";
    if (Objects.isNull(itemId)) {
      itemId = new Id();
      versionId = new Id();
      elementId = new Id();
    }

  }

  @Test
  public void testCreateElement() throws Exception {
    initSearchData();

    String message = "create es data test";
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");
    tags.add("c");

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, createName, message, tags);
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PRIVATE, itemId, versionId, elementId);
    elementSearchIndex.createElement(sessionContext, elementSearchableData);
  }

  @Test
  public void testUpdateElement() throws Exception {
    initSearchData();
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, updateName, null, tags);
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PRIVATE, itemId, versionId, elementId);
    Mockito.when(getResponseMock.isExists()).thenReturn(true);
    elementSearchIndex.updateElement(sessionContext, elementSearchableData);

  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Searchable data for tenant - 'elementsearchindextest' "
          + "was not found.")
  public void testUpdateIndexNotFound() throws Exception {
    initSearchData();
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, updateName, null, tags);
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PRIVATE, itemId, versionId, elementId);

    Mockito.when(elasticSearchDaoMock.get(anyObject(), anyString(), anyString(), anyString()))
        .thenThrow(new IndexNotFoundException("indexnotfound"));
    elementSearchIndex.updateElement(sessionContext, elementSearchableData);

  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = ".*Searchable data for tenant - " +
          "'elementsearchindextest', type - 'type1' and id - .* was not found.")
  public void testUpdateIdNotExist() throws Exception {
    initSearchData();
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, updateName, null, tags);
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PRIVATE, new Id(), versionId, elementId);
    Mockito.when(getResponseMock.isExists()).thenReturn(false);
    elementSearchIndex.updateElement(sessionContext, elementSearchableData);
  }

  @Test
  public void testDeleteElement() throws Exception {
    initSearchData();

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, null, null, null);
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PRIVATE, itemId, versionId, elementId);

    Mockito.when(deleteResponseMock.getResult()).thenReturn(DocWriteResponse.Result.UPDATED);
    elementSearchIndex.deleteElement(sessionContext, elementSearchableData);
  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Delete failed - Searchable data for tenant - "
          + "'elementsearchindextest', type - 'type1' and id - .* was not found.")
  public void testDeleteNotFound() throws Exception {
    initSearchData();

    SessionContext sessionContext = EsTestUtils.createSessionContext(tenant, user);
    EsSearchableData searchableData =
        EsTestUtils.createSearchableData(type, null, null, null);
    ElementSearchableData elementSearchableData = EsTestUtils
        .createElementSearchableData(searchableData, Space.PRIVATE, new Id(), versionId, elementId);

    Mockito.when(deleteResponseMock.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);
    elementSearchIndex.deleteElement(sessionContext, elementSearchableData);
  }
}


