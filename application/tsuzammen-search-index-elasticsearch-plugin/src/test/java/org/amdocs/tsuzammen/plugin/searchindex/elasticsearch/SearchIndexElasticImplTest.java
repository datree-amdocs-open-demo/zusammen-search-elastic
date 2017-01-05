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
import org.amdocs.tsuzammen.datatypes.UserInfo;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchContext;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.impl.SearchIndexElasticImpl;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;


public class SearchIndexElasticImplTest {
  @Test
  public void testCreate() throws Exception {
    String type = "type1";
    String user = "user1";
    String spaceName = "/public/abc";
    String tenant = "tenant1";
    String message = "message number 1";
    List<String> tags = new ArrayList<>();
    tags.add("a");
    tags.add("b");
    tags.add("c");

    SessionContext sessionContext = createSessionContext(tenant, user);
    SearchContext searchContext = createSearchContext(spaceName);
    EsSearchableData searchableData = createSearchableData(type, user, message, tags);
    SearchIndexElasticImpl elasticImpl = new SearchIndexElasticImpl();
    Id searchableId = new Id();
    elasticImpl.create(sessionContext, searchContext, searchableData, searchableId);

  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "Empty type in the searchableData object\n" +
          "Empty data in the searchableData object\n")
  public void testCreateFailed() throws Exception {

    String user = "user1";
    String spaceName = "/public/abc";
    String tenant = "tenant1";

    SessionContext sessionContext = createSessionContext(tenant, user);
    SearchContext searchContext = createSearchContext(spaceName);
    EsSearchableData searchableData = new EsSearchableData();

    SearchIndexElasticImpl elasticImpl = new SearchIndexElasticImpl();
    Id searchableId = new Id();
    elasticImpl.create(sessionContext, searchContext, searchableData, searchableId);

  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "SearchableData object is null")
  public void testCreateWithNullSearchableData() throws Exception {

    String user = "user1";
    String spaceName = "/public/abc";
    String tenant = "tenant1";

    SessionContext sessionContext = createSessionContext(tenant, user);
    SearchContext searchContext = createSearchContext(spaceName);

    SearchIndexElasticImpl elasticImpl = new SearchIndexElasticImpl();
    Id searchableId = new Id();
    elasticImpl.create(sessionContext, searchContext, null, searchableId);

  }

  @Test(expectedExceptions = {RuntimeException.class},
      expectedExceptionsMessageRegExp = "SearchableData object include invalid JSON data")
  public void testCreateWithInvalidData() throws Exception {

    String user = "user1";
    String spaceName = "/public/abc";
    String tenant = "tenant1";

    SessionContext sessionContext = createSessionContext(tenant, user);
    SearchContext searchContext = createSearchContext(spaceName);

    SearchIndexElasticImpl elasticImpl = new SearchIndexElasticImpl();
    Id searchableId = new Id();
    elasticImpl.create(sessionContext, searchContext, createInvalidSearchableData(), searchableId);

  }

  private EsSearchableData createInvalidSearchableData() {
    EsSearchableData searchableData = new EsSearchableData();

    String invalidJsonData = "{\n" +
        "    \"abc\":\"a\"\n" +
        "    \"dfs\":\"b\"\n" +
        "}";
    searchableData.setData(new ByteArrayInputStream(invalidJsonData.getBytes()));
    searchableData.setType("a");
    return searchableData;
  }

  private EsSearchableData createSearchableData(String type, String user, String message,
                                                List<String> tags) {
    EsSearchableData searchableData = new EsSearchableData();

    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < tags.size(); i++) {
      String tagVal = tags.get(i);
      if (i > 0) {
        stringBuilder.append(",");
      }
      stringBuilder.append("\"").append(tagVal).append("\"");
    }
    String jsonData = "{\n" +
        "  \"user\":\"" + user + "\",\n" +
        "  \"message\":\"" + message + "\",\n" +
        "  \"tags\": [" + stringBuilder.toString() + "]  \n" +
        "}";
    searchableData.setType(type);
    searchableData.setData(new ByteArrayInputStream(jsonData.getBytes()));
    return searchableData;
  }

  private SessionContext createSessionContext(String tenant, String user) {
    SessionContext sessionContext = new SessionContext();
    sessionContext.setTenant(tenant);
    sessionContext.setUser(new UserInfo(user));
    return sessionContext;
  }

  private SearchContext createSearchContext(String spaceName) {
    SearchContext searchContext = new SearchContext();
    searchContext.setItemId(new Id());
    searchContext.setVersionId(new Id());
    searchContext.setSpaceName(spaceName);
    return searchContext;
  }

  /*
  @Test
  public void testUpdate() throws Exception {

  }

  @Test
  public void testSearch() throws Exception {

  }

  @Test
  public void testDelete() throws Exception {

  }
*/
}