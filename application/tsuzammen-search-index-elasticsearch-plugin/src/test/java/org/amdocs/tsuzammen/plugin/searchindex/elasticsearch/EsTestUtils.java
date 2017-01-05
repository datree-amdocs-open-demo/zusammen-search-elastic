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
import org.amdocs.tsuzammen.datatypes.UserInfo;
import org.amdocs.tsuzammen.datatypes.searchindex.SearchContext;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Objects;

public class EsTestUtils {

  public static EsSearchableData createInvalidSearchableData() {
    EsSearchableData searchableData = new EsSearchableData();

    String invalidJsonData = "{\n" +
        "    \"abc\":\"a\"\n" +
        "    \"dfs\":\"b\"\n" +
        "}";
    searchableData.setData(new ByteArrayInputStream(invalidJsonData.getBytes()));
    searchableData.setType("a");
    return searchableData;
  }

  public static EsSearchableData createSearchableData(String type, String user, String message,
                                                      List<String> tags) {
    EsSearchableData searchableData = new EsSearchableData();

    StringBuilder stringBuilder = new StringBuilder();
    if (Objects.nonNull(tags)) {
      for (int i = 0; i < tags.size(); i++) {
        String tagVal = tags.get(i);
        if (i > 0) {
          stringBuilder.append(",");
        }
        stringBuilder.append("\"").append(tagVal).append("\"");
      }
    }
    StringBuilder jsonData = new StringBuilder();
    jsonData.append("{\n");
    if (!StringUtil.isNullOrEmpty(user)) {
      jsonData.append("  \"user\":\"").append(user).append("\",\n");
    }
    if (!StringUtil.isNullOrEmpty(message)) {
      jsonData.append("  \"message\":\"").append(message).append("\",\n");
    }
    if (Objects.nonNull(tags) && tags.size() > 0) {
      jsonData.append("  \"tags\": [").append(stringBuilder.toString());
      jsonData.append("]  \n");
    }
    jsonData.append("}");

    searchableData.setType(type);
    searchableData.setData(new ByteArrayInputStream(jsonData.toString().getBytes()));
    return searchableData;
  }

  public static SessionContext createSessionContext(String tenant, String user) {
    SessionContext sessionContext = new SessionContext();
    sessionContext.setTenant(tenant);
    sessionContext.setUser(new UserInfo(user));
    return sessionContext;
  }

  public static SearchContext createSearchContext(String spaceName) {
    SearchContext searchContext = new SearchContext();
    searchContext.setItemId(new Id());
    searchContext.setVersionId(new Id());
    searchContext.setSpaceName(spaceName);
    return searchContext;
  }
}