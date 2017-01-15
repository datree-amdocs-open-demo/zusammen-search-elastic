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

package org.amdocs.zusammen.plugin.searchindex.elasticsearch.impl;


import org.amdocs.zusammen.datatypes.SessionContext;
import org.amdocs.zusammen.datatypes.searchindex.SearchCriteria;
import org.amdocs.zusammen.datatypes.searchindex.SearchResult;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.ElementSearchIndex;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.SearchIndex;
import org.amdocs.zusammen.sdk.types.searchindex.ElementSearchableData;

public class SearchIndexElasticImpl implements org.amdocs.zusammen.sdk.SearchIndex {

  private ElementSearchIndex elementSearchIndex;
  private SearchIndex searchIndex;

  @Override
  public void createElement(SessionContext sessionContext,
                            ElementSearchableData elementSearchableData) {
    getElementSearchIndex().createElement(sessionContext, elementSearchableData);
  }

  @Override
  public void updateElement(SessionContext sessionContext,
                            ElementSearchableData elementSearchableData) {
    getElementSearchIndex().updateElement(sessionContext, elementSearchableData);
  }

  @Override
  public void deleteElement(SessionContext sessionContext,
                            ElementSearchableData elementSearchableData) {
    getElementSearchIndex().deleteElement(sessionContext, elementSearchableData);
  }

  @Override
  public SearchResult search(SessionContext sessionContext, SearchCriteria searchCriteria) {
    return getSearchIndex().search(sessionContext, searchCriteria);
  }

  private ElementSearchIndex getElementSearchIndex() {
    if (elementSearchIndex == null) {
      elementSearchIndex = new ElementSearchIndex();
    }
    return elementSearchIndex;
  }

  private SearchIndex getSearchIndex() {
    if (elementSearchIndex == null) {
      searchIndex = new SearchIndex();
    }
    return searchIndex;
  }

}