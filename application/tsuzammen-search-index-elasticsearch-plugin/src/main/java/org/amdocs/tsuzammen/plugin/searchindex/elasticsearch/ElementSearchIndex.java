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


import org.amdocs.tsuzammen.datatypes.SessionContext;
import org.amdocs.tsuzammen.plugin.searchindex.elasticsearch.datatypes.EsSearchableData;
import org.amdocs.tsuzammen.sdk.types.searchindex.ElementSearchableData;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.index.IndexNotFoundException;

public class ElementSearchIndex extends SearchIndex {

  public void createElement(SessionContext sessionContext,
                            ElementSearchableData elementSearchableData) {
    validation(elementSearchableData, true);
    EsSearchableData enrichedSearchableData =
        createEnrichedElasticSearchableData(sessionContext, elementSearchableData);
    String index = getEsIndex(sessionContext);
    String source = getEsSource(enrichedSearchableData);
    String searchableDataId = createSearchableDataId(sessionContext, elementSearchableData);

    getElasticSearchDao(sessionContext)
        .create(sessionContext, index, enrichedSearchableData.getType(), source, searchableDataId);
  }

  public void updateElement(SessionContext sessionContext,
                            ElementSearchableData elementSearchableData) {
    try {
      validation(elementSearchableData, true);
      String searchableDataId = createSearchableDataId(sessionContext, elementSearchableData);
      EsSearchableData esSearchableData = getEsSearchableData(elementSearchableData);

      checkIfSearchableDataExist(sessionContext, getEsIndex(sessionContext),
          esSearchableData.getType(), searchableDataId);
      createElement(sessionContext, elementSearchableData);

    } catch (IndexNotFoundException indexNotFoundExc) {
      String missingIndex = "Searchable data for tenant - '" + sessionContext.getTenant()
          + "' was not found.";
      throw new RuntimeException(missingIndex);
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }
  }

  public void deleteElement(SessionContext sessionContext,
                            ElementSearchableData elementSearchableData) {
    validation(elementSearchableData, false);
    String searchableDataId = createSearchableDataId(sessionContext, elementSearchableData);
    String index = getEsIndex(sessionContext);
    EsSearchableData esSearchableData = getEsSearchableData(elementSearchableData);

    DeleteResponse response = getElasticSearchDao(sessionContext)
        .delete(sessionContext, index, esSearchableData.getType(), searchableDataId);

    if (response.getResult().getLowercase().equals("not_found")) {
      String notFoundErrorMsg = "Delete failed - Searchable data for tenant - '" +
          sessionContext.getTenant() + "', type - '" + esSearchableData.getType() + "' and id - '"
          + searchableDataId + "' was not found.";
      throw new RuntimeException(notFoundErrorMsg);
    }
  }
}
