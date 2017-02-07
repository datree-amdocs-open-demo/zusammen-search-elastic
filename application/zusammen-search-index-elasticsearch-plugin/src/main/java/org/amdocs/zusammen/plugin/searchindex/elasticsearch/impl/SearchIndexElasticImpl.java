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


package org.amdocs.zusammen.plugin.searchindex.elasticsearch.impl;


import org.amdocs.zusammen.datatypes.SessionContext;
import org.amdocs.zusammen.datatypes.searchindex.SearchCriteria;
import org.amdocs.zusammen.datatypes.searchindex.SearchResult;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.ElementSearchIndex;
import org.amdocs.zusammen.plugin.searchindex.elasticsearch.SearchIndexServices;
import org.amdocs.zusammen.sdk.searchindex.types.SearchIndexElement;

public class SearchIndexElasticImpl implements org.amdocs.zusammen.sdk.searchindex.SearchIndex {

  private ElementSearchIndex elementSearchIndex;
  private SearchIndexServices searchIndexServices;

  @Override
  public void createElement(SessionContext sessionContext, SearchIndexElement element) {
    getElementSearchIndex().createElement(sessionContext, element);
  }

  @Override
  public void updateElement(SessionContext sessionContext, SearchIndexElement element) {
    getElementSearchIndex().updateElement(sessionContext, element);
  }

  @Override
  public void deleteElement(SessionContext sessionContext, SearchIndexElement element) {
    getElementSearchIndex().deleteElement(sessionContext, element);
  }

  @Override
  public SearchResult search(SessionContext sessionContext, SearchCriteria searchCriteria) {
    return getSearchIndexServices().search(sessionContext, searchCriteria);
  }

  private ElementSearchIndex getElementSearchIndex() {
    if (elementSearchIndex == null) {
      elementSearchIndex = new ElementSearchIndex();
    }
    return elementSearchIndex;
  }

  private SearchIndexServices getSearchIndexServices() {
    if (elementSearchIndex == null) {
      searchIndexServices = new SearchIndexServices();
    }
    return searchIndexServices;
  }

}