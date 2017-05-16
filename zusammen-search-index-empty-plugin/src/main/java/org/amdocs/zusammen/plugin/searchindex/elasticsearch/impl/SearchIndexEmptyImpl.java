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


import org.amdocs.zusammen.commons.health.data.HealthInfo;
import org.amdocs.zusammen.datatypes.SessionContext;
import org.amdocs.zusammen.datatypes.response.Response;
import org.amdocs.zusammen.datatypes.searchindex.SearchCriteria;
import org.amdocs.zusammen.datatypes.searchindex.SearchResult;
import org.amdocs.zusammen.sdk.searchindex.types.SearchIndexElement;

public class SearchIndexEmptyImpl implements org.amdocs.zusammen.sdk.searchindex.SearchIndex {

  @Override
  public Response<HealthInfo> checkHealth(SessionContext sessionContext) {
      return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> createElement(SessionContext sessionContext, SearchIndexElement element) {
    return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> updateElement(SessionContext sessionContext, SearchIndexElement element) {
    return new Response(Void.TYPE);
  }

  @Override
  public Response<Void> deleteElement(SessionContext sessionContext, SearchIndexElement element) {
    return new Response(Void.TYPE);
  }

  @Override
  public Response<SearchResult> search(SessionContext sessionContext, SearchCriteria
      searchCriteria) {
    return new Response(new SearchResult() {
    });
  }

}