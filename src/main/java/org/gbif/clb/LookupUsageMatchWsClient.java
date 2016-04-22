package org.gbif.clb;

import org.gbif.checklistbank.ws.client.guice.ChecklistBankWs;
import org.gbif.checklistbank.ws.util.SimpleParameterMap;

import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupUsageMatchWsClient {
  private static final Logger LOG = LoggerFactory.getLogger(LookupUsageMatchWsClient.class);

  private final WebResource resource;

  @Inject
  public LookupUsageMatchWsClient(@ChecklistBankWs WebResource resource) {
    this.resource = resource.path("species/lookup");
    LOG.info("Created LookupUsageMatchWsClient for {}", this.resource);
  }

  public LookupUsageMatch match(String canonicalName, String authorship, String kingdom, String rank, String year, Boolean verbose) {

    // make sure author is the recombination author only

    SimpleParameterMap parameters = new SimpleParameterMap()
        .param("name", canonicalName)
        .param("authorship", authorship)
        .param("kingdom", kingdom)
        .param("rank", rank)
        .param("year", year)
        .param("verbose", Boolean.toString(verbose));

    return resource.queryParams(parameters).type(MediaType.APPLICATION_JSON).get(LookupUsageMatch.class);
  }

}
