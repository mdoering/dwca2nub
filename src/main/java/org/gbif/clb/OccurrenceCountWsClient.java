package org.gbif.clb;

import org.gbif.api.vocabulary.Country;
import org.gbif.checklistbank.ws.util.SimpleParameterMap;

import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OccurrenceCountWsClient {
  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceCountWsClient.class);

  private final WebResource resource;

  /**
   * @param resource pointing to API root
   */
  public OccurrenceCountWsClient(WebResource resource) {
    this.resource = resource.path("occurrence/count");
    LOG.info("Created OccurrenceCountWsClient for {}", this.resource);
  }

  public long count(Country country) {
    SimpleParameterMap parameters = new SimpleParameterMap()
        .param("country", country.getIso2LetterCode());
    return resource.queryParams(parameters).get(Long.class);
  }

  public long count(int taxonKey) {
    SimpleParameterMap parameters = new SimpleParameterMap()
        .param("taxonKey", String.valueOf(taxonKey));
    return resource.queryParams(parameters).get(Long.class);
  }

  public long count(int taxonKey, Country country) {
    SimpleParameterMap parameters = new SimpleParameterMap()
        .param("taxonKey", String.valueOf(taxonKey))
        .param("country", country.getIso2LetterCode());
    return resource.queryParams(parameters).get(Long.class);
  }
}
