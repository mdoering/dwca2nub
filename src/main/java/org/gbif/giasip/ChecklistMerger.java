package org.gbif.giasip;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.vocabulary.Rank;
import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.DwcaWriter;
import org.gbif.dwca.record.Record;
import org.gbif.dwca.record.StarRecord;
import org.gbif.utils.file.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.ws.rs.core.UriBuilder;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ChecklistMerger {
  private static final Logger LOG = LoggerFactory.getLogger(ChecklistMerger.class);

  private final NameUsageMatchingService matchingService;
  private final File dwcaDir;
  private final File datasets;
  private final Set<String> datasetIDs = Sets.newHashSet();
  private Map<Integer, DwcaRecord > taxa = Maps.newHashMap();
  private Map<String, DwcaRecord > unmatched = Maps.newHashMap();

  public ChecklistMerger(URI api, File dwcaDir) {
    Properties props = new Properties();
    String matchUri = UriBuilder.fromUri(api).path("species/match").build().toString();
    LOG.info("Using species match service at {}", matchUri);
    props.setProperty("checklistbank.match.ws.url", matchUri);
    Injector inj = Guice.createInjector(new ChecklistBankWsClientModule(props, false, true));
    this.matchingService = inj.getInstance(NameUsageMatchingService.class);

    this.dwcaDir = dwcaDir;
    this.datasets = new File(dwcaDir, "dataset");
    if (dwcaDir.exists()) {
      LOG.warn("Remove existing dwca {}", dwcaDir.getAbsolutePath());
      org.apache.commons.io.FileUtils.deleteQuietly(dwcaDir);
    }
    datasets.mkdirs();
  }

  private File constituentEmlFile(String sourceCode){
    return new File(datasets, sourceCode+".xml");
  }

  public void add(File dwca, String sourceCode) throws IOException {
    sourceCode = sourceCode.trim().replaceAll(" ", "-");
    if (datasetIDs.contains(sourceCode)) {
      throw new IllegalArgumentException("Source code already exists: "+sourceCode);
    }

    File tmpDir = FileUtils.createTempDir();
    LOG.info("Opening source {} at {}", sourceCode, dwca.getAbsoluteFile());
    Archive archive = ArchiveFactory.openArchive(dwca, tmpDir);

    // add source EML as dataset constituent
    File eml = new File(tmpDir, archive.getMetadataLocation());
    if (!eml.exists()) {
      LOG.warn("Cannot find {} EML at {}", sourceCode, eml.getAbsoluteFile());
    } else {
      LOG.info("Copy {} EML from {}", sourceCode, eml.getAbsoluteFile());
      org.apache.commons.io.FileUtils.copyFile(eml, constituentEmlFile(sourceCode));
    }

    // add data
    int counter = 0;
    int newKeys = 0;
    int unmatched = 0;
    for (StarRecord star : archive) {
      DwcaRecord rec = toRec(star.core());
      String sciname = star.core().value(DwcTerm.scientificName);

      NameUsageMatch match = matchingService.match(sciname, null, null, true, false);
      if (match.getUsageKey() == null) {
        unmatched++;
        LOG.warn("Could not match >>>{}<<< to backbone", sciname);

      } else {
        if (!taxa.containsKey(match.getUsageKey())) {
          LOG.debug("Add match {} [{}] for >>>{}<<< to archive", match.getUsageKey(), match.getScientificName(), sciname);
          addBackboneTaxonomy(rec, match);
          rec.core.put(DwcTerm.datasetID, sourceCode);
          taxa.put(match.getUsageKey(), rec);
          newKeys++;
        }
        addExtensionData(match.getUsageKey(), star);
      }
      counter++;
    }
    LOG.info("Added {} new species from source {} with {} records. {} names could not be matched", newKeys, sourceCode, counter, unmatched);
  }

  private void addExtensionData(Integer taxonKey, StarRecord star) {

  }

  private void addBackboneTaxonomy(DwcaRecord rec, NameUsageMatch match) {
    // copy name & rank
    rec.core.put(DwcTerm.scientificName, match.getScientificName());
    rec.core.remove(DwcTerm.scientificNameAuthorship);
    rec.core.put(DwcTerm.taxonRank, match.getRank().name().toLowerCase());
    rec.core.put(DwcTerm.taxonomicStatus, match.getStatus().name().toLowerCase());
    // copy classification
    for (Rank r : Rank.LINNEAN_RANKS) {
      rec.setHigherRank(r, match.getHigherRank(r));
    }
  }

  private static DwcaRecord toRec(Record core) {
    DwcaRecord dr = new DwcaRecord();
    dr.core = toMap(core);
    return dr;
  }

  private static Map<Term, String> toMap(Record rec) {
    Map<Term, String> map = new HashMap<Term, String>();
    for (Term t : rec.terms()) {
      map.put(t, rec.value(t));
    }
    return map;
  }

  public void write() throws IOException {
    DwcaWriter writer = new DwcaWriter(DwcTerm.Taxon, DwcTerm.taxonID, dwcaDir, true);
    for (Map.Entry<Integer, DwcaRecord> tax : taxa.entrySet()) {
      // core
      final String id = tax.getKey().toString();
      writer.newRecord(id);
      DwcaRecord rec = tax.getValue();
      for (Map.Entry<Term, String> col : rec.core.entrySet()) {
        writer.addCoreColumn(col.getKey(), col.getValue());
      }
      // extensions
      for (Term rowType : rec.extensions.keySet()) {
        for (Map<Term, String> extRec : rec.extensions.get(rowType)) {
          writer.addExtensionRecord(rowType, extRec);
        }
      }
    }
    writer.close();
  }



  public static void main(String[] args) throws Exception {
    ChecklistMerger merger = new ChecklistMerger(new URI("http://api.gbif-uat.org/v1/"), new File("/Users/markus/Desktop/giasip"));

    merger.add(new File("/Users/markus/Desktop/dwca-isc.zip"), "ISC");
    //merger.add(new File("/Users/markus/Desktop/dwca-gisd.zip"), "GISD");
    merger.write();
  }
}
