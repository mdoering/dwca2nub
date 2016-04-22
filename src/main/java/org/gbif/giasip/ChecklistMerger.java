package org.gbif.giasip;

import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.model.common.LinneanClassification;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.checklistbank.NameUsageService;
import org.gbif.api.vocabulary.NameType;
import org.gbif.api.vocabulary.Rank;
import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.clb.LookupUsage;
import org.gbif.clb.LookupUsageMatch;
import org.gbif.clb.LookupUsageMatchWsClient;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveFactory;
import org.gbif.dwca.io.DwcaWriter;
import org.gbif.dwca.record.Record;
import org.gbif.dwca.record.StarRecord;
import org.gbif.io.TabWriter;
import org.gbif.nameparser.NameParser;
import org.gbif.nameparser.UnparsableException;
import org.gbif.utils.file.FileUtils;
import org.gbif.utils.file.csv.CSVReader;
import org.gbif.ws.client.BaseWsClient;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.ws.rs.core.UriBuilder;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.WebResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ChecklistMerger {
  private static final Logger LOG = LoggerFactory.getLogger(ChecklistMerger.class);
  private static final String GBIF_CODE = "GBIF";
  private static final Term OCC_COUNT_TERM = TermFactory.instance().findTerm("http://rs.gbif.org/terms/1.0/occurrenceCount");
  private static final Term DISTRIBUTION_ROWTYPE = TermFactory.instance().findTerm("http://rs.gbif.org/issg/terms/Distribution");
  private static final Term INVASIVENESS_TERM = TermFactory.instance().findTerm("http://rs.gbif.org/issg/terms/invasiveness");

  private final NameUsageMatchingService matchingService;
  private final LookupUsageMatchWsClient matchingClient;
  private final NameParser parser = new NameParser();
  private final Joiner spaceJoin = Joiner.on(" ").skipNulls();

  private final NameUsageService usageService;
  private final File dwcaDir;
  private final File datasets;
  private final Set<String> datasetIDs = Sets.newHashSet();
  // records by gbif taxonKey or negative ones for unmatched names
  private Map<Integer, DwcaRecord > taxa = Maps.newHashMap();
  private Map<Integer, Integer> taxonKey2ID = Maps.newHashMap();
  private int unmatchedCounter = 0;
  private File distributionFile;
  private int distributionCols;

  public ChecklistMerger(URI api, File dwcaDir) {
    Properties props = new Properties();
    LOG.info("Using GBIF API at {}", api);
    props.setProperty("checklistbank.ws.url", api.toString());
    String matchUri = UriBuilder.fromUri(api).path("species/match").build().toString();
    props.setProperty("checklistbank.match.ws.url", matchUri);
    Injector inj = Guice.createInjector(new ChecklistBankWsClientModule(props, true, true));
    this.matchingService = inj.getInstance(NameUsageMatchingService.class);
    this.usageService = inj.getInstance(NameUsageService.class);

    // really hacky, please excuse...
    WebResource apiResource = null;
    try {
      Field f = BaseWsClient.class.getDeclaredField("resource");
      f.setAccessible(true);
      apiResource = ((WebResource) f.get(usageService)).uri(URI.create("/v1/"));

    } catch (NoSuchFieldException | IllegalAccessException e) {
      Throwables.propagate(e);
    }
    this.matchingClient = new LookupUsageMatchWsClient(apiResource);

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
    final int unmatchedBefore = unmatchedCounter;
    for (StarRecord star : archive) {
      DwcaRecord rec = toRec(star.core());
      addCore(star, rec, sourceCode);
      addExtensionData(star, rec, sourceCode);
      counter++;
    }
    LOG.info("Added source {} with {} records. {} names could not be matched", sourceCode, counter, unmatchedCounter-unmatchedBefore);
  }

  private void addExtensionData(StarRecord star, DwcaRecord rec, String sourceCode) {
    for (Term rowType : star.rowTypes()) {
      for (Record ext : star.extension(rowType)) {
        Map<Term, String> map = toMap(ext);
        map.put(DwcTerm.datasetID, sourceCode);
        rec.addExtensionRecord(rowType, map);
      }
    }
  }

  private static String nameKey(String name) {
    return name.toUpperCase().trim().replaceAll("\\s+", "_");
  }

  private void addCore(StarRecord star, DwcaRecord rec, String sourceCode) {
    String kingdom = star.core().value(DwcTerm.kingdom);
    String name = star.core().value(DwcTerm.scientificName);
    String author = star.core().value(DwcTerm.scientificNameAuthorship);
    String year = star.core().value(DwcTerm.namePublishedInYear);

    try {

      String sciName;
      if (!Strings.isNullOrEmpty(author) && !name.contains(author)) {
        sciName = name + " " + author;
      } else {
        sciName = name;
      }
      ParsedName pn = parser.parse(sciName, null);
      name = pn.canonicalName();
      author = pn.getAuthorship();
      year = pn.getYear();
    } catch (UnparsableException e) {
      if (!e.type.equals(NameType.VIRUS)) {
        LOG.warn("Failed to parse name {}", name);
      }
    }

    // author aware matching
    LookupUsageMatch lmatch = matchingClient.match(name, author, kingdom, null, year, false);
    LookupUsage lookup = lmatch.hasMatch() ? lmatch.getMatch() : new LookupUsage();

    // classic matching
    LinneanClassification cl = new NameUsageMatch();
    cl.setKingdom(kingdom);
    cl.setPhylum(star.core().value(DwcTerm.phylum));
    cl.setClazz(star.core().value(DwcTerm.class_));
    cl.setOrder(star.core().value(DwcTerm.order));
    cl.setFamily(star.core().value(DwcTerm.family));
    NameUsageMatch match = matchingService.match(name, null, cl, true, false);

    int matchKey = match.getUsageKey() != null ? match.getUsageKey() : -1;
    int lookupKey = lmatch.hasMatch() ? lmatch.getMatch().getKey() : -1;
    if (matchKey != lookupKey) {
      LOG.warn("DIFFERENT MATCHES FOR {} > {}:{} |vs| {}:{}", spaceJoin.join(name, author, year),
          match.getUsageKey(), match.getScientificName(),
          lookup.getKey(), spaceJoin.join(lookup.getCanonical(), lookup.getAuthorship()));
    }

    // add to core using the most reliable of the 2 matches
    Integer taxonKey;
    rec.core.put(DwcTerm.datasetID, sourceCode);
    if (lmatch.hasMatch() && (match.getUsageKey() == null || author != null)  ) {
      taxonKey = lookup.getKey();
      LOG.debug("Add lookup {}:{}", taxonKey, spaceJoin.join(lookup.getCanonical(), lookup.getAuthorship()));
      if (!taxa.containsKey(taxonKey)) {
        addBackboneTaxonomy(rec, lmatch.getMatch());
        taxa.put(taxonKey, rec);
      }

    } else if (match.getUsageKey() != null) {
      taxonKey = match.getUsageKey();
      LOG.debug("Add match {}:{}", taxonKey, match.getScientificName());
      if (!taxa.containsKey(taxonKey)) {
        addBackboneTaxonomy(rec, match);
        taxa.put(taxonKey, rec);
      }

    } else {
      taxonKey = -1 * unmatchedCounter++;
      LOG.warn("Could not match: {}", name);
      addBackboneTaxonomy(rec, name, author);
      taxa.put(taxonKey, rec);
    }
  }

  private void addBackboneTaxonomy(DwcaRecord rec, String sciname, String authorship) {
    rec.core.put(DwcTerm.scientificName, sciname);
    rec.core.put(DwcTerm.scientificNameAuthorship, authorship);
  }

  private void addBackboneTaxonomy(DwcaRecord rec, LookupUsage match) {
    rec.core.put(DwcTerm.taxonID, String.valueOf(match.getKey()));
    // get full usage
    NameUsage usage = usageService.get(match.getKey(), null);
    rec.core.put(DwcTerm.taxonomicStatus, usage.getTaxonomicStatus().name().toLowerCase());
    rec.core.put(DwcTerm.scientificName, usage.getScientificName());
    rec.core.put(DwcTerm.taxonRank, usage.getRank().name().toLowerCase());
    rec.core.remove(DwcTerm.scientificNameAuthorship);

    // copy classification
    addClassification(rec, usage);

    // add accepted name
    if (usage.isSynonym()) {
      rec.core.put(DwcTerm.acceptedNameUsage, usage.getAccepted());
    }
  }
  private void addBackboneTaxonomy(DwcaRecord rec, NameUsageMatch match) {
    rec.core.put(DwcTerm.taxonID, match.getUsageKey().toString());
    // copy name & rank
    rec.core.put(DwcTerm.scientificName, match.getScientificName());
    rec.core.remove(DwcTerm.scientificNameAuthorship);
    rec.core.put(DwcTerm.taxonRank, match.getRank().name().toLowerCase());
    rec.core.put(DwcTerm.taxonomicStatus, match.getStatus().name().toLowerCase());

    // copy classification
    addClassification(rec, match);

    // get accepted name?
    if (match.isSynonym()) {
      LOG.debug("Retrieve accepted name for {} [{}]", match.getScientificName(), match.getUsageKey());
      NameUsage synonym = usageService.get(match.getUsageKey(), null);
      rec.core.put(DwcTerm.acceptedNameUsage, synonym.getAccepted());
    }
  }

  private void addClassification(DwcaRecord rec, LinneanClassification cl) {
    for (Rank r : Rank.LINNEAN_RANKS) {
      rec.setHigherRank(r, cl.getHigherRank(r));
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

  public void writeArchive() throws IOException, InterruptedException {
    LOG.info("Build archive");
    DwcaWriter writer = new DwcaWriter(DwcTerm.Taxon, dwcaDir, false);
    int id = 0;
    for (Map.Entry<Integer, DwcaRecord> tax : taxa.entrySet()) {
      // new core record
      id++;
      writer.newRecord(String.valueOf(id));
      // remember taxonKey to id mapping for merging in gbif occurrences later
      taxonKey2ID.put(tax.getKey(), id);
      DwcaRecord rec = tax.getValue();
      for (Map.Entry<Term, String> col : rec.core.entrySet()) {
        writer.addCoreColumn(col.getKey(), col.getValue());
      }
      // extensions
      for (Term rowType : rec.extensions.keySet()) {
        List<Map<Term, String>> records = rec.extensions.get(rowType);
          // make sure the writer knows about the GBIF occurrence columns
        if (DISTRIBUTION_ROWTYPE.equals(rowType)) {
          LinkedHashMap<Term, String> firstDistRec = Maps.newLinkedHashMap();
          // make sure this is the same order as used in addGbifOccurrences()
          firstDistRec.put(DwcTerm.datasetID, "");
          firstDistRec.put(DwcTerm.countryCode, "");
          firstDistRec.put(DwcTerm.establishmentMeans, "");
          firstDistRec.put(INVASIVENESS_TERM, "");
          firstDistRec.put(OCC_COUNT_TERM, "");
          firstDistRec.putAll(records.remove(0));
          writer.addExtensionRecord(rowType, firstDistRec);
        }
        // now write all the other records
        for (Map<Term, String> extRec : records) {
          writer.addExtensionRecord(rowType, extRec);
        }
      }
    }
    writer.close();

    // remember mappings for future merge of gbif distribution records
    distributionFile = new File(dwcaDir, writer.getDataFiles().get(DISTRIBUTION_ROWTYPE));
    distributionCols = writer.getTerms(DISTRIBUTION_ROWTYPE).size();
  }

  /**
   * Adds gbif occurrences as distribution records, appending to the already existing distributions file in the generated archive.
   * Make sure the writeArchive method was called before!
   *
   * Expected oder of columns is: taxonKey, countrycode, occ-count
   * @param occF gbif countries file as generated by the Hive query explained in the README!
   */
  public void addGbifOccurrences(File occF) throws IOException {
    LOG.info("Add GBIF occurrences from {}", occF.getAbsolutePath());

    CSVReader reader = new CSVReader(occF, "UTF8", "\t", null, 0);
    try(FileWriter fw = new FileWriter(distributionFile, true)) {
      TabWriter tw = new TabWriter(fw);
      for (String[] row : reader) {
        String[] row2 = new String[distributionCols+1];
        Integer taxonKey = Integer.valueOf(row[0]);
        if (!taxonKey2ID.containsKey(taxonKey)) {
          LOG.warn("TaxonKey {} not present in archive, ignore", taxonKey);

        } else {
          Integer key = taxonKey2ID.get(taxonKey);
          row2[0] = key.toString();
          row2[1] = GBIF_CODE;
          row2[2] = row[1];
          row2[5] = row[2];
          tw.write(row2);
        }
      }
      tw.close();
    }
    LOG.info("Added {} GBIF occurrences", reader.getReadRows());
    reader.close();
  }

  public static void main(String[] args) throws Exception {
    ChecklistMerger merger = new ChecklistMerger(new URI("http://api.gbif-uat.org/v1/"), new File("/Users/markus/Desktop/giasip"));

    merger.add(new File("dwca-gisd.zip"), "GISD");
    merger.add(new File("dwca-isc.zip"), "ISC");
    merger.writeArchive();
    merger.addGbifOccurrences(new File("gbif-country.csv"));
    System.out.println("DONE!");
  }
}
