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
import org.gbif.dwca.io.ArchiveField;
import org.gbif.dwca.io.ArchiveFile;
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
  private static final String ESTABLISHMENT_MEANS_UNSPECIFIED = "Unspecified";
  private static final String INVASIVENESS_UNSPECIFIED = "Unspecified";

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

  public ChecklistMerger() {
    this.matchingService = null;
    this.matchingClient = null;
    this.usageService = null;
    this.dwcaDir = null;
    this.datasets = null;
  }

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

  public void add(URI dwca, String sourceCode) throws IOException {
    sourceCode = sourceCode.trim().replaceAll(" ", "-");
    if (datasetIDs.contains(sourceCode)) {
      throw new IllegalArgumentException("Source code already exists: "+sourceCode);
    }

    // download dwca
    File tmpDir = FileUtils.createTempDir();
    LOG.info("Opening source {} at {}", sourceCode, dwca);
    Archive archive = ArchiveFactory.openArchive(dwca.toURL(), tmpDir);

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
      DwcaRecord rec = addCore(star, sourceCode);
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

  private DwcaRecord addCore(StarRecord star, String sourceCode) {
    DwcaRecord rec = toRec(star.core());
    rec.core.put(DwcTerm.datasetID, sourceCode);

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
    if (lmatch.hasMatch() && (match.getUsageKey() == null || author != null)  ) {
      taxonKey = addBackboneTaxonomy(rec, lookup.getKey());
      LOG.debug("Add lookup {}:{}", taxonKey, spaceJoin.join(lookup.getCanonical(), lookup.getAuthorship()));

    } else if (match.getUsageKey() != null) {
      taxonKey = addBackboneTaxonomy(rec, match.getUsageKey());
      LOG.debug("Add match {}:{}", taxonKey, match.getScientificName());

    } else {
      LOG.warn("Could not match: {}", name);
      taxonKey = addBackboneTaxonomy(rec, name, author);
    }

    if (!taxa.containsKey(taxonKey)) {
      taxa.put(taxonKey, rec);
    } else {
      rec = taxa.get(taxonKey);
    }

    return rec;
  }

  private int addBackboneTaxonomy(DwcaRecord rec, String sciname, String authorship) {
    rec.core.put(DwcTerm.scientificName, sciname);
    rec.core.put(DwcTerm.scientificNameAuthorship, authorship);
    return -1 * unmatchedCounter++;
  }

  /**
   * Adds backbone infos to DwcaRecord using the accetped usage in case of synoynms.
   * @return key of the accepted name usage
   */
  private int addBackboneTaxonomy(DwcaRecord rec, int usageKey) {
    NameUsage u = usageService.get(usageKey, null);
    // get accepted name?
    if (u.isSynonym()) {
      String synonym = u.getScientificName();
      u = usageService.get(u.getAcceptedKey(), null);
      LOG.debug("Retrieved accepted name {} [{}] for synonym {}", u.getScientificName(), u.getKey(), synonym);
    }

    rec.core.put(DwcTerm.taxonConceptID, String.valueOf(u.getKey()));
    // copy name & rank
    rec.core.put(DwcTerm.scientificName, u.getScientificName());
    rec.core.remove(DwcTerm.scientificNameAuthorship);
    rec.core.put(DwcTerm.taxonRank, u.getRank().name().toLowerCase());
    rec.core.put(DwcTerm.taxonomicStatus, u.getTaxonomicStatus().name().toLowerCase());

    // copy classification
    for (Rank r : Rank.LINNEAN_RANKS) {
      rec.setHigherRank(r, u.getHigherRank(r));
    }

    // return accepted taxon key
    return u.getKey();
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

    int counterIncluded = 0;
    int counterExcluded = 0;
    CSVReader reader = new CSVReader(occF, "UTF8", ";", null, 0);
    try(FileWriter fw = new FileWriter(distributionFile, true)) {
      TabWriter tw = new TabWriter(fw);
      for (String[] row : reader) {
        String[] row2 = new String[distributionCols+1];
        Integer taxonKey = Integer.valueOf(row[0]);
        if (!taxonKey2ID.containsKey(taxonKey)) {
          LOG.warn("TaxonKey {} not present in archive, ignore", taxonKey);
          counterExcluded++;

        } else {
          Integer key = taxonKey2ID.get(taxonKey);
          row2[0] = key.toString();
          row2[1] = GBIF_CODE;
          row2[2] = row[1];
          row2[3] = ESTABLISHMENT_MEANS_UNSPECIFIED;
          row2[4] = INVASIVENESS_UNSPECIFIED;
          row2[5] = row[2];
          tw.write(row2);
          counterIncluded++;
        }
      }
      tw.close();
    }
    LOG.info("Added {} GBIF occurrences, {} excluded", counterIncluded, counterExcluded);
    reader.close();
  }

  /**
   * Replaces GBIF occurrences in an existing archive
   */
  public void replaceGbifOccurrences(Archive a, File occF) throws IOException {

    ArchiveFile af = a.getExtension(DISTRIBUTION_ROWTYPE);
    distributionFile = af.getLocationFile();
    List<ArchiveField> fields = af.getFieldsSorted();
    distributionCols = fields.get(fields.size()-1).getIndex()+1;

    // copy existing file to a tmp one to read so we can rewrite the old one
    File oldDistributionFile = new File(dwcaDir, "oldDistFile.txt");
    org.apache.commons.io.FileUtils.copyFile(distributionFile, oldDistributionFile);
    distributionFile.delete();

    // mappings should be fixed
    // 0=id
    // 1=datasetID
    // 2=country code
    // 3=establishment means
    // 4=invasiveness
    // 5=occ count
    LOG.info("Removing existing GBIF occurrences");
    CSVReader reader = new CSVReader(oldDistributionFile, "UTF8", "\t", null, 0);
    int delCounter = 0;
    try(FileWriter fw = new FileWriter(distributionFile, false)) {
      TabWriter tw = new TabWriter(fw);
      for (String[] row : reader) {
        // ignore GBIF records
        if (!GBIF_CODE.equalsIgnoreCase(row[1])) {
          tw.write(row);
        } else {
          delCounter++;
        }
      }
      tw.close();
    }
    LOG.info("Removed {} GBIF occurrences", delCounter);


    LOG.info("Build id map for existing archive");
    taxonKey2ID.clear();
    for (StarRecord star : a) {
      if (!Strings.isNullOrEmpty(star.core().value(DwcTerm.taxonID))) {
        taxonKey2ID.put(Integer.valueOf(star.core().value(DwcTerm.taxonID)), Integer.valueOf(star.core().id()));
      }
    }

    addGbifOccurrences(occF);
  }

  public static void main(String[] args) throws Exception {

    ChecklistMerger merger = new ChecklistMerger();
    Archive a = ArchiveFactory.openArchive(new File("/Users/markus/Desktop/giasip"));
    merger.replaceGbifOccurrences(a, new File("gbif-country.csv"));

    //ChecklistMerger merger = new ChecklistMerger(new URI("http://api.gbif.org/v1/"), new File("/Users/markus/Desktop/giasip"));
    //merger.add(URI.create("http://giasip.gbif.org/archive.do?r=isc"), "ISC");
    //merger.add(URI.create("http://giasip.gbif.org/archive.do?r=gisd"), "GISD");
    //merger.writeArchive();
    //merger.addGbifOccurrences(new File("gbif-country.csv"));
    System.out.println("DONE!");
  }
}
