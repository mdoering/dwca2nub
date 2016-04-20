package org.gbif.giasip;

import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *
 */
public class DwcaRecord {
  public Map<Term, String> core = Maps.newHashMap();
  public Map<Term, List<Map<Term, String>>> extensions = Maps.newHashMap();

  public void addExtensionRecord(Term rowType, Map<Term, String> record){
    if (!extensions.containsKey(rowType)) {
      extensions.put(rowType, Lists.<Map<Term, String>>newArrayList());
    }
    extensions.get(rowType).add(record);
  }

  public void setHigherRank(Rank rank, String name) {
    switch (rank) {
      case KINGDOM:
        core.put(DwcTerm.kingdom, name);
        break;
      case PHYLUM:
        core.put(DwcTerm.phylum, name);
        break;
      case CLASS:
        core.put(DwcTerm.class_, name);
        break;
      case ORDER:
        core.put(DwcTerm.order, name);
        break;
      case FAMILY:
        core.put(DwcTerm.family, name);
        break;
      case GENUS:
        core.put(DwcTerm.genus, name);
        break;
      case SUBGENUS:
        core.put(DwcTerm.subgenus, name);
        break;
    }
  }
}
