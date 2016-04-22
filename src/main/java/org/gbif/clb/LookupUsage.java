package org.gbif.clb;

import org.gbif.api.vocabulary.Kingdom;
import org.gbif.api.vocabulary.Rank;

import java.util.Objects;

/**
 * Simple usage representing the minimal nub usage info needed to match names.
 */
public class LookupUsage {
    private int key;
    private String canonical;
    private String authorship;
    private String year;
    private Rank rank;
    private Kingdom kingdom;
    private boolean deleted;

    public LookupUsage() {
    }

    public String getAuthorship() {
        return authorship;
    }

    public void setAuthorship(String authorship) {
        this.authorship = authorship;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getCanonical() {
        return canonical;
    }

    public void setCanonical(String canonical) {
        this.canonical = canonical;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public Kingdom getKingdom() {
        return kingdom;
    }

    public void setKingdom(Kingdom kingdom) {
        this.kingdom = kingdom;
    }

    public Rank getRank() {
        return rank;
    }

    public void setRank(Rank rank) {
        this.rank = rank;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, rank, kingdom, canonical, authorship, year, deleted);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final LookupUsage other = (LookupUsage) obj;
        return Objects.equals(this.key, other.key)
                && Objects.equals(this.rank, other.rank)
                && Objects.equals(this.year, other.year)
                && Objects.equals(this.kingdom, other.kingdom)
                && Objects.equals(this.canonical, other.canonical)
                && Objects.equals(this.authorship, other.authorship)
                && Objects.equals(this.deleted, other.deleted);
    }

}
