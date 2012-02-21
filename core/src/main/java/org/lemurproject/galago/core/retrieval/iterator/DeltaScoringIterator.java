// BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;

/**
 *
 * @author irmarc
 */
public interface DeltaScoringIterator extends ScoreValueIterator {
  public void score(DeltaScoringContext context);
  public void maximumDifference(DeltaScoringContext context);
  public long totalEntries();
}