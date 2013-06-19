/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DisjointExtentsIterator extends DisjointIndexesIterator implements MovableExtentIterator, CountIterator {

  public DisjointExtentsIterator(Collection<MovableExtentIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int count() {
    return ((CountIterator) head).count();
  }

  @Override
  public int maximumCount() {
    return ((CountIterator) head).maximumCount();
  }

  @Override
  public ExtentArray extents() {
    return ((MovableExtentIterator) head).extents();
  }

  @Override
  public ExtentArray getData() {
    return ((MovableExtentIterator) head).getData();
  }

  @Override
  public byte[] key() {
    return Utility.fromString("DisEI");
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "extents";
    String className = this.getClass().getSimpleName();
    String parameters = this.getKeyString();
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = extents().toString();
    List<AnnotatedNode> children = new ArrayList();
    for (MovableIterator child : this.allIterators) {
      children.add(child.getAnnotatedNode());
    }

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
