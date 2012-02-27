package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.Map;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.MovableValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Wraps the WindowIndexReader to act as a lengths reader for a
 * particular field.
 * 
 * @author irmarc
 */
public class FieldLengthsReader implements LengthsReader {

  String field;
  WindowIndexReader reader;

  public FieldLengthsReader(WindowIndexReader reader) {
    this.field = "";
    this.reader = reader;
  }

  @Override
  public int getLength(int document) throws IOException {
    LengthsReader.Iterator li = getLengthsIterator();
    li.moveTo(document);
    if (li.atCandidate(document)) {
      return li.getCurrentLength();
    } else {
      return 0;
    }
  }

  public void setField(String f) {
    this.field = f;
  }

  public Iterator getLengthsIterator(String f) throws IOException {
    return new LengthIterator(reader.getTermExtents(f));
  }

  @Override
  public Iterator getLengthsIterator() throws IOException {
    return new LengthIterator(reader.getTermExtents(field));
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public String getDefaultOperator() {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public org.lemurproject.galago.core.index.KeyIterator getIterator() throws IOException {
    return reader.getIterator();
  }

  @Override
  public MovableValueIterator getIterator(Node node) throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Parameters getManifest() {
    return reader.getManifest();
  }

  public class LengthIterator implements LengthsReader.Iterator {

    private WindowIndexReader.TermExtentIterator extentsIterator;

    public LengthIterator(WindowIndexReader.TermExtentIterator counts) {
      this.extentsIterator = counts;
    }

    @Override
    public int getCurrentLength() throws IOException {
      int total = 0;
      ExtentArray extents = extentsIterator.extents();
      for (int i = 0; i < extents.size(); i++) {
        total += extents.end(i) - extents.begin(i);
      }
      return total;
    }

    @Override
    public int getCurrentIdentifier() throws IOException {
      return extentsIterator.currentCandidate();
    }

    @Override
    public int currentCandidate() {
      return extentsIterator.currentCandidate();
    }

    @Override
    public boolean atCandidate(int identifier) {
      return (extentsIterator.currentCandidate() == identifier);
    }

    @Override
    public boolean next() throws IOException {
      return extentsIterator.next();
    }

    @Override
    public boolean moveTo(int identifier) throws IOException {
      return extentsIterator.moveTo(identifier);
    }

    @Override
    public void movePast(int identifier) throws IOException {
      extentsIterator.movePast(identifier);
    }

    @Override
    public String getEntry() throws IOException {
      return extentsIterator.getEntry();
    }

    @Override
    public long totalEntries() {
      return extentsIterator.totalEntries();
    }

    @Override
    public void reset() throws IOException {
      extentsIterator.reset();
    }

    @Override
    public boolean isDone() {
      return extentsIterator.isDone();
    }

    @Override
    public int compareTo(MovableIterator t) {
      return extentsIterator.compareTo(t);
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }
  }
}
