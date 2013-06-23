// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.KeyValueReader;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.DataIterator;

import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads a binary file of document names produced by DocumentNameWriter2
 *
 * @author sjh
 */
public class DiskNameReader extends KeyValueReader implements NamesReader {

  /**
   * Creates a new instance of DiskNameReader
   */
  public DiskNameReader(String fileName) throws IOException {
    super(BTreeFactory.getBTreeReader(fileName));
  }

  public DiskNameReader(BTreeReader r) {
    super(r);
  }

  // gets the document name of the internal id index.
  @Override
  public String getDocumentName(int index) throws IOException {
    byte[] data = reader.getValueBytes(Utility.fromLong(index));
    if (data == null) {
      return null;
    }
    return Utility.toString(data);
  }

  // gets the document id for some document name
  @Override
  public int getDocumentIdentifier(String documentName) throws IOException {
    throw new UnsupportedOperationException("This index file does not support doc name -> doc int mappings");
  }

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    return new ValueIterator(getIterator());
  }

  @Override
  public KeyIterator getIterator() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Map<String, NodeType> getNodeTypes() {
    HashMap<String, NodeType> types = new HashMap<String, NodeType>();
    types.put("names", new NodeType(ValueIterator.class));
    return types;
  }

  @Override
  public KeyToListIterator getIterator(Node node) throws IOException {
    if (node.getOperator().equals("names")) {
      return (KeyToListIterator) getNamesIterator();
    } else {
      throw new UnsupportedOperationException(
              "Index doesn't support operator: " + node.getOperator());
    }
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator {

    protected BTreeReader input;
    protected BTreeReader.BTreeIterator iterator;

    public KeyIterator(BTreeReader input) throws IOException {
      super(input);
    }

    public boolean skipToKey(int identifier) throws IOException {
      // TODO stop casting document to int
      return skipToKey(Utility.fromLong(identifier));
    }

    public String getCurrentName() throws IOException {
      return Utility.toString(getValueBytes());
    }

    public int getCurrentIdentifier() throws IOException {
      // TODO stop casting document to int
      return (int) Utility.toLong(getKey());
    }

    @Override
    public String getValueString() {
      try {
        return Utility.toString(getValueBytes());
      } catch (IOException e) {
        return "Unknown";
      }
    }

    @Override
    public String getKeyString() {
      return Long.toString(Utility.toLong(getKey()));
    }

    @Override
    public KeyToListIterator getValueIterator() throws IOException {
      return new ValueIterator(this);
    }
  }

  public class ValueIterator extends KeyToListIterator implements DataIterator<String>, NamesReader.NamesIterator {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    @Override
    public int currentCandidate() {
      try {
        // TODO stop casting document to int
        return (int) Utility.toLong(iterator.getKey());
      } catch (IOException ioe) {
        return Integer.MAX_VALUE;
      }
    }

    @Override
    public void syncTo(int identifier) throws IOException {
      iterator.skipToKey(Utility.fromLong(identifier));
    }

    @Override
    public void movePast(int identifier) throws IOException {
      iterator.skipToKey(Utility.fromLong(identifier + 1));
    }

    @Override
    public String getValueString() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      StringBuilder sb = new StringBuilder();
      sb.append(ki.getCurrentIdentifier());
      sb.append(",");
      sb.append(ki.getCurrentName());
      return sb.toString();
    }

    @Override
    public long totalEntries() {
      return reader.getManifest().getLong("keyCount");
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public String getData() {
      try {
        return getCurrentName();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public String getCurrentName() throws IOException {
      if (context.document == this.getCurrentIdentifier()) {
        KeyIterator ki = (KeyIterator) iterator;
        return ki.getCurrentName();
      }
      // return null by default
      return null;
    }

    @Override
    public int getCurrentIdentifier() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.getCurrentIdentifier();
    }

    @Override
    public String getKeyString() throws IOException {
      return "names";
    }

    @Override
    public AnnotatedNode getAnnotatedNode() throws IOException {
      String type = "names";
      String className = this.getClass().getSimpleName();
      String parameters = "";
      int document = currentCandidate();
      boolean atCandidate = hasMatch(this.context.document);
      String returnValue = getCurrentName();
      List<AnnotatedNode> children = Collections.EMPTY_LIST;

      return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
    }
  }
}
