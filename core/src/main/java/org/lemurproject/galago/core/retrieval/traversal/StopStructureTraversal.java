// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.util.WordLists;
import org.lemurproject.galago.tupleflow.Parameters;

/*
 * Removes stop structures, as defined by the stopStructure file 
 *  (may need to be appended with new structures)
 * 
 * @author sjh, jbing
 */
public class StopStructureTraversal extends Traversal {

  public static Set<String> stopstructures = null;
  private static Logger logger = Logger.getLogger("StopStructureTraversal");

  public StopStructureTraversal(Retrieval retrieval, Parameters queryParameters) throws IOException {

    if (stopstructures == null) {
      // default to 'stopStructure' list
      String stopstructurelist = queryParameters.get("stopstructurelist", retrieval.getGlobalParameters().get("stopstructurelist", "stopStructure"));
      Set<String> ss_set = WordLists.getWordList(stopstructurelist);
      stopstructures = new TreeSet();
      for (String ss : ss_set) {
        // need to ensure that each ss ends with a space (ensures terms are not cutoff)
        stopstructures.add(ss.trim() + " ");
      }
    }
  }

  @Override
  public void beforeNode(Node node) throws Exception {
  }

  @Override
  public Node afterNode(Node original) throws Exception {
    if (original.getOperator().equals("stopstructure")) {
      Node newHead = new Node("combine", original.getInternalNodes());
      // recursively find node with an array of text nodes 
      Node parent = newHead;
      while (parent.numChildren() == 1 && !parent.getChild(0).getOperator().equals("text")) {
        parent = parent.getChild(0);
      }

      if (parent.numChildren() >= 1 && parent.getChild(0).getOperator().equals("text")) {
        removeStopStructure(parent);
      } else {
        logger.info("Unable to remove stop structure, could not find array of text-only nodes in :\n" + original.toPrettyString() );
      }
      return newHead;
    }
    return original;
  }

  private void removeStopStructure(Node parent) {
    String queryString = "";
    for (Node child : parent.getInternalNodes()) {
      if (child.getOperator().equals("text")) {
        queryString += child.getDefaultParameter() + " ";
      } else {
        // found something that is not a text node -- can not construct a query string, returning.
        logger.info("Unable to remove stop structure, could not find array of text-only nodes in :\n" + parent.toPrettyString() );
        return;
      }
    }
    queryString = queryString.trim() + " ";
    String longestStopStruct = "";
    for (String ss : stopstructures) {
      if (ss.length() > longestStopStruct.length() && queryString.startsWith(ss)) {
        longestStopStruct = ss;
      }
    }

    // if the queruy starts with a ss, remove it.
    if (!longestStopStruct.isEmpty()) {
      queryString = queryString.replaceFirst(longestStopStruct, "").trim();

      String[] queryItems = queryString.split(" ");
      parent.clearChildren();

      for (int i = 0; i < queryItems.length; i++) {
        Node Child = new Node("text", queryItems[i]);
        parent.addChild(Child);
      }
    }
  }
}