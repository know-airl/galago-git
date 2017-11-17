package org.lemurproject.galago.core.parse;

import co.nstant.in.cbor.CborDecoder;
import co.nstant.in.cbor.CborException;
import co.nstant.in.cbor.model.*;
import co.nstant.in.cbor.model.DataItem;
import edu.unh.cs.treccar.Data;
import edu.unh.cs.treccar.read_data.DeserializeData;
import org.apache.commons.lang3.StringEscapeUtils;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.Parameters;

import java.io.*;
import java.net.URLDecoder;
import java.util.*;

/**
 * Parser for the TREC Complex Answer Retrieval CBOR data, the pages and paragraphs.
 * <p>
 * http://trec-car.cs.unh.edu/
 *
 * @author jdalton
 */
public class TrecCarCborParser extends DocumentStreamParser {

  BufferedInputStream inputStream;

  // An iterator for either paragraphs or pages; both will not be used together.
  Iterator dataIterator;

  // Categories of document types for the CAR data.
  public enum DocumentType {
    PARAGRAPH, PAGES
  }

  // The type of CBOR document being parsed.
  private DocumentType documentType = DocumentType.PAGES;

  private String redirectPath = "/media/jeff/main/Downloads/entity-redirects/unprocessed_train.cbor.redirects.tsv";

  HashMap<String,String> redirectMap = new HashMap(); // loadRedirectMap(redirectPath);

  private HashMap<String, String> loadRedirectMap(String filePath) throws IOException {
    HashMap<String,String> redirects = new HashMap<String, String>();
    FileReader fileReader = new FileReader(filePath);
    BufferedReader bufReader = new BufferedReader(fileReader);
    try {
      String input = null;
      while ((input = bufReader.readLine()) != null) {
        String[] fields = input.split("\t");
        redirects.put(fields[0], fields[1]);
      }
    } finally {
      bufReader.close();
    }
    return redirects;
  }

  private int numDocuments = 0;
  /**
   * Creates a new TrecCarCborParser from Galago parameters.
   * Note: Requires "documentType" parameter to be specified with one of the valid
   * enum values defined above.
   */
  public TrecCarCborParser(DocumentSplit split, Parameters p) throws IOException {
    super(split, p);

    // Default mode is paragraph corpus.
    if (p.containsKey("documentType")) {
      documentType = DocumentType.valueOf(p.getAsString("documentType"));
    }
    this.inputStream = getBufferedInputStream(split);
    String filename = getFileName(split);
    System.out.println("Starting CBOR parse with type: " + documentType.name());
    switch (documentType) {
      case PARAGRAPH:
        dataIterator = DeserializeData.iterParagraphs(this.inputStream);
        break;
      case PAGES:
        dataIterator = DeserializeData.iterAnnotations(this.inputStream);
        break;
      default:
        throw new IllegalArgumentException("Invalid corpus data.");
    }
  }

  /**
   * Parses the next document from the CBOR stream.
   *
   * @return Document for Galago; returns null if there is no next document.
   * @throws IOException
   */
  @Override
  public Document nextDocument() throws IOException {
    if (!dataIterator.hasNext()) {
      return null;
    }
    numDocuments++;
  //  if (numDocuments > 50000) return null;
    switch (documentType) {
      case PARAGRAPH:
        return nextParagraph();
      case PAGES:
        return nextPage();
      default:
        throw new IllegalArgumentException("Invalid corpus data.");
    }
  }

  private Document nextParagraph() {
    Data.Paragraph paragraph = (Data.Paragraph) dataIterator.next();
    String paragraphText = textFromParagraph(paragraph);
    Document document = new Document(paragraph.getParaId(), paragraphText);
    return document;
  }


  private Document nextPage() {
    Data.Page page = (Data.Page) dataIterator.next();
    StringBuilder buffer = new StringBuilder();
    createField("title", page.getPageName(), buffer, false, true);
    for (Data.PageSkeleton skel : page.getSkeleton()) {
      recurseArticle(skel, buffer);
    }
    // TODO:JeffDalton add support for page metadata.

//    Data.PageMetadata metadata = page.getPageMetadata();

    Document result = new Document(page.getPageName(), buffer.toString());
//    res.metadata.put("title", page.getPageName());
//    res.metadata.put("inlink", inlinkCount + "");
//    res.metadata.put("kbName", kbName);
//    res.metadata.put("kbId", kbId);
//    res.metadata.put("kbType", kbType);
//    res.metadata.put("fbname", freebaseNames);
//    res.metadata.put("category", categories);
//    res.metadata.put("fbtype", freebaseTypes);
//    res.metadata.put("srcInlinks", sourceInlinks);
//    res.metadata.put("xml", wikiXml);
//    res.metadata.put("externalLinkCount", externalInlinkCount + "");
//    res.metadata.put("contextLinks", contextLinks);

    return result;
  }

  /**
   * Traverse the page skeleton sections and add the text to the buffer.
   *
   * @param skel
   * @param buffer
   */
  private static void recurseArticle(Data.PageSkeleton skel, StringBuilder buffer) {
    if (skel instanceof Data.Section) {
      final Data.Section section = (Data.Section) skel;
      createField("heading", section.getHeading(), buffer, false, true);
      buffer.append(System.lineSeparator());
      for (Data.PageSkeleton child : section.getChildren()) {
        recurseArticle(child, buffer);
      }
    } else if (skel instanceof Data.Para) {
      Data.Para para = (Data.Para) skel;
      Data.Paragraph paragraph = para.getParagraph();
      buffer.append(textFromParagraph(paragraph));
    } else if (skel instanceof Data.Image) {
      Data.Image image = (Data.Image) skel;
      for (Data.PageSkeleton child: image.getCaptionSkel()) {
        recurseArticle(child, buffer);
      }
    } else {
      throw new UnsupportedOperationException("not known skel " + skel);
    }
  }

  private static String textFromParagraph(Data.Paragraph paragraph) {
    StringBuilder buffer = new StringBuilder();
    for (Data.ParaBody body : paragraph.getBodies()) {
      if (body instanceof Data.ParaLink) {
        Data.ParaLink link = (Data.ParaLink) body;

        // Resolve redirects.
        String targetPage = link.getPageId();
//        if (redirectMap.containsKey(link.getPageId())) {
//          targetPage = redirectMap.get(link.getPageId());
//        }

        createField("link", link.getPageId(), buffer, false, false);
//        String decodedAnchors = StringEscapeUtils.unescapeHtml4(link.getAnchorText());
//        decodedAnchors = decodedAnchors.replaceAll("%20", " ");

        createField("page", link.getPage(), buffer, false, true);
        buffer.append(link.getAnchorText());
      } else if (body instanceof Data.ParaText) {
        buffer.append(((Data.ParaText) body).getText());
      } else {
        throw new UnsupportedOperationException("not known body " + body);
      }
    }
    return buffer.toString();
  }

  /**
   * Create a field with tags for a name and value pair. It includes parameters for whether the field needs to be
   * parsed (a default comma delimiter), and tokenized for galago.
   * @param fieldName
   * @param fieldValue
   * @param sb
   * @param compound
   * @param tokenize
   * @return
   */
  private static String createField(String fieldName, String fieldValue, StringBuilder sb, boolean compound, boolean tokenize) {
    List<String> values = null;
    if (compound) {
      String[] strings = fieldValue.split(",");
      values = Arrays.asList(strings);
    } else {
      values = Collections.singletonList(fieldValue);
    }
    return createField(fieldName, values, sb, tokenize);
  }

  /**
   * Create a field with tags for a name and value pair.
   * @param fieldName
   * @param values
   * @param sb
   * @param tokenize
   * @return
   */
  private static String createField(String fieldName, List<String> values, StringBuilder sb, boolean tokenize) {
    if (values == null || values.size() == 0) {
      return "";
    }
    for (String val : values) {
      start(fieldName, sb, tokenize);
      sb.append(val.trim());
      end(fieldName, sb);
    }

    //System.out.println(fieldValue);
    return sb.toString();
  }

  private static void start(String tag, StringBuilder sb, boolean tokenize) {
    sb.append("<");
    sb.append(tag);

    // This tag parameter keeps Galago from tokenizing the text in the field.
    if (!tokenize) {
      sb.append(" tokenizeTagContent=\"false\"");
    }
    sb.append(">");
  }

  private static void end(String tag, StringBuilder sb) {
    sb.append("</");
    sb.append(tag);
    sb.append("> \n");
  }

  @Override
  public void close() throws IOException {
    this.inputStream.close();
  }
}
