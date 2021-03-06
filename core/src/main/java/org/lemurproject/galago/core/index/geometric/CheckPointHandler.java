/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.File;
import java.io.IOException;

import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

/**
 * 
 * @author sjh
 */
public class CheckPointHandler {
  private String path;
  
  public void setDirectory(String dir){
    this.path = dir + File.separator + "checkpoint" ;
    FSUtil.makeParentDirectories(path);
  }
  
  public void saveCheckpoint(Parameters checkpoint) throws IOException {
    StreamUtil.copyStringToFile(checkpoint.toString(), new File(path));
  }
  
  public Parameters getRestore() throws IOException {
    return Parameters.parseFile( new File(path) );
  }
}
