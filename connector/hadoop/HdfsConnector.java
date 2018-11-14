package fr.edf.dco.common.connector.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import fr.edf.dco.common.connector.base.ConnectorException;
import fr.edf.dco.common.connector.base.Constants;

/**
 * HDFS Files access connector
 * 
 * @author fahd-externe.essid@edf.fr
 */
public class HdfsConnector extends AbstractHadoopConnector {

  //-----------------------------------------------------------------
  // CONSTRUCTOR
  //-----------------------------------------------------------------

  /**
   * Creating HDFS connector using configuration xml files
   * and kerberos user and keytab
   */
  public HdfsConnector(String coreSiteFile,
      String hdfsSiteFile,
      String krb5,
      String user,
      String keytab) 
  {
    super(coreSiteFile, hdfsSiteFile, krb5, user, keytab);
  }

  /**
   * Creating HDFS connector using configuration xml files
   * Non secured
   */
  public HdfsConnector(String coreSiteFile, String hdfsSiteFile) {
    super(coreSiteFile, hdfsSiteFile);
  }
  
  /**
   * Create a hdfs connector with default parameters
   * @param user
   * @param keytab
   * @param secured
   */
  public HdfsConnector(String user, String keytab, boolean secured) {
    super(Constants.DEFAULT_CORE_SITE, Constants.DEFAULT_HDFS_SITE, Constants.DEFAULT_KRB_CONF, user, keytab);
    this.secured = secured;
  }

  //-----------------------------------------------------------------
  // IMPLEMENTATION
  //-----------------------------------------------------------------

  /**
   * Configure hdfs connector from running instance xml properties files 
   */
  private void configure() throws ConnectorException, IOException {    
    conf = new Configuration();

    if (secured) {
      secure();
    }


    conf.addResource(coreSiteFile);
    conf.addResource(baseSiteFile);
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());    
  }

  /**
   * HDFS Connection
   */
  public void connect() throws ConnectorException {
    try {
      configure();
      this.fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new ConnectorException(e.getMessage());
    }
  }

  /**
   * HDFS Deconnection
   */
  public void disconnect() throws ConnectorException {
    try {
      fs.close();
    } catch (IOException e) {
      throw new ConnectorException(e.getMessage());
    }
  }

  /**
   * Returns Array of Hadoop File Objects located in the given path
   * If Compressed files are found, they will be decompressed in the tempFolder
   */
  public List<HadoopFile> getFiles(String folder) throws ConnectorException, IOException {
    Path path = new Path(folder);
    ArrayList<HadoopFile> files = new ArrayList<HadoopFile>();

    if (path != null) {
      if (fs.exists(path)) {
        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(path, true);
        while (fileStatusIterator.hasNext()) {
          LocatedFileStatus fileStatus = fileStatusIterator.next();
          String fileName = fileStatus.getPath().getName();

          // ignoring files like _SUCCESS
          if(fileName.startsWith("_")) {
            continue;
          }

          // if extension patterns are set, ignoring not matching files
          if (fileExtensionsPatterns != null) {
            if (! extensionMatch(fileName)) {
              continue;
            }
          }

          files.add(new HadoopFile(fs, fileStatus.getPath()));
        }
      } else {
        throw new ConnectorException("Path not found in HDFS : " + folder);
      }
    }

    return files;
  }

  /**
   * Uncompress zip and tar.gz files and extract files to ouputDir
   * @param file
   * @param attachArchiveName : if set to true the files will have the original archive name with a "~" separator
   */
  public void uncompress(String file, String outputDir, boolean attachArchiveName) throws ConnectorException, IOException {
    Path outputPath;
    ArchiveInputStream input = null;
    ArchiveEntry entry;
    boolean duplicate = false;

    // badly named duplicates archives
    if (file.contains(".zip.") || file.contains(".zip_") || file.contains("tar.gz_") || file.contains("tar.gz.")) {
      duplicate = true;
      int index = file.indexOf(".zip");
      int from = 5; //"length(.zip. || .sip_)
      String extension = ".zip";
      
      if (index == -1) {
        index = file.indexOf(".tar.gz");
        from = 8; //length(.tar.gz.)
        extension = ".tar.gz";
      }
            
      String toCut = file.substring(index + from , file.length());
      String destination = file.substring(0, index + from - 1).replace(extension, "_" + toCut + extension);
      destination = outputDir + destination.substring(destination.lastIndexOf("/"), destination.length());
      copy(file, destination);
      
      file  = destination;
    }
    
    // creating input
    if (file.endsWith(".zip")) {
      input = new ZipArchiveInputStream(fs.open(new Path(file)));
    } else if (file.endsWith(".tar.gz")) {
      input = new TarArchiveInputStream(new GzipCompressorInputStream(fs.open(new Path(file))));
    } else {
      throw new ConnectorException("Unable de decompress file, Unknown Archive type for file : " + file);
    }

    while ((entry = input.getNextEntry()) != null) {
      String name = entry.getName();

      if (fileNamePatterns != null && !fileMatchPatterns(name)) {
        continue ;
      }
      
      if (attachArchiveName) {
        name = file.substring(file.lastIndexOf("/") + 1, file.length()).replace(".", "") + "file" + "~" + name;
      }

      outputPath = new Path(outputDir, name);
      FSDataOutputStream outStream = fs.create(outputPath);

      try {
        IOUtils.copyBytes(input, outStream, 2048, false);
      } finally {
        outStream.close();
      }

      // Cleaning badly archives mess
      if (duplicate) {
        fs.delete(new Path(file), true);
      }

      // AWL feed back case where we may find csv.zip inside a zip archive (which is brilliant !!)
      if (name.contains(".zip")) {
        uncompress(outputPath.toString(), outputDir, true);
        fs.delete(outputPath, false);
      }

      // continuing with the AWL case
      if (name.lastIndexOf("~") > name.indexOf("~")) {
        String base = outputPath.toString();
        fs.rename(outputPath, new Path(base.replace(base.substring(base.indexOf("~"), base.lastIndexOf("~")), "")));
      }
    }
  }

  /**
   * Uncompress files located in fromDir in toDir
   * @param fromDir
   * @param toDir
   * @throws ConnectorException
   * @throws IOException
   */
  public void uncompressDir(String fromDir, String toDir, boolean ignoreIfExists) throws ConnectorException, IOException {
    Path path = new Path(fromDir);

    if (path != null) {
      if (fs.exists(path)) {
        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(path, true);
        while (fileStatusIterator.hasNext()) {
          LocatedFileStatus fileStatus = fileStatusIterator.next();
          String fileName = fileStatus.getPath().getName();

          if (fileName.endsWith(".zip") || fileName.endsWith(".tar.gz")) {
            // if pattern names are set, ignoring not matching files
            if (archiveNamePatterns != null) {
              if (! archiveMatchPatterns(fileName)) {
                continue;
              }
            }

            if (toDir != null) {
              String targetFolder = toDir + "/" + fileName.replace(".", "") + "file";
              if (!fs.exists(new Path(targetFolder)) || !ignoreIfExists) {
                try {
                  uncompress(fileStatus.getPath().toString(), targetFolder, false);
                } catch (IOException e) {
                  //TODO : complete this without terminating the loop
                }
              }
            } else {
              throw new ConnectorException("Path not found in HDFS : " + toDir);  
            }
          } 
        }
      } else {
        throw new ConnectorException("Path not found in HDFS : " + fromDir);
      }
    }
  }

  /**
   * Uncompress files located in fromDir in toDir
   * Files names will have the original archive name attached with a "~" separator
   * copy archive that won't be decompressed
   * @param fromDir
   * @param toDir
   * @param destination
   * @throws ConnectorException
   * @throws IOException
   */
  public void uncompressDirFiles(String fromDir, String toDir, String destination) throws ConnectorException, IOException {
    Path path = new Path(fromDir);

    if (path != null) {
      if (fs.exists(path)) {
        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(path, true);
        int i = 0;
        while (fileStatusIterator.hasNext()) {
          LocatedFileStatus fileStatus = fileStatusIterator.next();
          String fileName = fileStatus.getPath().getName();
          System.out.println(i++);
          
          if ((fileName.contains(".zip") || fileName.contains(".tar.gz"))) {
              // if pattern names are set, ignoring not matching files
              if (archiveNamePatterns != null && !archiveMatchPatterns(fileName)) {
                continue;
              }
              
              if (toDir != null) {
                try {
                  uncompress(fileStatus.getPath().toString(), toDir, true);
                } catch (IOException e) {
                  System.out.println("ERROR : " + fileName);
                  if(!"".equals(destination))
                  copy(fileStatus.getPath().toString(), destination);
                  continue;
                }

              } else {
                throw new ConnectorException("Path not found in HDFS : " + toDir);  
              }
          }
        }
      } else {
        throw new ConnectorException("Path not found in HDFS : " + fromDir);
      }
    }
  }
  
  public void cleanEmptyFiles(String dir) throws IOException, ConnectorException {
    Path path = new Path(dir);

    if (path != null) {
      if (fs.exists(path)) {
        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(path, true);
        while (fileStatusIterator.hasNext()) {
          LocatedFileStatus fileStatus = fileStatusIterator.next();
          
          if (fileStatus.getLen() == 0) {
            deleteFile(fileStatus.getPath());
          }

        }
      } else {
        throw new ConnectorException("Path not found in HDFS : " + dir);
      }
    }
  }
  
  public void deleteSpecificFiles(String dir) throws IOException, ConnectorException {
    Path path = new Path(dir);

    if (path != null) {
      if (fs.exists(path)) {
        RemoteIterator<LocatedFileStatus> fileStatusIterator = fs.listFiles(path, true);
        while (fileStatusIterator.hasNext()) {
          LocatedFileStatus fileStatus = fileStatusIterator.next();
          
          if (fileStatus.getPath().toString().contains("2015") && !fileStatus.getPath().toString().contains("2016")) {
            deleteFile(fileStatus.getPath());
          }

        }
      } else {
        throw new ConnectorException("Path not found in HDFS : " + dir);
      }
    }
  }

  /**
   * Create a hdfs directory
   * @param path
   */
  public void createDirectory(String path) throws IOException {
    fs.mkdirs(new Path(path));
  }

  /**
   * Copy a file from a local directory to hdfs
   * @param localFile
   * @param target
   */
  public void copyFromLocal(String localFile, String targetFile) throws IOException {
    Path local = new Path(localFile);
    Path target = new Path(targetFile);

    fs.copyFromLocalFile(local, target);
  }

  /**
   * Copy a file from to local directory from HDFS
   * @param localFile
   * @param target
   */
  public void copyToLocal(String distantFile, String localFile, boolean deleteSrc) throws IOException {
    Path distant = new Path(distantFile);
    Path local = new Path(localFile);

    fs.copyToLocalFile(deleteSrc, distant, local, true);
  }

  public void appendToFile(String file, String text) throws IOException {
    Path folderPath = new Path(file);

    if (fs.exists(folderPath)) {

      BufferedReader bfr=new BufferedReader(new InputStreamReader(fs.open(folderPath)));     //open file first
      String str = null;
      BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(folderPath, true))); 
      while ((str = bfr.readLine())!= null) {
        br.write(str); // write file content
        br.newLine();
      }

      br.write(text);  // append into file
      br.newLine();
      br.close(); // close it 
    } else {
      BufferedWriter br =new BufferedWriter(new OutputStreamWriter(fs.create(folderPath, true)));
      br.write(text);
      br.newLine();
      br.close();
    }
  }

  public BufferedWriter getFileAppender(String file) throws IOException {
    Path folderPath = new Path(file);

    if (fs.exists(folderPath)) {

      BufferedReader bfr=new BufferedReader(new InputStreamReader(fs.open(folderPath))); 
      String str = null;

      BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(folderPath, true))); 
      while ((str = bfr.readLine())!= null) {
        br.write(str); // write file content
        br.newLine();
      }

      return br; 
    } else {
      BufferedWriter br =new BufferedWriter(new OutputStreamWriter(fs.create(folderPath, true)));
      return br;
    }
  }

  public void removeDuplicateLines(String file) throws IOException {
    Path filePath = new Path(file);
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
    Set<String> lines = new HashSet<String>(); 
    String line;

    while ((line = reader.readLine()) != null) {
      lines.add(line);
    }

    reader.close();

    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)));
    for (String unique : lines) {
      writer.write(unique);
      writer.newLine();
    }

    writer.close();
  }

  /**
   * Clean folder
   * @param folder
   */
  public void deleteFolderContent(String folder) throws IllegalArgumentException, IOException {
    FileStatus[] stats = fs.listStatus(new Path(folder));

    for (FileStatus s : stats) {
      fs.delete(s.getPath(), true);
    }
  }

  /**
   * delete file given its path
   * @param folder
   */
  public void deleteFile(Path file) throws IllegalArgumentException, IOException {
    fs.delete(file, true);
  }

  /**
   * Tests if a file matches with the pattern names set with setArchivePatterns
   * @param file
   * @return
   */
  private boolean archiveMatchPatterns(String file) {
    for (String pattern : archiveNamePatterns) {
      if (file.contains(pattern)) {
        return true;
      }
    }

    return false;
  }
  
  /**
   * Tests if a file matches with the pattern names set with setFileNamePatterns
   * @param file
   * @return
   */
  private boolean fileMatchPatterns(String file) {
    for (String pattern : fileNamePatterns) {
      if (file.contains(pattern)) {
        return true;
      }
    }

    return false;
  }

  /**
   * Tests if a file matches with the pattern extensions set with setFileExtension
   * @param file
   * @return
   */
  private boolean extensionMatch(String file) {
    for (String pattern : fileExtensionsPatterns) {
      if (file.endsWith(pattern)) {
        return true;
      }
    }

    return false;
  }

  public void serializeToHdfs(Serializable object, String path) throws IOException {
    byte[] classBytes = SerializationUtils.serialize(object);
    FileUtils.writeByteArrayToFile(new File(object.getClass().getName()), classBytes);

    copyFromLocal(object.getClass().getName(), path);
  }

  public Object deserializeFromHdfs(String path) throws IOException { 
    copyToLocal(path, "toDeserialize", true);
    File file = new File("toDeserialize");
    Object object = SerializationUtils.deserialize(new FileInputStream(file));
    file.delete();

    return object;
  }

  public void copy(String source, String destination) throws IOException {
    FileUtil.copy(fs, new Path(source), this.fs, new Path(destination), false, true, this.conf);
  }
  
  public void move(String source, String destination) throws IOException {
    FileUtil.copy(fs, new Path(source), this.fs, new Path(destination), true, true, this.conf);
  }
  
  public boolean exists(String path) throws IOException {
    return fs.exists(new Path(path));
  }
  
  /**
   * Specific to Awl Message Sent files
   * @param path
   * @throws IOException
   */
  public void removeLineBreaks(String file, int fieldsCount) throws IOException {
    Path path = new Path(file);
    BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(path)));   

    List<String> lines = new ArrayList<String>(); 

    String line;
    while ((line = oldReader.readLine()) != null) {
      String l1 = line;
      
       while (l1.split(";", -1).length != fieldsCount) {
         l1 = l1.replace("\n", " ") + oldReader.readLine();
      } 
       
       lines.add(l1);
    }
    
    oldReader.close();
    deleteFile(path);
    
    BufferedWriter newWriter = new BufferedWriter(new OutputStreamWriter(fs.create(path, true))); 

    for (String s : lines) {
      newWriter.write(s);
      newWriter.newLine();
    }
    
    newWriter.close();    
  }
  
  //-----------------------------------------------------------------
  // INNER CLASS : HadoopFile
  //-----------------------------------------------------------------

  /**
   * A representation of a file located in HDFS
   */
  public static class HadoopFile {

    //-------------------------------------------------------------------
    // CONSTRUCTORS
    //-------------------------------------------------------------------

    /**
     * Constructor
     * @param fs    file system instance
     * @param path  file Path
     * @param archiveName file archive name if located under a compressed file
     */
    public HadoopFile(FileSystem fs, Path path) throws IOException {
      this.fs = fs;
      this.path = path;

      configure();
    }

    //-------------------------------------------------------------------
    // IMPLEMENTATION
    //-------------------------------------------------------------------

    /**
     * Sets file access objects
     */
    private void configure() throws IOException {
      this.name = path.getName();
      this.archiveName = buildArchiveName();

      createReader();
    }

    private String buildArchiveName() {
      String result =  path.toString().replace("/" + path.getName(), "");
      result = result.substring(result.lastIndexOf("/") + 1, result.length());

      return (result.contains(".zip") || result.contains(".gz")) ? result : null;       
    }

    /**
     * Creates a reader upon file
     */
    public void createReader() throws IOException { 
      reader = new BufferedReader(new InputStreamReader(fs.open(path)));
    }

    /**
     * Reads next line from file
     */
    public String readLine() throws IOException {
      return reader.readLine();
    }

    /**
     * Deletes file from HDFS
     */
    public void delete() throws IOException {
      fs.delete(path, false);
    }
        
    /**
     * Create XML document for xml file
     */
    public void setXmlDocument() throws IOException, SAXException, ParserConfigurationException {
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      this.xmlDocument = dBuilder.parse(fs.open(path));
      xmlDocument.getDocumentElement().normalize();
    }
    
    public long getLastModificationDate() throws IOException {
      return fs.getFileStatus(path).getModificationTime();
    }
    
    /**
     * Return node list f
     * @param tag
     * @return
     */
    public NodeList getNodesList(String tag) {
      return xmlDocument.getElementsByTagName(tag);
    }

    //-------------------------------------------------------------------
    // ACCESSORS
    //-------------------------------------------------------------------

    public String getName() {
      return name;
    }

    public String getArchive() {
      return archiveName;
    }

    public Path getPath() {
      return path;
    }

    //-------------------------------------------------------------------
    // DATA MEMBERS
    //-------------------------------------------------------------------

    private FileSystem            fs;
    private BufferedReader        reader;
    private String                name;
    private Path                  path;
    private Document              xmlDocument;
    private String                archiveName;          // Archive Name or parent folder 
  }

  //-------------------------------------------------------------------
  // ACCESSORS
  //-------------------------------------------------------------------

  public void setArchivePatterns(String[] patterns) {
    this.archiveNamePatterns = patterns;
  }

  public void setFileNamePatterns(String[] filePatterns) {
    this.fileNamePatterns = filePatterns;
  }
  
  public void setFileExtensionsPatterns(String[] exts) {
    this.fileExtensionsPatterns = exts;
  }

  //-----------------------------------------------------------------
  // DATA MEMBERS
  //-----------------------------------------------------------------

  private FileSystem                        fs;
  private String[]                          archiveNamePatterns;
  private String[]                          fileNamePatterns;
  private String[]                          fileExtensionsPatterns;
}
