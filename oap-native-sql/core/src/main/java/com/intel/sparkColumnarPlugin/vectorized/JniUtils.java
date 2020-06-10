/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.sparkColumnarPlugin.vectorized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** Helper class for JNI related operations. */
public class JniUtils {
  private static final String LIBRARY_NAME = "spark_columnar_jni";
  private static boolean isLoaded = false;
  private static volatile JniUtils INSTANCE;

  public static JniUtils getInstance() throws IOException {
    if (INSTANCE == null) {
      synchronized (JniUtils.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = new JniUtils();
          } catch (IllegalAccessException ex) {
            throw new IOException("IllegalAccess");
          }
        }
      }
    }

    return INSTANCE;
  }

  private JniUtils() throws IOException, IllegalAccessException, IllegalStateException {
    if (!isLoaded) {
      try {
        loadLibraryFromJar();
      } catch (IOException ex) {
        System.loadLibrary(LIBRARY_NAME);
      }
      loadIncludeFromJar();
      isLoaded = true;
    }
  }

  static void loadLibraryFromJar() throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      final String libraryToLoad = System.mapLibraryName(LIBRARY_NAME);
      final File libraryFile = moveFileFromJarToTemp(System.getProperty("java.io.tmpdir"), libraryToLoad);
      System.load(libraryFile.getAbsolutePath());
    }
  }

  private static void loadIncludeFromJar() throws IOException, IllegalAccessException {
    synchronized (JniUtils.class) {
      final String folderToLoad = "include";
      final URLConnection urlConnection = JniUtils.class.getClassLoader().getResource("include").openConnection();
      if (urlConnection instanceof JarURLConnection) {
        final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
        copyResourcesToDirectory(jarFile, folderToLoad, System.getProperty("java.io.tmpdir") + "/" + folderToLoad);
      } else {
        throw new IOException(urlConnection.toString() + " is not JarUrlConnection");
      }
    }
  }

  private static File moveFileFromJarToTemp(final String tmpDir, String libraryToLoad) throws IOException {
    // final File temp = File.createTempFile(tmpDir, libraryToLoad);
    final File temp = new File(tmpDir + "/" + libraryToLoad);
    try (final InputStream is = JniUtils.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      } else {
        Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
      }
    }
    return temp;
  }

  /**
   * Copies a directory from a jar file to an external directory.
   */
  public static void copyResourcesToDirectory(JarFile fromJar, String jarDir, String destDir) throws IOException {
    for (Enumeration<JarEntry> entries = fromJar.entries(); entries.hasMoreElements();) {
      JarEntry entry = entries.nextElement();
      if (entry.getName().startsWith(jarDir + "/") && !entry.isDirectory()) {
        File dest = new File(destDir + "/" + entry.getName().substring(jarDir.length() + 1));
        File parent = dest.getParentFile();
        if (parent != null) {
          parent.mkdirs();
        }

        FileOutputStream out = new FileOutputStream(dest);
        InputStream in = fromJar.getInputStream(entry);

        try {
          byte[] buffer = new byte[8 * 1024];

          int s = 0;
          while ((s = in.read(buffer)) > 0) {
            out.write(buffer, 0, s);
          }
        } catch (IOException e) {
          throw new IOException("Could not copy asset from jar file", e);
        } finally {
          try {
            in.close();
          } catch (IOException ignored) {
          }
          try {
            out.close();
          } catch (IOException ignored) {
          }
        }
      }
    }
  }
}
