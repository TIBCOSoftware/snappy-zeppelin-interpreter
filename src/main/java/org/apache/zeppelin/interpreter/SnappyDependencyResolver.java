package org.apache.zeppelin.interpreter;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkContext;
import org.apache.zeppelin.spark.dep.SparkDependencyResolver;
import scala.Some;
import scala.collection.IndexedSeq;
import scala.reflect.io.AbstractFile;
import scala.tools.nsc.Global;
import scala.tools.nsc.backend.JavaPlatform;
import scala.tools.nsc.util.ClassPath;
import scala.tools.nsc.util.MergedClassPath;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.*;

public class SnappyDependencyResolver extends SparkDependencyResolver {
    private Global global;
    private ClassLoader runtimeClassLoader;
    private SparkContext sc;

    private final String[] fileExclusions = new String[] { "org.scala-lang",
            "snappy-spark", "spark-core", "spark-catalyst", "spark-graphx",
            "spark-hive", "spark-mllib", "spark-network", "spark-repl",
            "spark-sketch", "spark-sql", "spark-streaming", "spark-tags",
            "spark-unsafe", "spark-yarn", "zeppelin" };

    public SnappyDependencyResolver(Global global, ClassLoader runtimeClassLoader, SparkContext sc, String localRepoPath, String additionalRemoteRepository) {
        super(global, runtimeClassLoader, sc, localRepoPath, additionalRemoteRepository);
        this.global = global;
        this.runtimeClassLoader = runtimeClassLoader;
        this.sc = sc;
    }

    private void updateCompilerClassPath(URL[] urls) throws IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {

        JavaPlatform platform = (JavaPlatform) global.platform();
        MergedClassPath<AbstractFile> newClassPath = mergeUrlsIntoClassPath(platform, urls);

        Method[] methods = platform.getClass().getMethods();
        for (Method m : methods) {
            if (m.getName().endsWith("currentClassPath_$eq")) {
                m.invoke(platform, new Some(newClassPath));
                break;
            }
        }

        // NOTE: Must use reflection until this is exposed/fixed upstream in Scala
        List<String> classPaths = new LinkedList<>();
        for (URL url : urls) {
            classPaths.add(url.getPath());
        }

        // Reload all jars specified into our compiler
        global.invalidateClassPathEntries(scala.collection.JavaConversions.asScalaBuffer(classPaths)
                .toList());
    }

    // Until spark 1.1.x
    // check https://github.com/apache/spark/commit/191d7cf2a655d032f160b9fa181730364681d0e7
    private void updateRuntimeClassPath_1_x(URL[] urls) throws SecurityException,
            IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException {
        Method addURL;
        addURL = runtimeClassLoader.getClass().getDeclaredMethod("addURL", new Class[]{URL.class});
        addURL.setAccessible(true);
        for (URL url : urls) {
            addURL.invoke(runtimeClassLoader, url);
        }
    }

    private MergedClassPath<AbstractFile> mergeUrlsIntoClassPath(JavaPlatform platform, URL[] urls) {
        IndexedSeq<ClassPath<AbstractFile>> entries =
                ((MergedClassPath<AbstractFile>) platform.classPath()).entries();
        List<ClassPath<AbstractFile>> cp = new LinkedList<>();

        for (int i = 0; i < entries.size(); i++) {
            cp.add(entries.apply(i));
        }

        for (URL url : urls) {
            AbstractFile file;
            if ("file".equals(url.getProtocol())) {
                File f = new File(url.getPath());
                if (f.isDirectory()) {
                    file = AbstractFile.getDirectory(scala.reflect.io.File.jfile2path(f));
                } else {
                    file = AbstractFile.getFile(scala.reflect.io.File.jfile2path(f));
                }
            } else {
                file = AbstractFile.getURL(url);
            }

            ClassPath<AbstractFile> newcp = platform.classPath().context().newClassPath(file);

            // distinct
            if (cp.contains(newcp) == false) {
                cp.add(newcp);
            }
        }

        return new MergedClassPath(scala.collection.JavaConversions.asScalaBuffer(cp).toIndexedSeq(),
                platform.classPath().context());
    }

    public List<String> load(String artifact, Collection<String> excludes,
                             boolean addSparkContext) throws Exception {
        if (StringUtils.isBlank(artifact)) {
            // Should throw here
            throw new RuntimeException("Invalid artifact to load");
        }

        // <groupId>:<artifactId>[:<extension>[:<classifier>]]:<version>
        int numSplits = artifact.split(":").length;
        if (numSplits >= 3 && numSplits <= 6) {
            return super.load(artifact, excludes, addSparkContext);
        } else {
            loadFromFs(artifact, addSparkContext);
            LinkedList<String> libs = new LinkedList<>();
            libs.add(artifact);
            return libs;
        }
    }

    private void loadFromFs(String artifact, boolean addSparkContext) throws Exception {
        // check fileExclusions
        for (String exclusionPattern : fileExclusions) {
            if (artifact.contains(exclusionPattern)) return;
        }

        File jarFile = new File(artifact);

        global.new Run();

        URL[] newURL = new URL[]{jarFile.toURI().toURL()};
        updateRuntimeClassPath_1_x(newURL);
        updateCompilerClassPath(newURL);

        if (addSparkContext) {
            sc.addJar(jarFile.getAbsolutePath());
        }
    }
}
