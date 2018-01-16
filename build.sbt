name := "akka-streams"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.9"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "org.bouncycastle" % "bcprov-jdk15on" % "1.59"

libraryDependencies += "net.jpountz.lz4" % "lz4" % "1.3.0"

libraryDependencies += "org.tukaani" % "xz" % "1.8"

libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-smile" % "2.9.3"

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.2"

libraryDependencies += "commons-io" % "commons-io" % "2.6"

libraryDependencies += "org.iq80.snappy" % "snappy" % "0.4"

libraryDependencies += "io.reactivex.rxjava2" % "rxjava" % "2.1.8"

libraryDependencies += "com.github.davidmoten" % "rxjava2-extras" % "0.1.18"

enablePlugins(PackPlugin)
