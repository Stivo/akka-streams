name := "akka-streams"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.8"

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "org.bouncycastle" % "bcprov-jdk15on" % "1.59"

libraryDependencies += "net.jpountz.lz4" % "lz4" % "1.3.0"

libraryDependencies += "org.tukaani" % "xz" % "1.8"


enablePlugins(PackPlugin)
