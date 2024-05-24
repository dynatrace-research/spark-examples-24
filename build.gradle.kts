plugins {
    id("java")
}

java {
    setSourceCompatibility("11")
    setTargetCompatibility("11")
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.12.9")
    implementation("org.apache.spark:spark-core_2.12:3.5.1")
    implementation("org.apache.spark:spark-sql_2.12:3.5.1")
    implementation("org.apache.spark:spark-streaming_2.12:3.5.1")
}

tasks.test {
    useJUnitPlatform()
}