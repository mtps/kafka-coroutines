plugins {
	// Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
	id("org.jetbrains.kotlin.jvm") version "1.5.31"
}

allprojects {
	repositories {
		// Use Maven Central for resolving dependencies.
		mavenCentral()
	}
}

subprojects {
	apply {
		plugin("java")
		plugin("kotlin")
	}

	tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().configureEach {
		kotlinOptions {
			jvmTarget = "11"
		}
	}

	configure<JavaPluginExtension> {
		sourceCompatibility = JavaVersion.VERSION_11
		targetCompatibility = JavaVersion.VERSION_11
	}

	dependencies {
		implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
		implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

		// https://mvnrepository.com/artifact/org.jetbrains.kotlinx/kotlinx-coroutines-core
		implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2")

		// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
		implementation("org.apache.kafka:kafka-clients:3.0.0")

		// https://mvnrepository.com/artifact/io.github.microutils/kotlin-logging
		implementation("io.github.microutils:kotlin-logging:2.1.21")

		// https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
		implementation("ch.qos.logback:logback-classic:1.2.10")

		// https://mvnrepository.com/artifact/ch.qos.logback/logback-classic
		implementation("ch.qos.logback:logback-core:1.2.10")

	}

	testing {
		suites {
			// Configure the built-in test suite
			val test by getting(JvmTestSuite::class) {
				// Use Kotlin Test test framework
				useKotlinTest()
			}
		}
	}
}
