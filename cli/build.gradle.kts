plugins {
    // Apply the application plugin for API and implementation separation.
    application
}

dependencies {
    implementation(project(":lib"))
}

application {
    mainClass.set("com.github.mtps.kafka.cli.MainKt")
}