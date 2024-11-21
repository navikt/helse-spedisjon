rootProject.name = "spedisjon"

pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

include("spedisjon-opprydding-dev", "spedisjon-async", "spedisjon-selve", "spedisjon-migrering")
