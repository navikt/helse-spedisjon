package no.nav.helse.spedisjon.api

import com.github.navikt.tbd_libs.test_support.CleanupStrategy
import com.github.navikt.tbd_libs.test_support.DatabaseContainers

val databaseContainer = DatabaseContainers.container("spedisjon-selve", CleanupStrategy.tables("melding"))