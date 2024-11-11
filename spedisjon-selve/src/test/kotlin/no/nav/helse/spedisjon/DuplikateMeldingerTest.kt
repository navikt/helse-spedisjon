package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers_api.MessageProblems
import com.github.navikt.tbd_libs.test_support.TestDataSource
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

internal class DuplikateMeldingerTest {

    private lateinit var meldingDao: MeldingDao
    private lateinit var testDataSource: TestDataSource

    @BeforeEach
    fun setup() {
        testDataSource = databaseContainer.nyTilkobling()
        meldingDao = MeldingDao(testDataSource.ds)
    }

    @AfterEach
    fun tearDown() {
        databaseContainer.droppTilkobling(testDataSource)
    }

    @Test
    fun `duplikat inntektsmelding slipper ikke igjennom`() {
        val packet = lagMelding(
            """
                {
                    "arkivreferanse": "1",
                    "arbeidstakerFnr": "123",
                    "mottattDato": "${LocalDateTime.now()}"
                }
            """).apply {
            requireKey("arkivreferanse", "arbeidstakerFnr", "mottattDato")
        }

        assertTrue(meldingDao.leggInn(Melding.Inntektsmelding(packet)))
        assertFalse(meldingDao.leggInn(Melding.Inntektsmelding(packet)))
    }

    @Test
    fun `duplikat sendt søknad til nav slipper ikke igjennom`() {
        val packet = lagMelding(
            """
                {
                    "id": "${UUID.randomUUID()}",
                    "fnr": "123",
                    "sendtNav": "${LocalDateTime.now()}",
                    "status": "SENDT"
                }
            """
        ).apply {
            requireKey("id", "fnr", "sendtNav", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadNav(packet)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadNav(packet)))
    }

    @Test
    fun `duplikat sendt søknad til arbeidsgiver slipper ikke igjennom`() {
        val packet = lagMelding(
            """
                {
                    "id": "${UUID.randomUUID()}",
                    "fnr": "123",
                    "sendtArbeidsgiver": "${LocalDateTime.now()}",
                    "status": "SENDT"
                }
            """
        ).apply {
            requireKey("id", "fnr", "sendtArbeidsgiver", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(packet)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(packet)))
    }

    @Test
    fun `duplikat sendt søknad til arbeidsgiver (ettersending) slipper ikke igjennom`() {
        val id = UUID.randomUUID()
        val førsteInnsending = lagMelding(
            """
                {
                    "id": "$id",
                    "fnr": "123",
                    "sendtArbeidsgiver": "${LocalDateTime.now()}",
                    "sendtNav": null,
                    "status": "SENDT"
                }
            """
        ).apply {
            requireKey("id", "fnr", "sendtArbeidsgiver", "status")
        }
        val andreInnsending = lagMelding(
            """
                {
                    "id": "$id",
                    "fnr": "123",
                    "sendtArbeidsgiver": "${LocalDateTime.now()}",
                    "sendtNav": "${LocalDateTime.now().minusDays(1)}",
                    "status": "SENDT"
                }
            """
        ).apply {
            requireKey("id", "fnr", "sendtArbeidsgiver", "sendtNav", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(førsteInnsending)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadNav(andreInnsending)))
    }

    @Test
    fun `duplikat ny søknad slipper ikke igjennom`() {
        val packet = lagMelding(
            """
                {
                    "id": "${UUID.randomUUID()}",
                    "fnr": "123",
                    "opprettet": "${LocalDateTime.now()}",
                    "status": "NY"
                }
            """).apply {
            requireKey("id", "fnr", "opprettet", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.NySøknad(packet)))
        assertFalse(meldingDao.leggInn(Melding.NySøknad(packet)))
    }

    private val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    private fun lagMelding(json: String) = JsonMessage(json, MessageProblems(json), registry)
}
