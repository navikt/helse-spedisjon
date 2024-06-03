package no.nav.helse.spedisjon

import com.github.navikt.tbd_libs.test_support.TestDataSource
import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
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
        val packet = JsonMessage(
            """
                {
                    "arkivreferanse": "1",
                    "arbeidstakerFnr": "123",
                    "mottattDato": "${LocalDateTime.now()}"
                }
            """, MessageProblems("")
        ).apply {
            requireKey("arkivreferanse", "arbeidstakerFnr", "mottattDato")
        }

        assertTrue(meldingDao.leggInn(Melding.Inntektsmelding(packet)))
        assertFalse(meldingDao.leggInn(Melding.Inntektsmelding(packet)))
    }

    @Test
    fun `duplikat sendt søknad til nav slipper ikke igjennom`() {
        val packet = JsonMessage(
            """
                {
                    "id": "${UUID.randomUUID()}",
                    "fnr": "123",
                    "sendtNav": "${LocalDateTime.now()}",
                    "status": "SENDT"
                }
            """, MessageProblems("")
        ).apply {
            requireKey("id", "fnr", "sendtNav", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadNav(packet)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadNav(packet)))
    }

    @Test
    fun `duplikat sendt søknad til arbeidsgiver slipper ikke igjennom`() {
        val packet = JsonMessage(
            """
                {
                    "id": "${UUID.randomUUID()}",
                    "fnr": "123",
                    "sendtArbeidsgiver": "${LocalDateTime.now()}",
                    "status": "SENDT"
                }
            """, MessageProblems("")
        ).apply {
            requireKey("id", "fnr", "sendtArbeidsgiver", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(packet)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(packet)))
    }

    @Test
    fun `duplikat sendt søknad til arbeidsgiver (ettersending) slipper ikke igjennom`() {
        val id = UUID.randomUUID()
        val førsteInnsending = JsonMessage(
            """
                {
                    "id": "$id",
                    "fnr": "123",
                    "sendtArbeidsgiver": "${LocalDateTime.now()}",
                    "sendtNav": null,
                    "status": "SENDT"
                }
            """, MessageProblems("")
        ).apply {
            requireKey("id", "fnr", "sendtArbeidsgiver", "status")
        }
        val andreInnsending = JsonMessage(
            """
                {
                    "id": "$id",
                    "fnr": "123",
                    "sendtArbeidsgiver": "${LocalDateTime.now()}",
                    "sendtNav": "${LocalDateTime.now().minusDays(1)}",
                    "status": "SENDT"
                }
            """, MessageProblems("")
        ).apply {
            requireKey("id", "fnr", "sendtArbeidsgiver", "sendtNav", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.SendtSøknadArbeidsgiver(førsteInnsending)))
        assertFalse(meldingDao.leggInn(Melding.SendtSøknadNav(andreInnsending)))
    }

    @Test
    fun `duplikat ny søknad slipper ikke igjennom`() {
        val packet = JsonMessage(
            """
                {
                    "id": "${UUID.randomUUID()}",
                    "fnr": "123",
                    "opprettet": "${LocalDateTime.now()}",
                    "status": "NY"
                }
            """, MessageProblems("")
        ).apply {
            requireKey("id", "fnr", "opprettet", "status")
        }

        assertTrue(meldingDao.leggInn(Melding.NySøknad(packet)))
        assertFalse(meldingDao.leggInn(Melding.NySøknad(packet)))
    }
}
