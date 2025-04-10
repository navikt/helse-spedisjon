package no.nav.helse.opprydding

import com.github.navikt.tbd_libs.rapids_and_rivers.JsonMessage
import com.github.navikt.tbd_libs.rapids_and_rivers.test_support.TestRapid
import kotliquery.queryOf
import kotliquery.sessionOf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

internal class AppTest: DataSourceBuilderTest() {
    private lateinit var testRapid: TestRapid
    private lateinit var personRepository: PersonRepository

    @BeforeEach
    fun beforeEach() {
        testRapid = TestRapid()
        personRepository = PersonRepository(testDataSource.ds)
        SlettPersonRiver(testRapid, personRepository)
    }

    @Test
    fun `slettemelding medfører at person slettes fra databasen`() {
        opprettDokumenter("123")
        assertEquals(1, finnMeldinger("123"))
        assertEquals(1, finnBerikelser("123"))
        assertEquals(1, finnInntektsmeldinger("123"))

        testRapid.sendTestMessage(slettemelding("123"))

        assertEquals(0, finnMeldinger("123"))
        assertEquals(0, finnBerikelser("123"))
        assertEquals(0, finnInntektsmeldinger("123"))
    }

    @Test
    fun `sletter kun aktuelt fnr`() {
        opprettDokumenter("123")
        opprettDokumenter("1234")
        testRapid.sendTestMessage(slettemelding("123"))
        assertEquals(1, finnMeldinger("1234"))
        assertEquals(1, finnBerikelser("1234"))
        assertEquals(1, finnInntektsmeldinger("1234"))

        assertEquals(0, finnMeldinger("123"))
        assertEquals(0, finnBerikelser("123"))
        assertEquals(0, finnInntektsmeldinger("123"))
    }

    private fun slettemelding(fødselsnummer: String) = JsonMessage.newMessage("slett_person", mapOf("fødselsnummer" to fødselsnummer)).toJson()

    private fun opprettDokumenter(fødselsnummer: String) {
        opprettMelding(fødselsnummer)
        opprettBerikelse(fødselsnummer)
        opprettInntektsmelding(fødselsnummer)

        assertEquals(1, finnMeldinger(fødselsnummer))
        assertEquals(1, finnBerikelser(fødselsnummer))
        assertEquals(1, finnInntektsmeldinger(fødselsnummer))
    }
    private fun finnMeldinger(fødselsnummer: String): Int {
        return sessionOf(testDataSource.ds).use { session ->
            session.run(queryOf("SELECT COUNT(1) FROM melding WHERE fnr = ?", fødselsnummer).map { it.int(1) }.asSingle)
        } ?: 0
    }

    private fun finnBerikelser(fødselsnummer: String): Int {
        return sessionOf(testDataSource.ds).use { session ->
            session.run(queryOf("SELECT COUNT(1) FROM berikelse WHERE fnr = ?", fødselsnummer).map { it.int(1) }.asSingle)
        } ?: 0
    }


    private fun finnInntektsmeldinger(fødselsnummer: String): Int {
        return sessionOf(testDataSource.ds).use { session ->
            session.run(queryOf("SELECT COUNT(1) FROM inntektsmelding WHERE fnr = ?", fødselsnummer).map { it.int(1) }.asSingle)
        } ?: 0
    }

    private fun opprettMelding(fødselsnummer: String) {
        val query = """INSERT INTO melding (fnr) VALUES (?)"""
        sessionOf(testDataSource.ds).use { it.run(queryOf(query, fødselsnummer).asUpdate)}
    }

    private fun opprettBerikelse(fødselsnummer: String) {
        val query = "INSERT INTO berikelse (fnr) VALUES (?)"
        sessionOf(testDataSource.ds).use { it.run(queryOf(query, fødselsnummer).asUpdate)}
    }

    private fun opprettInntektsmelding(fødselsnummer: String) {
        val query = """INSERT INTO inntektsmelding (fnr) VALUES (?)"""
        sessionOf(testDataSource.ds).use { it.run(queryOf(query, fødselsnummer).asUpdate)}
    }
}
