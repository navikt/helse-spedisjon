package no.nav.helse.spedisjon.async

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BerikelseTest {

    @Test
    fun `skal putte på fødselsdato og aktørId og dødsdato`() {
        val json = Melding.NySøknad(UUID.randomUUID(), Meldingsdetaljer(
            type = "ny_søknad",
            fnr = "fnr",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf("unik key"),
            jsonBody = "{}"
        ))
        val beriketJson = Berikelse(
            fødselsdato = LocalDate.of(2012, 12, 31),
            dødsdato = LocalDate.of(2023, 1, 2),
            aktørId = "12345",
            historiskeFolkeregisteridenter = emptyList()
        ).berik(json).toJsonNode()
        assertEquals("2012-12-31", beriketJson.path("fødselsdato").asText())
        assertEquals("2023-01-02", beriketJson.path("dødsdato").asText())
        assertEquals("12345", beriketJson.path("aktorId").asText())
        assertEquals("ny_søknad", beriketJson.path("@event_name").asText())
    }

    private fun BeriketMelding.toJsonNode() =
        jacksonObjectMapper().readTree(json)
}
