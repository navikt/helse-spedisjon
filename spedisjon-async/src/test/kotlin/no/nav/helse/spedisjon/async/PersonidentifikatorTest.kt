package no.nav.helse.spedisjon.async

import no.nav.helse.spedisjon.async.Personidentifikator.Companion.fødselsdatoOrNull
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDate

internal class PersonidentifikatorTest {

    @Test
    fun `kan hente ut fødselsdato fra fødselsnummer`() {
        assertEquals(LocalDate.parse("1990-09-29"), "29099012345".fødselsdatoOrNull())
    }

    @Test
    fun `kan hente ut fødselsdato fra d-nummer`() {
        assertEquals(LocalDate.parse("1990-09-29"), "69099012345".fødselsdatoOrNull())
    }

    @Test
    fun `kan ikke hente ut fødselsdato fra andre identifikatorer`() {
        assertNull("79099012345".fødselsdatoOrNull())
        assertNull("9".fødselsdatoOrNull())
        assertNull("".fødselsdatoOrNull())
        assertNull("ABC".fødselsdatoOrNull())
    }
}