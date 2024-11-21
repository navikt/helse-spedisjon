package no.nav.helse.spedisjon

import no.nav.helse.spedisjon.Meldingsdetaljer.Companion.sha512
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

class MeldingsdetaljerTest {

    @Test
    fun `duplikatkontroll`() {
        val detaljer = Meldingsdetaljer(
            type = "ny_søknad",
            fnr = "123",
            eksternDokumentId = UUID.randomUUID(),
            rapportertDato = LocalDateTime.now(),
            duplikatnøkkel = listOf("bit_a", "bit_b"),
            jsonBody = "{}"
        )
        assertEquals("bit_abit_b".sha512(), detaljer.duplikatkontroll)
        assertEquals("56cf3c91019a0d437e2ff6f735ab95ab6239d93a11a0841589b981ce337a4459dcece37b6b06baa2dbc9884354793db462af5c4adf4cc97749fe16b50ecf5ae5", detaljer.duplikatkontroll)
    }
}