package no.nav.helse.spedisjon.api

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.github.navikt.tbd_libs.naisful.FeilResponse
import io.ktor.http.*
import io.ktor.server.plugins.NotFoundException
import io.ktor.server.plugins.callid.callId
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.util.getOrFail
import no.nav.helse.spedisjon.api.tjeneste.Meldingtjeneste
import java.net.URI
import java.time.LocalDateTime
import java.util.*

internal fun Route.api(meldingtjeneste: Meldingtjeneste) {
    route("/api/melding") {
        /*
            sette inn ny melding i db

            409 Conflict hvis meldingen allerede finnes
            200 OK hvis OK
         */
        post {
            val request = call.receive<NyMeldingRequest>()
            val dto = no.nav.helse.spedisjon.api.tjeneste.NyMeldingRequest(
                type = request.type,
                fnr = request.fnr,
                eksternDokumentId = request.eksternDokumentId,
                rapportertDato = request.rapportertDato,
                duplikatkontroll = request.duplikatkontroll,
                jsonBody = request.jsonBody
            )
            val response = meldingtjeneste.nyMelding(dto)

            call.respond(if (response.lagtInnNå) HttpStatusCode.OK else HttpStatusCode.Conflict, NyMeldingResponse(
                internDokumentId = response.internDokumentId
            ))
        }
        /* hente melding */
        get("/{internDokumentId}") {
            val internDokumentId = UUID.fromString(call.parameters.getOrFail("internDokumentId"))
            val response = meldingtjeneste.hentMeldinger(listOf(internDokumentId))

            if (response.meldinger.size != 1) throw NotFoundException()
            val melding = response.meldinger.single()
            call.respond(HttpStatusCode.OK, MeldingResponse(
                type = melding.type,
                fnr = melding.fnr,
                internDokumentId = melding.internDokumentId,
                eksternDokumentId = melding.eksternDokumentId,
                rapportertDato = melding.rapportertDato,
                duplikatkontroll = melding.duplikatkontroll,
                jsonBody = melding.jsonBody
            ))
        }
    }
    /* hente meldinger (flertall) */
    get("/api/meldinger") {
        val request = call.receive<HentMeldingerRequest>()
        val response = meldingtjeneste.hentMeldinger(request.internDokumentIder)

        val meldinger = response.meldinger.map { melding ->
            MeldingResponse(
                type = melding.type,
                fnr = melding.fnr,
                internDokumentId = melding.internDokumentId,
                eksternDokumentId = melding.eksternDokumentId,
                rapportertDato = melding.rapportertDato,
                duplikatkontroll = melding.duplikatkontroll,
                jsonBody = melding.jsonBody
            )
        }
        call.respond(HttpStatusCode.OK, HentMeldingerResponse(meldinger = meldinger))
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class NyMeldingRequest(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime,
    val duplikatkontroll: String,
    val jsonBody: String
)

data class NyMeldingResponse(
    val internDokumentId: UUID
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class HentMeldingerRequest(
    val internDokumentIder: List<UUID>
)

data class HentMeldingerResponse(
    val meldinger: List<MeldingResponse>
)

data class MeldingResponse(
    val type: String,
    val fnr: String,
    val internDokumentId: UUID,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime,
    val duplikatkontroll: String,
    val jsonBody: String
)