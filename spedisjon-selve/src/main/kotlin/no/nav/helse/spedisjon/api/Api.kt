package no.nav.helse.spedisjon.api

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.ktor.http.*
import io.ktor.server.plugins.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.util.*
import io.ktor.utils.io.ClosedWriteChannelException
import java.util.*
import no.nav.helse.spedisjon.api.tjeneste.ApiMeldingtjeneste
import no.nav.sykepenger.libs.logging.navngittLogger

private val logger = navngittLogger("no.nav.helse.spedisjon.api.Api")

internal fun Route.api(meldingtjeneste: ApiMeldingtjeneste) {
    route("/api/melding") {
        /*
            Lagrer dokumenter i databasem.

            Returnerer:
              - 201 CREATED hvis meldingen ble lagret
              - 200 OK hvis meldingen allerede var lagret
         */
        post {
            val request = call.receive<NyMeldingRequest>()
            val dto =
                no.nav.helse.spedisjon.api.tjeneste.NyMeldingRequest(
                    type = request.type,
                    fnr = request.fnr,
                    eksternDokumentId = request.eksternDokumentId,
                    duplikatkontroll = request.duplikatkontroll,
                    jsonBody = request.jsonBody
                )
            val (responseStatus, internDokumentId) =
                meldingtjeneste.lagreNyMelding(dto).run {
                    (if (bleLagtInnNå) HttpStatusCode.Created else HttpStatusCode.OK) to internDokumentId
                }

            logger.info("Prøver å sende svar på POST /api/melding med status ${responseStatus.value}")
            try {
                call.respond(responseStatus, NyMeldingResponse(internDokumentId = internDokumentId))
            } catch (err: ClosedWriteChannelException) {
                logger.warn(
                    "Skrivekanalen var eller ble lukket under skriving av svar på POST til /api/melding",
                    err,
                    "status" to responseStatus.value.toString()
                )
                throw err
            }
        }

        // hente melding
        get("/{internDokumentId}") {
            val internDokumentId = UUID.fromString(call.parameters.getOrFail("internDokumentId"))
            val response = meldingtjeneste.hentMeldinger(listOf(internDokumentId))

            if (response.meldinger.size != 1) throw NotFoundException()
            val melding = response.meldinger.single()
            call.respond(
                HttpStatusCode.OK,
                MeldingResponse(
                    type = melding.type,
                    fnr = melding.fnr,
                    internDokumentId = melding.internDokumentId,
                    eksternDokumentId = melding.eksternDokumentId,
                    duplikatkontroll = melding.duplikatkontroll,
                    jsonBody = melding.jsonBody
                )
            )
        }
    }
    // hente meldinger (flertall)
    get("/api/meldinger") {
        val request = call.receive<HentMeldingerRequest>()
        val response = meldingtjeneste.hentMeldinger(request.internDokumentIder)

        val meldinger =
            response.meldinger.map { melding ->
                MeldingResponse(
                    type = melding.type,
                    fnr = melding.fnr,
                    internDokumentId = melding.internDokumentId,
                    eksternDokumentId = melding.eksternDokumentId,
                    duplikatkontroll = melding.duplikatkontroll,
                    jsonBody = melding.jsonBody
                )
            }
        call.respond(HttpStatusCode.OK, HentMeldingerResponse(meldinger = meldinger))
    }
}

data class SpedisjonFeilresponse(
    val type: String,
    val title: String = HttpStatusCode.ServiceUnavailable.description,
    val status: Int = HttpStatusCode.ServiceUnavailable.value,
    val detail: String?,
    val instance: String = "/api/melding",
    val callId: String? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class NyMeldingRequest(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
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
    val duplikatkontroll: String,
    val jsonBody: String
)
