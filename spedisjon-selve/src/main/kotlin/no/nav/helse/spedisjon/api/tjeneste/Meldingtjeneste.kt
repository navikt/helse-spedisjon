package no.nav.helse.spedisjon.api.tjeneste

import no.nav.helse.spedisjon.api.MeldingDao
import no.nav.helse.spedisjon.api.MeldingDto
import no.nav.helse.spedisjon.api.NyMeldingDto
import java.time.LocalDateTime
import java.util.*

interface Meldingtjeneste {
    fun nyMelding(meldingsdetaljer: NyMeldingRequest): NyMeldingResponse
    fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse
}

internal class LokalMeldingtjeneste(private val dao: MeldingDao) : Meldingtjeneste {
    override fun nyMelding(request: NyMeldingRequest): NyMeldingResponse {
        val dto = NyMeldingDto(
            type = request.type,
            fnr = request.fnr,
            eksternDokumentId = request.eksternDokumentId,
            rapportertDato = request.rapportertDato,
            duplikatkontroll = request.duplikatkontroll,
            jsonBody = request.jsonBody
        )
        val result = dao.leggInn(dto)
        return NyMeldingResponse(
            internDokumentId = result.internId,
            lagtInnNå = result.utfall == MeldingDao.Resultat.Utfall.SATT_INN_NY
        )
    }

    override fun hentMeldinger(interneDokumentIder: List<UUID>): HentMeldingerResponse {
        return HentMeldingerResponse(
            meldinger = dao.hentMeldinger(interneDokumentIder)
        )
    }
}

data class NyMeldingResponse(val internDokumentId: UUID, val lagtInnNå: Boolean)

data class NyMeldingRequest(
    val type: String,
    val fnr: String,
    val eksternDokumentId: UUID,
    val rapportertDato: LocalDateTime,
    val duplikatkontroll: String,
    val jsonBody: String
)

class HentMeldingerResponse(
    val meldinger: List<MeldingDto>
)