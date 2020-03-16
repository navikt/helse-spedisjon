package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class NyeSøknader(
    rapidsConnection: RapidsConnection,
    private val meldingDao: MeldingDao
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate { it.requireKey("fnr", "aktorId", "arbeidsgiver.orgnummer", "opprettet", "soknadsperioder") }
            validate { it.requireValue("status", "NY") }
            validate { it.requireKey("id", "sykmeldingId", "fom", "tom") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val nySøknad = Melding.NySøknad(packet)
        if (!meldingDao.leggInn(nySøknad)) return
        context.send(nySøknad.fødselsnummer(), nySøknad.json())
    }
}
