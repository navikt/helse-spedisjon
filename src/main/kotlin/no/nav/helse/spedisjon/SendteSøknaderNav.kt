package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class SendteSøknaderNav(
    rapidsConnection: RapidsConnection,
    private val meldingDao: MeldingDao,
    private val problemsCollector: ProblemsCollector,
    private val aktørregisteretClient: AktørregisteretClient
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate { it.requireKey("aktorId", "arbeidsgiver.orgnummer", "opprettet", "soknadsperioder") }
            validate { it.requireValue("status", "SENDT") }
            validate { it.requireKey("id", "sendtNav", "fom", "tom", "egenmeldinger", "fravar") }
            validate {it.interestedIn("fnr")}
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        packet.putIfAbsent("fnr") { aktørregisteretClient.hentFødselsnummer(packet["aktorId"].asText()) }
        val sendtSøknad = Melding.SendtSøknadNav(packet)
        if (!meldingDao.leggInn(sendtSøknad)) return
        context.send(sendtSøknad.fødselsnummer(), sendtSøknad.json())
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        problemsCollector.add("Sendt søknad Nav", problems)
    }
}
