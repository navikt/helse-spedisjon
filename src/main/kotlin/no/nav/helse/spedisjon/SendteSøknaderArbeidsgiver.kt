package no.nav.helse.spedisjon

import no.nav.helse.rapids_rivers.JsonMessage
import no.nav.helse.rapids_rivers.MessageProblems
import no.nav.helse.rapids_rivers.RapidsConnection
import no.nav.helse.rapids_rivers.River

internal class SendteSøknaderArbeidsgiver(
    rapidsConnection: RapidsConnection,
    private val meldingDao: MeldingDao,
    private val problemsCollector: ProblemsCollector
) : River.PacketListener {

    init {
        River(rapidsConnection).apply {
            validate { it.forbid("@event_name") }
            validate { it.requireKey("fnr", "aktorId", "arbeidsgiver.orgnummer", "opprettet", "soknadsperioder") }
            validate { it.requireValue("status", "SENDT") }
            validate { it.requireKey("id", "sendtArbeidsgiver", "fom", "tom", "egenmeldinger", "fravar") }
            validate { it.forbid("sendtNav") }
        }.register(this)
    }

    override fun onPacket(packet: JsonMessage, context: RapidsConnection.MessageContext) {
        val sendtSøknad = Melding.SendtSøknadArbeidsgiver(packet)
        if (!meldingDao.leggInn(sendtSøknad)) return
        context.send(sendtSøknad.fødselsnummer(), sendtSøknad.json())
    }

    override fun onError(problems: MessageProblems, context: RapidsConnection.MessageContext) {
        problemsCollector.add("Sendt søknad arbeidsgiver", problems)
    }
}
