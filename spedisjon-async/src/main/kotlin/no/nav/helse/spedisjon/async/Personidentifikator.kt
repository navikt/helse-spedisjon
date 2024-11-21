package no.nav.helse.spedisjon.async

import java.time.LocalDate

internal class Personidentifikator(value: String) {
    private val fødselsdato = try { LocalDate.of(
        value.substring(4, 6).toInt().toYear(value.substring(6, 9).toInt()),
        value.substring(2, 4).toInt(),
        value.toFørsteSiffer().toDay()
    )} catch (ex: Exception) { null }

    internal companion object {
        private fun String.toFørsteSiffer() = substring(0, 2).toInt()
        private fun Int.toDay() = if (this > 40) this - 40 else this
        private fun Int.toYear(individnummer: Int): Int {
            return this + when {
                this in (54..99) && individnummer in (500..749) -> 1800
                this in (0..99) && individnummer in (0..499) -> 1900
                this in (40..99) && individnummer in (900..999) -> 1900
                else -> 2000
            }
        }
        internal fun String.fødselsdatoOrNull() = Personidentifikator(this).fødselsdato
    }
}