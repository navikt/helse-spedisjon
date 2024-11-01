package no.nav.helse.spedisjon

import kotliquery.Row
import kotliquery.Session
import kotliquery.queryOf
import kotliquery.sessionOf
import javax.sql.DataSource

abstract class AbstractDao(protected val dataSource: DataSource) {
    protected fun String.update(argMap: Map<String, Any?>) =
        sessionOf(dataSource).use { update(it, argMap) }

    protected fun String.update(session: Session, argMap: Map<String, Any?>) =
       session.run(queryOf(this, argMap).asUpdate)

    protected fun <A> String.listQuery(argMap: Map<String, Any?>, resultMapping: (Row) -> A) =
        sessionOf(dataSource).use { listQuery(it, argMap, resultMapping) }

    protected fun <A> String.listQuery(session: Session, argMap: Map<String, Any?>, resultMapping: (Row) -> A) =
        session.run(queryOf(this, argMap).map { row -> resultMapping(row) }.asList)

    protected fun <A> String.singleQuery(argMap: Map<String, Any?>, resultMapping: (Row) -> A) =
        sessionOf(dataSource).use { singleQuery(it, argMap, resultMapping) }

    protected fun <A> String.singleQuery(session: Session, argMap: Map<String, Any?>, resultMapping: (Row) -> A) =
        session.run(queryOf(this, argMap).map { row -> resultMapping(row) }.asSingle)
}