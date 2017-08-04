const promise = require('bluebird')
const PQ = require('pg-promise').ParameterizedQuery

const cfg = require('../config')
const secrets = require('../secrets')
const sql = require('../sql/sql')

const cn = {
  host: cfg.pg_host,
  port: cfg.pg_port,
  database: cfg.pg_db,
  user: secrets.pg_user,
  password: secrets.pg_passwd
}

const pgp = require('pg-promise')({
  promiseLib: promise
})

const db = pgp(cn)
const topGamesTotalHours = new PQ(sql.twitch_top_games.totalHours)

async function getTwitchGamesTotalViewerHours (start, end, limit) {
  const res = await db.any(topGamesTotalHours, [start, end, limit])
  return res
}

module.exports = {
  getTwitchGamesTotalViewerHours: getTwitchGamesTotalViewerHours
}
