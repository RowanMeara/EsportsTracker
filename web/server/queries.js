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

const timeout = cfg.pg_timeout
let queryCache = {}
queryCache.twitchGameCumVHLast30 = {}

/*
 *
 * @param {Number} start - unix epoch
 * @param {Number} end - unix epoch
 * @param {Number} limit - number of games to return data for
 * @return {Object} raw query result
 */
async function twitchGameCumVH (start, end, limit) {
  const res = await db.any(topGamesTotalHours, [start, end, limit])
  return res
}

async function twitchGameCumVHLast30 (refresh = false) {
  if (refresh || isEmpty(queryCache.twitchGameCumVHLast30)) {
    try {
      let epoch = Math.floor(new Date() / 1000)
      let start = epoch - 2592000
      queryCache.twitchGameCumVHLast30 = await twitchGameCumVH(start, epoch, 10)
    } catch (e) {
      console.log(e.message)
    }
  }
  return queryCache.twitchGameCumVHLast30
}

async function refreshQueryCache () {
  let queries = []
  queries.push(twitchGameCumVHLast30(true))

  await Promise.all(queries)
  console.log('Query Cache Updated')
}

function isEmpty (obj) {
  return Object.keys(obj).length === 0
}

module.exports = {
  twitchGameCumVH: twitchGameCumVH,
  twitchGameCumVHLast30: twitchGameCumVHLast30,
  refreshCache: refreshQueryCache
}
