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
const qYoutubeTotalHours = new PQ(sql.twitch_top_games.cumHours)
const qTwitchTotalHours = new PQ(sql.youtube_stream.cumHours)

const timeout = cfg.pg_timeout
let queryCache = {}
queryCache.twitchGameCumVHLast30 = {}
queryCache.youtubeTotalVHLast30 = {}
queryCache.twitchTotalVHLast30 = {}

const days30 = 2592000

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
      let start = epoch - days30
      queryCache.twitchGameCumVHLast30 = await twitchGameCumVH(start, epoch, 10)
    } catch (e) {
      console.log(e.message)
    }
  }
  return queryCache.twitchGameCumVHLast30
}

async function youtubeTotalVH (start, end) {
  const res = await db.any(qYoutubeTotalHours, [start, end])
  return res
}

async function youtubeTotalVHLast30 (refresh = false) {
  if (refresh || isEmpty(queryCache.youtubeTotalVHLast30)) {
    try {
      let epoch = Math.floor(new Date() / 1000)
      let start = epoch - days30
      queryCache.youtubeTotalVHLast30 = await youtubeTotalVH(start, epoch)
    } catch (e) {
      console.log(e.message)
    }
  }
  console.log(queryCache.youtubeTotalVHLast30)
  return queryCache.youtubeTotalVHLast30
}

async function twitchTotalVH (start, end) {
  const res = await db.any(qTwitchTotalHours, [start, end])
  return res
}

async function twitchTotalVHLast30 (refresh = false) {
  if (refresh || isEmpty(queryCache.twitchTotalVHLast30)) {
    try {
      let epoch = Math.floor(new Date() / 1000)
      let start = epoch - days30
      queryCache.twitchTotalVHLast30 = await twitchTotalVH(start, epoch)
    } catch (e) {
      console.log(e.message)
    }
  }
  console.log(queryCache.twitchTotalVHLast30)
  return queryCache.twitchTotalVHLast30
}

async function refreshQueryCache () {
  let queries = []
  queries.push(twitchGameCumVHLast30(true))
  queries.push(twitchTotalVHLast30(true))
  queries.push(youtubeTotalVHLast30(true))

  await Promise.all(queries)
  console.log('Query Cache Updated')
}

function isEmpty (obj) {
  return Object.keys(obj).length === 0
}

module.exports = {
  twitchGameCumVH: twitchGameCumVH,
  twitchGameCumVHLast30: twitchGameCumVHLast30,
  refreshCache: refreshQueryCache,
  youtubeTotalVH: youtubeTotalVH,
  youtubeTotalVHLast30: youtubeTotalVHLast30,
  twitchTotalVH: twitchTotalVH,
  twitchTotalVHLast30: twitchTotalVHLast30
}
