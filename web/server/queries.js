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
const qTwitchTotalHours = new PQ(sql.twitch_top_games.cumHours)
const qYoutubeTotalHours = new PQ(sql.youtube_stream.cumHours)

const timeout = cfg.pg_timeout
let queryCache = {}
queryCache.twitchGameCumVHLast30 = {}
queryCache.youtubeTotalVHLast30 = 0
queryCache.twitchTotalVHLast30 = 0

const DAYS_30 = 2592000
const DAY = 86400

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

async function twitchGameCumVHLast30 (days = 30, limit = 10, cached = false) {
  let results = {}
  if (days === 30) {
    results = queryCache.twitchGameCumVHLast30
  }
  if (!cached || isEmpty(results)) {
    try {
      let epoch = Math.floor(new Date() / 1000)
      let start = epoch - days * DAY
      let resp = await twitchGameCumVH(start, epoch, 1000)
      let data = []
      for (let i = 0; i < limit - 1; i++) {
        data.push([resp[i]['name'], parseInt(resp[i]['viewers'])])
      }
      let other = 0
      for (let i = limit - 1; i < resp.length; i++) {
        other += parseInt(resp[i]['viewers'])
      }
      data.push(['Other', other])
      if (days === 30) {
        queryCache.twitchGameCumVHLast30 = data
      }
    } catch (e) {
      console.log(e.message)
    }
  }
  console.log(queryCache.twitchGameCumVHLast30)
  return results
}

async function youtubeTotalVH (start, end) {
  const res = await db.any(qYoutubeTotalHours, [start, end])
  return res
}

async function youtubeTotalVHLast30 (refresh = false) {
  if (refresh || isEmpty(queryCache.youtubeTotalVHLast30)) {
    try {
      let epoch = Math.floor(new Date() / 1000)
      let start = epoch - DAYS_30
      let p = await youtubeTotalVH(start, epoch)
      queryCache.youtubeTotalVHLast30 = parseInt(p[0].sum)
    } catch (e) {
      console.log(e.message)
    }
  }
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
      let start = epoch - DAYS_30
      let p = await twitchTotalVH(start, epoch)
      queryCache.twitchTotalVHLast30 = parseInt(p[0].sum)
    } catch (e) {
      console.log(e.message)
    }
  }
  return queryCache.twitchTotalVHLast30
}

async function refreshQueryCache () {
  let queries = []
  queries.push(twitchGameCumVHLast30(30, 10, false))
  queries.push(twitchTotalVHLast30(true))
  queries.push(youtubeTotalVHLast30(true))

  await Promise.all(queries)
  console.log('Query Cache Updated')
}

function isEmpty (obj) {
  return Object.keys(obj).length === 0
}

function hoursToYears (hours) {
  return hours / 8760
}

module.exports = {
  cache: queryCache,
  twitchGameCumVH: twitchGameCumVH,
  twitchGameCumVHLast30: twitchGameCumVHLast30,
  refreshCache: refreshQueryCache,
  youtubeTotalVH: youtubeTotalVH,
  youtubeTotalVHLast30: youtubeTotalVHLast30,
  twitchTotalVH: twitchTotalVH,
  twitchTotalVHLast30: twitchTotalVHLast30
}
