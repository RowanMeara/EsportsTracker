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
const qEsportsGames = new PQ(sql.game.esportsGames)
const qEsportsGameHourly = new PQ(sql.twitch_stream.gameViewershipHourly)
const qYoutubeEsportsGameHourly = new PQ(sql.youtube_stream.gameViewershipHourly)
const qCombinedGameHourly = new PQ(sql.youtube_stream.combinedGameViewershipHourly)
const qGameidToName = new PQ(sql.game.gameidToName)
const qOrgCumMarketshare = new PQ(sql.tournament_organizer.cumMarketshare)
const qEsportsHoursByGame = new PQ(sql.youtube_stream.esportsHoursByGame)
const timeout = cfg.pg_timeout
var cacheEsportsGames = {}

/*
 * Gets the name and game_ids of current esports titles.  Results are in descending order
 * by popularity.
 *
 * @return {Object} raw query result
 */
async function esportsGames () {
  try {
    let res = await db.any(qEsportsGames)
    return res
  } catch (e) {
    console.log(e.message)
  }
}

/*
 * Return hourly English language viewership data for the given game.
 *
 * @param {Number} gameID - Twitch's game id number
 * @param {Number} start - unix epoch
 * @param {Number} end - unix epoch
 */
async function esportsGameHourly (gameID, start, end) {
  let res = await db.any(qEsportsGameHourly, [gameID, start, end])
  return res
}

/*
 * Return hourly English language viewership data for the given game.
 *
 * @param {Number} gameID - Twitch's game id number
 * @param {Number} start - unix epoch
 * @param {Number} end - unix epoch
 */
async function youtubeEsportsGameHourly (gameID, start, end) {
  let res = await db.any(qYoutubeEsportsGameHourly, [gameID, start, end])
  return res
}

/*
 * Return hourly English language viewership data for the given game.
 *
 * @param {Number} gameID - Twitch's game id number
 * @param {Number} start - unix epoch
 * @param {Number} end - unix epoch
 */
async function combinedGameVHHourly (gameID, start, end) {
  let res = await db.any(qCombinedGameHourly, [gameID, start, end])
  return res
}

/*
 *
 * @param {Number} start - unix epoch
 * @param {Number} end - unix epoch
 * @param {Number} limit - number of games to return data for
 * @return {Object} raw query result
 */
async function twitchGameCumVH (start, end, limit) {
  let res = await db.any(topGamesTotalHours, [start, end, limit])
  return res
}

async function youtubeTotalVH (start, end) {
  const res = await db.any(qYoutubeTotalHours, [start, end])
  return res
}

async function twitchTotalVH (start, end) {
  const res = await db.any(qTwitchTotalHours, [start, end])
  return res
}

async function refreshESG () {
  let res = await esportsGames()
  cacheEsportsGames = res
}

function getEsportsGames () {
  return cacheEsportsGames
}

async function gameidToName (gid) {
  const res = await db.any(qGameidToName, [gid])
  return res
}

async function esportsHoursByGame(start, end, numGames) {
  const res = await db.any(qEsportsHoursByGame, [start, end, numGames])
  return res
}
/*
 * Returns the total number of viewer hours watched between start and end
 * for each tournament organizer.
 *
 * @param {Number} start - unix epoch
 * @param {Number} end - unix epoch
 * @return {Object} raw query result
 */
async function orgMarketshareCum(start, end) {
  let res = await db.any(qOrgCumMarketshare, [start, end])
  return res
}

module.exports = {
  twitchGamesCumVH: twitchGameCumVH,
  esportsGameHourly: esportsGameHourly,
  esportsGames: esportsGames,
  youtubeTotalVH: youtubeTotalVH,
  twitchTotalVH: twitchTotalVH,
  youtubeEsportsGameHourly: youtubeEsportsGameHourly,
  combinedGameVHHourly: combinedGameVHHourly,
  gameidToName: gameidToName,
  orgMarketshareCum: orgMarketshareCum,
  esportsHoursByGame: esportsHoursByGame,
  cache: {
    esportsGames: getEsportsGames,
    refreshESG: refreshESG
  }
}
