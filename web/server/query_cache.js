const queries = require('./queries')

const DAYS_30 = 2592000
const DAY = 86400

let cache = {
  refresh: refreshQueryCache,
  esportsGames: {},
  youtubeTotalVH: {
    7: {},
    30: {}
  },
  twitchTotalVH: {
    7: {},
    30: {}
  },
  twitchGameCumVH: {
    7: {},
    30: {}
  }
}

function isEmpty (obj) {
  return Object.keys(obj).length === 0
}

async function esportsGames () {
  let r = await queries.esportsGames()
  cache.esportsGames = r
}

async function refreshQueryCache () {
  let t1 = Date.now()
  if (isEmpty(cache.esportsGames)) {
    await esportsGames()
  }

  let queries = []
  queries.push(twitchGamesCumVH(7))
  queries.push(twitchGamesCumVH(30))
  queries.push(twitchTotalVH(7))
  queries.push(twitchTotalVH(30))
  queries.push(youtubeTotalVH(7))
  queries.push(youtubeTotalVH(30))
  await Promise.all(queries)
  let t2 = Date.now()

  console.log('Updated Query Cache ' + (t2 - t1) + ' ms')
}

async function twitchGamesCumVH (days = 30, numGames = 10) {
  try {
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY

    // Games beyond numGames will be lumped into 'Other' so the limit is 1000, not 10.
    let resp = await queries.twitchGamesCumVH(start, epoch, 1000)
    let data = []
    for (let i = 0; i < numGames - 1; i++) {
      data.push([resp[i]['name'], parseInt(resp[i]['viewers'])])
    }
    let other = 0
    for (let i = numGames - 1; i < resp.length; i++) {
      other += parseInt(resp[i]['viewers'])
    }
    data.push(['Other', other])
    cache.twitchGameCumVH[days] = data
  } catch (e) {
    console.log(e.message)
  }
}

async function youtubeTotalVH (days = 30) {
  try {
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let p = await queries.youtubeTotalVH(start, epoch)
    cache.youtubeTotalVH[days] = parseInt(p[0].sum)
  } catch (e) {
    console.log(e.message)
  }
}

async function twitchTotalVH (days = 30) {
  try {
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let p = await queries.twitchTotalVH(start, epoch)
    cache.twitchTotalVH[days] = parseInt(p[0].sum)
  } catch (e) {
    console.log(e.message)
  }
}

module.exports = cache
