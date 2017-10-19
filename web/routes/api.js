const http = require('http')
const os = require('os')

const express = require('express')
const apicache = require('apicache')

const config = require('../config')
const queries = require('../server/queries')
const util = require('../server/et-util')
const secrets = require('../secrets')

const router = express.Router()
// TODO: Consider changing cache control back.
let options = {
  defaultDuration: '2 hours',
  headers: {
    // There is a bug with 304 responses in apicache.
    //'cache-control': 'no-cache, no-store, must-revalidate'
    'cache-control': 'no-cache'
  }
}
let cache = apicache.options(options).middleware

const DAY = 60 * 60 * 24

router.get('/twitchtopgames', cache(), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let numGames = req.query.numgames || 10
    let now = Math.floor(new Date() / 1000)
    let start = now - days * DAY

    // Games beyond numGames will be lumped into 'Other' so the limit is 1000, not 10.
    let resp = await queries.twitchGamesCumVH(start, now, 1000)
    let data = []
    let len = Math.min(numGames - 1, resp.length)
    for (let i = 0; i < len; i++) {
      data.push([resp[i]['name'], parseInt(resp[i]['viewers'])])
    }
    let other = 0
    for (let i = len; i < resp.length; i++) {
      other += parseInt(resp[i]['viewers'])
    }
    data.push(['Other', other])
    res.status(200).json(data)
  } catch (e) {
    console.trace(e.message)
    res.status(500)
  }
})

router.get('/marketshare', cache(), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let now = Math.floor(new Date() / 1000)
    let start = now - days * DAY
    let tvh = queries.twitchTotalVH(start, now)
    let yvh = queries.youtubeTotalVH(start, now)
    let results = await Promise.all([tvh, yvh])

    let q = [
      ['Twitch', parseInt(results[0][0].sum)],
      ['Youtube', parseInt(results[1][0].sum)]
    ]
    res.status(200).json(q)
  } catch (e) {
    console.trace(e.message)
    res.status(500)
  }
})

router.get('/twitchgameviewership', cache(), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let gameID = req.query.id
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let rows = await queries.esportsGameHourly(gameID, start, epoch)
    let r = { name: rows[0].name }
    let l = []
    rows.forEach((row) => {
      l.push([row['epoch'], parseInt(row['viewers'])])
    })
    r.data = l
    res.status(200).json(r)
  } catch (e) {
    console.trace(e.message)
    res.status(500)
  }
})

router.get('/youtubegameviewership', cache(), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let gameID = req.query.id
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let rows = await queries.youtubeEsportsGameHourly(gameID, start, epoch)
    if (rows.length > 0) {
      let resp = { name: rows[0].name }
      let l = []
      rows.forEach((row) => {
        l.push([row['epoch'], parseInt(row['viewers'])])
      })
      resp.data = l
      res.status(200).json(resp)
    } else {
      let name = await queries.gameidToName(gameID)
      let resp = {name: name, data: []}
      res.status(200).json(resp)
    }
  } catch (e) {
    console.trace(e.message)
    res.status(500)
  }
})

router.get('/gameviewership', cache(), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let gameID = req.query.id
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let q = await queries.combinedGameVHHourly(gameID, start, epoch)
    let r = { name: q[0].name }
    let l = []
    q.forEach((entry) => {
      l.push([entry.epoch, parseInt(entry.viewers), parseInt(entry.ytviewers)])
    })
    r.data = l
    res.status(200).json(r)
  } catch (e) {
    console.trace(e.message)
    res.status(500)
  }
})

router.get('/organizerviewership', cache(), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let numOrgs = req.query.num || 10
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let q = await queries.orgMarketshareCum(start, epoch)
    let data = []
    let len = Math.min(numOrgs - 1, q.length)
    for (let i = 0; i < len; i++) {
      data.push([q[i]['org_name'], parseInt(q[i]['viewers'])])
    }
    let other = 0
    for (let i = len; i < q.length; i++) {
      other += parseInt(q[i]['viewers'])
    }
    data.push(['Other', other])
    res.status(200).json(data)
  } catch (e) {
    console.trace(e.message)
    res.status(500)
  }
})

router.get('/esportshoursbygame', cache(), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let numGames = req.query.num || 10
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let q = await queries.esportsHoursByGame(start, epoch, numGames)
    let data = []
    // TODO: Standardize headers.
    // data.push(['gameName', 'gameID', 'ytEsports', 'twEsports', 'ytAll', 'twAll'])
    q.forEach((q) => {
      data.push([
        q.name,
        parseInt(q.game_id),
        parseInt(q.ythours),
        parseInt(q.twhours),
        parseInt(q.ytallhours),
        parseInt(q.twallhours)
      ])
    })
    res.status(200).json(data)
  } catch (e) {
    console.trace(e.message)
    res.status(500)
  }
})

/**
 * Called by the aggregator service to refresh the cache.  Requires the database
 * username and password specified in the secrets file to be sent as query strings.
 */
router.get('/refreshcache', async function (req, res) {
  try {
    let user = req.query.user || ''
    let pwd = req.query.pwd || ''
    if (user === secrets.pg_user && pwd === secrets.pg_passwd) {
      refreshCache()
      res.status(200)
      res.send()
    } else {
      res.status(403)
    }
  } catch (e) {
    res.status(500)
  }
})

/**
 * Refresh the api cache.
 */
async function refreshCache () {
  try {
    let start = Date.now()
    let days = config.api.days
    const host = 'localhost'
    const port = util.getPort()
    const fifteenMins = 1000 * 60 * 15
    let esg = await queries.esportsGames()
    let esgid = []
    esg.forEach((g) => {
      esgid.push(g.game_id)
    })
    let daypaths = [
      '/api/marketshare?days=',
      '/api/twitchtopgames?days=',
      '/api/organizerviewership?days=',
      '/api/esportshoursbygame?days='
    ]
    let paths = []
    esgid.forEach((gid) => {
      daypaths.push('/api/twitchgameviewership?id=' + gid.toString() + '&days=')
      daypaths.push('/api/youtubegameviewership?id=' + gid.toString() + '&days=')
      daypaths.push('/api/gameviewership?id=' + gid.toString() + '&days=')
    })

    daypaths.forEach((path) => {
      days.forEach((day) => {
        paths.push(path + day)
      })
    })

    let reqs = []
    for (let i = 0; i < paths.length; i++) {
      if (i % 200 === 0) {
        await Promise.all(reqs)
        reqs = []
      }
      apicache.clear(paths[i])
      let options = {
        hostname: host,
        port: port,
        path: paths[i],
        timeout: fifteenMins
      }
      reqs.push(httpPromise(options))
    }
    await Promise.all(reqs)
    let total = (Date.now() - start) / 1000
    console.log('Refresh Complete: ' + paths.length + ' requests in ' + total + 's')
  } catch (e) {
    console.log('Refresh Partially Failed')
    console.trace(e.message)
  }
}

async function httpPromise (options) {
  return new Promise((resolve, reject) => {
    let req = http.get(options)
    req.on('response', res => {
      resolve(res)
    })
    req.on('error', err => {
      console.log('API request failed: ' + url)
      resolve(err)
    })
  })
}

module.exports = {
  router: router,
  refreshCache: refreshCache
}
