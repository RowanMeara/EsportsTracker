const http = require('http')

const express = require('express')
const apicache = require('apicache')

const config = require('../config')
const queries = require('../server/queries')
const util = require('../server/et-util')

let app = express()
const router = express.Router()

let cache
if (app.get('env') === 'development') {
  cache = apicache.options({headers: {'cache-control': 'no-cache'}}).middleware
} else {
  cache = apicache.middleware
}
let DAY = 60 * 60 * 24

router.get('/twitchtopgames', cache('60 minutes'), async function (req, res) {
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

router.get('/marketshare', cache('60 minutes'), async function (req, res) {
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

router.get('/twitchgameviewership', cache('60 minutes'), async function (req, res) {
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

router.get('/youtubegameviewership', cache('60 minutes'), async function (req, res) {
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

router.get('/gameviewership', cache('60 minutes'), async function (req, res) {
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

router.get('/organizerviewership', cache('60 minutes'), async function (req, res) {
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

/**
 * Refresh the api cache.
 */
async function refreshCache () {
  try {
    let start = Date.now()
    let days = config.api.days
    let host = 'http://localhost:' + util.getPort()
    let esg = await queries.esportsGames()
    let esgid = []
    esg.forEach((g) => {
      esgid.push(g.game_id)
    })
    let daypaths = [
      host + '/api/marketshare?days=',
      host + '/api/twitchtopgames?days=',
      host + '/api/organizerviewership?days='
    ]
    let urls = []
    esgid.forEach((gid) => {
      daypaths.push(host + '/api/twitchgameviewership?id=' + gid.toString() + '&days=')
      daypaths.push(host + '/api/youtubegameviewership?id=' + gid.toString() + '&days=')
      daypaths.push(host + '/api/gameviewership?id=' + gid.toString() + '&days=')
    })

    daypaths.forEach((path) => {
      days.forEach((day) => {
        urls.push(path + day)
      })
    })

    let httpPromise = async (url) => {
      return new Promise((resolve, reject) => {
        let req = http.get(url)
        req.on('response', res => {
          resolve(res)
        })
        req.on('error', err => {
          console.log('API request failed: ' + url)
          resolve(err)
        })
      })
    }

    let reqs = []
    for (let i = 0; i < urls.length; i++) {
      if (i % 10 === 0) {
        await Promise.all(reqs)
        reqs = []
      }
      let target = urls[i].substring(host.length, urls[i].length)
      apicache.clear(target)
      reqs.push(httpPromise(urls[i]))
    }
    await Promise.all(reqs)
    let total = (Date.now() - start) / 1000
    console.log('Refresh Complete: ' + urls.length + ' requests in ' + total + 's')
  } catch (e) {
    console.log('Refresh Partially Failed')
    console.trace(e.message)
  }
}

module.exports = {
  router: router,
  refreshCache: refreshCache
}
