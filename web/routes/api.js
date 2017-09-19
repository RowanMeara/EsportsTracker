const express = require('express')
const apicache = require('apicache')

const queries = require('../server/queries')

let app = express()
const router = express.Router()

let cache
if (app.get('env') === 'development') {
  cache = apicache.options({headers: {'cache-control': 'no-cache'}}).middleware
} else {
  cache = apicache.middleware
}
let DAY = 60 * 60 * 24

router.get('/twitchtopgames', cache('30 minutes'), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let numGames = req.query.numgames || 10
    let now = Math.floor(new Date() / 1000)
    let start = now - days * DAY

    // Games beyond numGames will be lumped into 'Other' so the limit is 1000, not 10.
    let resp = await queries.twitchGamesCumVH(start, now, 1000)
    let data = []
    for (let i = 0; i < numGames - 1; i++) {
      data.push([resp[i]['name'], parseInt(resp[i]['viewers'])])
    }
    let other = 0
    for (let i = numGames - 1; i < resp.length; i++) {
      other += parseInt(resp[i]['viewers'])
    }
    data.push(['Other', other])
    res.status(200).json(data)
  } catch (e) {
    console.trace(e.message)
  }
})

router.get('/marketshare', cache('30 minutes'), async function (req, res) {
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
  }
})

router.get('/twitchgameviewership', cache('30 minutes'), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let gameID = req.query.id
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let q = await queries.esportsGameHourly(gameID, start, epoch)
    let r = { name: q[0].name }
    let l = []
    q.forEach((entry) => {
      l.push([entry['epoch'], parseInt(entry['viewers'])])
    })
    r.data = l
    res.status(200).json(r)
  } catch (e) {
    console.trace(e.message)
  }
})

router.get('/youtubegameviewership', cache('30 minutes'), async function (req, res) {
  try {
    let days = parseInt(req.query.days) || 30
    let gameID = req.query.id
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - days * DAY
    let q = await queries.youtubeEsportsGameHourly(gameID, start, epoch)
    let r = { name: q[0].name }
    let l = []
    q.forEach((entry) => {
      l.push([entry['epoch'], parseInt(entry['viewers'])])
    })
    r.data = l
    res.status(200).json(r)
  } catch (e) {
    console.trace(e.message)
  }
})

router.get('/gameviewership', cache('30 minutes'), async function (req, res) {
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
  }
})

module.exports = router
