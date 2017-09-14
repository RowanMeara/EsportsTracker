const express = require('express')
const cache = require('../server/query_cache')
const queries = require('../server/queries')
const router = express.Router()

router.get('/twitchtopgames', async function (req, res) {
  try {
    let start = 0
    let epoch = Math.floor(new Date() / 1000)
    if (req.query.time === 'last30days') {
      start = epoch - 2592000
    }
    let q = cache.twitchGameCumVH[30]
    let l = []
    q.forEach((entry) => {
      l.push([entry['name'], parseInt(entry['viewers'])])
    })
    res.status(200).json(l)
  } catch (e) {
    console.log(e.message)
  }
})

router.get('/twitchgamescumlast30', async function (req, res) {
  try {
    res.status(200).json(cache.twitchGameCumVH[30])
  } catch (e) {
    console.log(e.message)
  }
})

async function marketshareHandler(start, end) {

}

router.get('/marketsharelast30', async function (req, res) {
  let q = [
    ['Twitch', cache.twitchTotalVH[30]],
    ['Youtube', cache.youtubeTotalVH[30]]
  ]
  res.status(200).json(q)
})

router.get('/gameviewership', async function (req, res) {
  try {
    let gameID = req.query.id
    let epoch = Math.floor(new Date() / 1000)
    let start = epoch - 2592000
    let q = await queries.esportsGameHourly(gameID, start, epoch)
    let r = { name: q[0].name }
    let l = []
    q.forEach((entry) => {
      l.push([entry['epoch'], parseInt(entry['viewers'])])
    })
    r.data = l
    res.status(200).json(r)
  } catch (e) {
    console.log(e.message)
  }
})

module.exports = router
