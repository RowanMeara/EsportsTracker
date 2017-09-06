const express = require('express')
const queries = require('../server/queries')
const router = express.Router()

router.get('/twitchtopgames', async function (req, res) {
  try {
    let start = 0
    let epoch = Math.floor(new Date() / 1000)
    if (req.query.time === 'last30days') {
      start = epoch - 2592000
    }
    let q = await queries.twitchGameCumVH(start, epoch, 10)
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
    console.log(queries.cache.twitchGameCumVHLast30)
    res.status(200).json(queries.cache.twitchGameCumVHLast30)
  } catch (e) {
    console.log(e.message)
  }
})

router.get('/marketsharelast30', async function (req, res) {
  let q = [
    ['Twitch', queries.cache.twitchTotalVHLast30],
    ['Youtube', queries.cache.youtubeTotalVHLast30]
  ]
  console.log(q)
  res.status(200).json(q)
})

module.exports = router
