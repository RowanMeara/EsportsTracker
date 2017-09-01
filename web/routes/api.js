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
    let q = await queries.twitchGameCumVHLast30()
    let l = []
    q.forEach((entry) => {
      l.push([entry['name'], parseInt(entry['viewers'])])
    })
    res.status(200).json(l)
  } catch (e) {
    console.log(e.message)
  }
})

module.exports = router
