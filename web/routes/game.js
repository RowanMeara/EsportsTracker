const express = require('express')
const queries = require('../server/queries')
const config = require('../config')

const router = express.Router()

/* GET home page. */
router.get('/:gameid(\\d+)/', async function (req, res) {
  let vars = {
    title: config.site.title,
    esportsGames: queries.cache.esportsGames(),
    gameID: req.params.gameid,
    timePeriods: config.api.days
  }
  res.render('game', vars)
})

module.exports = router
