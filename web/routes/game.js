const express = require('express')
const queries = require('../server/queries')
const router = express.Router()

/* GET home page. */
router.get('/:gameid(\\d+)/', async function (req, res) {
  let vars = {
    title: 'Esports Market',
    esportsGames: queries.cache.esportsGames(),
    gameID: req.params.gameid
  }
  res.render('game', vars)
})

module.exports = router
