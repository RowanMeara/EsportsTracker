const express = require('express')
const queries = require('../server/query_cache')
const router = express.Router()

/* GET home page. */
router.get('/:gameid(\\d+)/', async function (req, res) {
  let vars = {
    title: 'Esports Market',
    esportsGames: queries.esportsGames,
    gameID: req.params.gameid
  }
  res.render('game', vars)
})

module.exports = router
