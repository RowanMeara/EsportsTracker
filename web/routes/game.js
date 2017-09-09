const express = require('express')
const queries = require('../server/queries')
const router = express.Router()

/* GET home page. */
router.get('/:gameid(\\d+)/', async function (req, res) {
  let vars = {
    title: 'Esports Market', esportsGames: queries.cache.esportsGames,
    gameID: req.params.gameid,
  }
  console.log(vars.gameID)
  console.log(vars.esportsGames)
  res.render('index', vars)
})

module.exports = router
