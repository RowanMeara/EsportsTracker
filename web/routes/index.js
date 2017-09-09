const express = require('express')
const queries = require('../server/query_cache')
const router = express.Router()

/* GET home page. */
router.get('/', async function (req, res) {
  res.render('index', { title: 'Esports Market', esportsGames: queries.esportsGames })
})

module.exports = router
