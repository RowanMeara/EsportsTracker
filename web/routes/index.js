const express = require('express')
const queries = require('../server/queries')
const router = express.Router()

/* GET home page. */
router.get('/', async function (req, res) {
  res.render('index', { title: 'Esports Market', esportsGames: queries.cache.esportsGames })
})

module.exports = router
