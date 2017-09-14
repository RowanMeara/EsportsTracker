const express = require('express')
const router = express.Router()

const queries = require('../server/queries')

/* GET home page. */
router.get('/', async function (req, res) {
  res.render('index', {title: 'Esports Market', esportsGames: queries.cache.esportsGames()})
})

module.exports = router
