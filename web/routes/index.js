const express = require('express')
const router = express.Router()

const config = require('../config')
const queries = require('../server/queries')

/* GET home page. */
router.get('/', async function (req, res) {
  res.render('index', {title: config.site.title, esportsGames: queries.cache.esportsGames()})
})

module.exports = router
