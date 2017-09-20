const express = require('express')
const router = express.Router()

const config = require('../config')
const queries = require('../server/queries')

/* GET home page. */
router.get('/', async function (req, res) {
  let vars = {
    title: config.site.title,
    esportsGames: queries.cache.esportsGames(),
    timePeriods: config.api.days
  }
  res.render('index', vars)
})

module.exports = router
