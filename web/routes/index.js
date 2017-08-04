const express = require('express')
const queries = require('../server/queries')
const router = express.Router()

/* GET home page. */
router.get('/', async function (req, res) {
  let q = 'Failure'
  console.log(JSON.stringify(q))
  res.render('index', { title: 'Esports Market', query: q })
})

module.exports = router
