const express = require('express')
const queries = require('../server/queries')
const router = express.Router()

/* GET home page. */
router.get('/', async function (req, res) {
  let q = 'Failure'
  try {
    q = await queries.getTwitchGamesTotalViewerHours(0, 1501822962, 20)
  }
  catch (e) {
    console.log(e.message)
  }
  console.log(JSON.stringify(q))
  res.render('index', { title: 'Esports Market', query: JSON.stringify(q) })
})

module.exports = router
