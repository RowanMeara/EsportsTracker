const express = require('express')
const queries = require('../server/queries')
const router = express.Router()

router.get('/test1', function (req, res) {
  // TODO: remove
  res.send('TESTING ME')
})

router.get('/', async function (req, res) {
  let q = 'Failure'
  try {
    q = await queries.getTwitchGamesTotalViewerHours(0, 1501822962, 20)
    let l = []
    q.forEach((entry) => {
      l.push([entry['name'], parseInt(entry['viewers'])])
    })
    console.log(l)
    res.status(200).json(l)
  } catch (e) {
    console.log(e.message)
  }
})

module.exports = router
