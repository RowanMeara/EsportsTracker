import $ from 'jquery'
import {charts} from '../modules/loadCharts.js'
import {GoogleCharts} from '../modules/googleCharts.js'
import '../modules/reqs.js'

let hgv, gameID, days
GoogleCharts.load(onLoad)

function onLoad () {
  let pn = window.location.pathname
  gameID = parseInt(pn.substring(6, pn.length))
  let active = $('div.btn.period-btn.active').text()
  days = parseInt(active.substring(0, active.length - ' Days'.length))
  hgv = new charts.HourlyGameViewership(gameID, 'gameviewership', days)
  drawCharts(days)
}

function drawCharts (days) {
  hgv.draw(days)
}

$(window).resize(() => {
  drawCharts(days)
})

$('#time_period_btn').change(async () => {
  await sleep(1)
  let active = $('div.btn.period-btn.active').text()
  days = parseInt(active.substring(0, active.length - ' Days'.length))
  drawCharts(days)
})

function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
