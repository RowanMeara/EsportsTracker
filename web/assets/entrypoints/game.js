import $ from 'jquery'
import {charts} from '../modules/loadcharts.js'
import {GoogleCharts} from '../modules/googleCharts.js'

let days = 30
let hgv, gameID
GoogleCharts.load(onLoad)

function onLoad () {
  let pn = window.location.pathname
  gameID = parseInt(pn.substring(6, pn.length))
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
