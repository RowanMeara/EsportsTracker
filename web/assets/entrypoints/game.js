import $ from 'jquery'
import {charts} from '../modules/loadcharts.js'

let days = 30
let pn = window.location.pathname
let gameID = parseInt(pn.substring(6, pn.length))
let hgv = new charts.HourlyGameViewership(gameID, 'gameviewership', days)
drawCharts()

function drawCharts (resize = false, days = 30) {
  hgv.draw(resize, days)
}

$(window).resize(() => {
  drawCharts(true, days)
})

$('#time_period_btn').change(async () => {
  await sleep(1)
  let active = $('div.btn.period-btn.active').text()
  days = parseInt(active.substring(0, active.length - ' Days'.length))
  drawCharts(false, days)
})

function sleep (ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}
