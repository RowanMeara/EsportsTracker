import $ from 'jquery'
import {charts} from '../modules/loadcharts.js'
import {GoogleCharts} from '../modules/googleCharts.js'

GoogleCharts.load(drawCharts)
let days = 7

function drawCharts (resize = false, days = 7) {
  charts.twitchGameViewership(resize, days)
  charts.marketshare(resize, days)
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
