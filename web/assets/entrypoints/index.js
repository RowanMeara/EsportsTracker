import $ from 'jquery'
import {charts} from '../modules/loadcharts.js'
import {GoogleCharts} from '../modules/googleCharts.js'

GoogleCharts.load(drawCharts)

function drawCharts (resize = false) {
  charts.twitchGameViewership(resize)
  charts.marketshare(resize)
}

$(window).resize(() => {
  drawCharts(true)
})
