import $ from 'jquery'
import {charts} from '../modules/loadcharts.js'
import {GoogleCharts} from '../modules/googleCharts.js'

GoogleCharts.load(drawCharts)

function drawCharts (resize = false) {
  charts.twitchGameViewershipLast30(resize)
  charts.marketshareLast30(resize)
}

$(window).resize(() => {
  drawCharts(true)
})
