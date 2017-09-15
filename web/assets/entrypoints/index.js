import $ from 'jquery'
import {charts} from '../javascripts/loadcharts.js'
import {GoogleCharts} from '../javascripts/googleCharts.js'

GoogleCharts.load(drawCharts)

function drawCharts (resize = false) {
  charts.twitchGameViewershipLast30(resize)
  charts.marketshareLast30(resize)
}

$(window).resize(() => {
  drawCharts(true)
})
