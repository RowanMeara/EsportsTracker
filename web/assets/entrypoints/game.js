import $ from 'jquery'
import {charts} from '../modules/loadcharts.js'
import {GoogleCharts} from '../modules/googleCharts.js'

GoogleCharts.load(drawCharts)

function drawCharts (resize = false) {
  let pn = window.location.pathname
  let gameID = parseInt(pn.substring(6, pn.length))
  charts.hourlyGameViewership(gameID, resize)
}

$(window).resize(() => {
  drawCharts(true)
})
