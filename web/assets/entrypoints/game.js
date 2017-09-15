import $ from 'jquery'
import {charts} from '../modules/loadcharts.js'
import {GoogleCharts} from '../modules/googleCharts.js'

GoogleCharts.load(drawCharts)
let pn = window.location.pathname

function drawCharts (resize = false) {
  let gameID = parseInt(pn.substring(6, pn.length))
  charts.hourlyGameViewership(gameID, resize)
}

$(window).resize(() => {
  drawCharts(true)
})
