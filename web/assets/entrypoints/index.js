import $ from 'jquery'
import {charts} from '../modules/loadCharts.js'
import {GoogleCharts} from '../modules/googleCharts.js'
import '../modules/reqs.js'

let tgv, mks, tmk, days, dt

function onLoad () {
  let active = $('div.btn.period-btn.active').text()
  days = parseInt(active.substring(0, active.length - ' Days'.length))
  tgv = new charts.TwitchGameViewership('twitchgamevh', days)
  mks = new charts.Marketshare('marketshare', days)
  tmk = new charts.OrganizerMarketshare('orgmarketshare', days)
  dt = new charts.EsportsGamesList('datatable', days)
  drawCharts(days)
}

function drawCharts (ldays) {
  dt.draw(ldays)
  tgv.draw(ldays)
  mks.draw(ldays)
  tmk.draw(ldays)
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

$(document).ready(() => {
  GoogleCharts.load(onLoad)
})
