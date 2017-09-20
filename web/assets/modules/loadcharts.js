import {GoogleCharts} from './googleCharts.js'
import $ from 'jquery'
import 'bootstrap/dist/js/bootstrap.js'
const Plotly = require('./customPlotly')
const dateFormat = require('dateformat')

let lineChartHeight = 0.55
let piChartHeight = 0.7

let chartTGV
let dataTGV
function twitchGameViewership (resize = false, days = 30) {
  let options = {
    title: 'Twitch Viewership by Game Last ' + days + ' Days',
    vAxis: {format: '# years'},
    width: '100%',
    height: piChartHeight * $('#twitchgamevh').width()
  }
  if (resize) {
    chartTGV.draw(dataTGV, options)
    return
  }
  let render = function (numbers) {
    formatTooltip(numbers)
    dataTGV = new GoogleCharts.api.visualization.DataTable()
    dataTGV.addColumn('string', 'Game')
    dataTGV.addColumn('number', 'Viewer Years')
    dataTGV.addColumn({type: 'string', role: 'tooltip'})
    dataTGV.addRows(numbers)

    // Instantiate and draw our chart, passing in some options.
    chartTGV = new GoogleCharts.api.visualization.PieChart(document.getElementById('twitchgamevh'))
    chartTGV.draw(dataTGV, options)
  }

  $.ajax({
    url: '/api/twitchtopgames?numgames=10',
    data: {days: days},
    dataType: 'json',
    async: true,
    success: function (msg) {
      render(msg)
    }
  })
}

let dataMks
let chartMks
function marketshare (resize = false, days = 30) {
  let options = {
    title: 'Platform Marketshare Last ' + days + ' Days',
    vAxis: {format: '# years'},
    width: '100%',
    height: piChartHeight * $('#marketshare').width()
  }
  if (resize) {
    chartMks.draw(dataMks, options)
    return
  }
  let render = function (numbers) {
    formatTooltip(numbers)
    dataMks = new GoogleCharts.api.visualization.DataTable()
    dataMks.addColumn('string', 'Platform')
    dataMks.addColumn('number', 'Viewer Years')
    dataMks.addColumn({type: 'string', role: 'tooltip'})
    dataMks.addRows(numbers)

    // Instantiate and draw our chart, passing in some options.
    chartMks = new GoogleCharts.api.visualization.PieChart(document.getElementById('marketshare'))
    chartMks.draw(dataMks, options)
  }

  $.ajax({
    url: '/api/marketshare',
    data: {days: days},
    dataType: 'json',
    async: true,
    success: function (msg) {
      render(msg)
    }
  })
}

let dataHGV
let chartHGV
let optionsHGV
function hourlyGameViewership (gameID, resize = false, days = 30) {
  if (resize) {
    optionsHGV.height = lineChartHeight * $('#gameviewership').width()
    chartHGV.draw(dataHGV, optionsHGV)
    return
  }
  optionsHGV = {
    width: '100%',
    height: lineChartHeight * $('#gameviewership').width(),
    legend: {position: 'bottom'},
    title: '',
    subtitle: 'English Language Streams Only',
    titleTextStyle: {
      fontName: 'Helvetica',
      fontSize: 16,
      bold: true
    },
    vAxis: {
      textStyle: {
        fontSize: 14
      }
    },
    hAxis: {
      textStyle: {
        fontSize: 14
      }
    },
    chart: {}
  }

  let render = function (data) {
    dataHGV = new GoogleCharts.api.visualization.DataTable()
    chartHGV = new GoogleCharts.api.visualization.LineChart(document.getElementById('gameviewership'))
    data.data.forEach((ts) => {
      ts[0] = new Date(ts[0] * 1000)
    })
    // formatTooltip(data)
    dataHGV.addColumn('date', 'Date')
    dataHGV.addColumn('number', 'Twitch')
    dataHGV.addColumn('number', 'Youtube')
    dataHGV.addRows(data.data)
    optionsHGV.title = data.name + ' Concurrent Viewership Last ' + days + ' Days'
    optionsHGV.subtitle = 'English Language Streams Only'
    // Instantiate and draw our chart, passing in some options.
    chartHGV.draw(dataHGV, optionsHGV)
  }

  $.ajax({
    url: '/api/gameviewership',
    data: {id: gameID, days: days},
    dataType: 'json',
    async: true,
    success: function (msg) {
      render(msg)
    }
  })
}

class HourlyGameViewership {
  constructor (gameID, divID, days = 30) {
    this.gameID = gameID
    this.divID = divID
    this.days = days
    this.traces = []
    this.layout = {
      autosize: true,
      legend: {
        orientation: 'h'
      },
      title: '',
      titlefont: {
        size: 16
      },
      yaxis: {
        title: 'Concurrent Viewers',
      }
    }
  }

  draw (resize = false, days = 30) {
    if (resize) {
      Plotly.newPlot(this.divID, this.traces, this.layout)
      return
    }

    this.days = days

    let render = (msg) => {
      let dates = []
      let twitch = []
      let youtube = []
      msg.data.forEach((row) => {
        dates.push(epochToDateString(row[0]))
        twitch.push(row[1])
        youtube.push(row[2])
      })
      let twitchTrace = {
        x: dates,
        y: twitch,
        mode: 'lines',
        name: 'Twitch'
      }
      let youtubeTrace = {
        x: dates,
        y: youtube,
        mode: 'lines',
        name: 'YouTube'
      }
      this.layout.title = msg.name + ' Viewership Last ' + this.days + ' Days <br> (English Streams Only)'
      this.traces = [twitchTrace, youtubeTrace]
      Plotly.newPlot(this.divID, this.traces, this.layout)
    }

    $.ajax({
      url: '/api/gameviewership',
      data: {id: this.gameID, days: this.days},
      dataType: 'json',
      async: true,
      success: function (msg) {
        render(msg)
      }
    })
  }
}

function splitMille (n, separator = ',') {
  let num = (n + '')
  let decimals = ''
  if (/\./.test(num)) {
    decimals = num.replace(/^.*(\..*)$/, '$1')
  }
  num = num.replace(decimals, '')
    .split('').reverse().join('')
    .match(/[0-9]{1,3}-?/g)
    .join(separator).split('').reverse().join('')

  return `${num}${decimals}`
}

function formatTooltip (apiResponse) {
  let sum = 0
  apiResponse.forEach((resp) => {
    sum += resp[1]
  })
  sum /= 365 * 24
  apiResponse.forEach((resp) => {
    resp[1] = resp[1] / (365 * 24)
    let years = resp[1].toFixed(2)
    let gamename = resp[0] + '\n '
    let time = splitMille(years) + ' years '
    let percent = '(' + (years / sum * 100).toFixed(0) + '%)'
    let tooltip = gamename + time + percent
    resp.push(tooltip)
  })
}

function epochToDateString (epoch) {
  let d = new Date(0)
  d.setUTCSeconds(epoch)
  return dateFormat(d, 'yyyy-mm-dd HH:MM:ss')
}

export let charts = {
  twitchGameViewership: twitchGameViewership,
  marketshare: marketshare,
  hourlyGameViewership: hourlyGameViewership,
  HourlyGameViewership: HourlyGameViewership
}
