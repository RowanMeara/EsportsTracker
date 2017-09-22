import {GoogleCharts} from './googleCharts.js'
import $ from 'jquery'

const LINE_CHART_HEIGHT = 0.55
const LINE_CHART_HEIGHT_MOBILE = 0.7
const PI_CHART_HEIGHT = 0.7
const MOBILE_WIDTH = 767

class TwitchGameViewership {
  constructor (divID, days) {
    this.divID = divID
    this.data = null
    let div = document.getElementById(divID)
    this.chart = new GoogleCharts.api.visualization.PieChart(div)
    this.options = {
      title: 'Twitch Viewership by Game Last ' + days + ' Days',
      vAxis: {format: '# years'},
      width: '100%'
    }
  }

  draw (days) {
    if (this.data && days === this.days) {
      let div = document.getElementById(this.divID)
      let width = div.getBoundingClientRect().width
      this.options.height = PI_CHART_HEIGHT * width
      this.chart.draw(this.data, this.options)
      return
    }
    this.days = days
    let render = (msg) => {
      formatTooltip(msg)
      this.data = new GoogleCharts.api.visualization.DataTable()
      this.data.addColumn('string', 'Game')
      this.data.addColumn('number', 'Viewer Years')
      this.data.addColumn({type: 'string', role: 'tooltip'})
      this.data.addRows(msg)
      this.options.title = 'Twitch Viewership by Game Last ' + days + ' Days'
      this.draw(days)
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
}

class Marketshare {
  constructor (divID, days) {
    this.days = days
    this.divID = divID
    let div = document.getElementById(divID)
    this.chart = new GoogleCharts.api.visualization.PieChart(div)
    this.data = null
    this.options = {
      vAxis: {format: '# years'},
      width: '100%'
    }
  }

  draw (days) {
    if (this.data && days === this.days) {
      let div = document.getElementById(this.divID)
      let width = div.getBoundingClientRect().width
      this.options.height = PI_CHART_HEIGHT * width
      this.chart.draw(this.data, this.options)
      return
    }
    this.days = days
    let render = (numbers) => {
      formatTooltip(numbers)
      this.data = new GoogleCharts.api.visualization.DataTable()
      this.data.addColumn('string', 'Platform')
      this.data.addColumn('number', 'Viewer Years')
      this.data.addColumn({type: 'string', role: 'tooltip'})
      this.data.addRows(numbers)
      this.options.title = 'Platform Marketshare Last ' + days + ' Days'
      this.draw(this.days)
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
}

class HourlyGameViewership {
  constructor (gameID, divID, days) {
    this.gameID = gameID
    this.divID = divID
    this.days = days
    this.data = null
    let div = document.getElementById(divID)
    this.chart = new GoogleCharts.api.charts.Line(div)
    this.options = {
      width: '100%',
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
      chart: {},
      legend: {
        textStyle: {
          fontSize: 14
        }
      }
    }
  }

  draw (days) {
    if (this.data && this.days === days) {
      let div = document.getElementById(this.divID)
      let width = div.getBoundingClientRect().width
      if (window.innerWidth > MOBILE_WIDTH) {
        this.options.legend.textStyle.fontSize = 16
        this.options.height = LINE_CHART_HEIGHT * width
      } else {
        this.options.legend.textStyle.fontSize = 10
        this.options.height = LINE_CHART_HEIGHT_MOBILE * width
      }
      let opt = GoogleCharts.api.charts.Line.convertOptions(this.options)
      this.chart.draw(this.data, opt)
      return
    }
    this.days = days

    let render = (msg) => {
      this.data = new GoogleCharts.api.visualization.DataTable()
      msg.data.forEach((ts) => {
        ts[0] = new Date(ts[0] * 1000)
      })
      this.data.addColumn('date', 'Date')
      this.data.addColumn('number', 'Twitch')
      this.data.addColumn('number', 'Youtube')
      this.data.addRows(msg.data)
      this.options.title = msg.name + ' Viewers Last ' + this.days + ' Days'
      this.draw(this.days)
    }

    $.ajax({
      url: '/api/gameviewership',
      data: {id: this.gameID, days: days},
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

export let charts = {
  TwitchGameViewership: TwitchGameViewership,
  Marketshare: Marketshare,
  HourlyGameViewership: HourlyGameViewership
}
