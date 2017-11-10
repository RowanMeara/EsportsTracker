import {GoogleCharts} from './googleCharts.js'
import $ from 'jquery'
import 'datatables.net'
import 'datatables.net-bs4'

const fmt = require('./format')

const LINE_CHART_HEIGHT = 0.55
const LINE_CHART_HEIGHT_MOBILE = 0.8
const PI_CHART_HEIGHT = 0.7
const MOBILE_WIDTH = 767
const CHART_TITLE_SIZE = 16
const CHART_TITLE_MOBLE_SIZE = 12

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
      this.options.width = width - 1
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

class OrganizerMarketshare {
  constructor (divID, days) {
    this.divID = divID
    this.data = null
    let div = document.getElementById(divID)
    this.chart = new GoogleCharts.api.visualization.PieChart(div)
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
      this.options.width = width - 1
      this.chart.draw(this.data, this.options)
      return
    }
    this.days = days
    let render = (msg) => {
      formatTooltip(msg)
      this.data = new GoogleCharts.api.visualization.DataTable()
      this.data.addColumn('string', 'Organization')
      this.data.addColumn('number', 'Viewer Years')
      this.data.addColumn({type: 'string', role: 'tooltip'})
      this.data.addRows(msg)
      this.options.title = 'Organizer Marketshare Last ' + days + ' Days'
      this.draw(days)
    }
    $.ajax({
      url: '/api/organizerviewership',
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
      this.options.width = width - 1
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
          fontSize: 16
        }
      }
    }
  }

  draw (days) {
    if (this.data && this.days === days) {
      let div = document.getElementById(this.divID)
      let width = div.getBoundingClientRect().width
      if (window.innerWidth > MOBILE_WIDTH) {
        this.options.height = LINE_CHART_HEIGHT * width
        this.options.legend.position = 'bottom'
        this.options.titleTextStyle.fontSize = CHART_TITLE_SIZE
      } else {
        this.options.legend.position = 'none'
        this.options.titleTextStyle.fontSize = CHART_TITLE_MOBLE_SIZE
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
      this.data.addColumn('date', '')
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

function formatTooltip (apiResponse) {
  let sum = 0
  apiResponse.forEach((resp) => {
    sum += resp[1]
  })
  sum /= 1000000
  apiResponse.forEach((resp) => {
    resp[1] = resp[1] / 1000000
    let millions = resp[1].toFixed(2)
    let gamename = resp[0] + '\n '
    let time = fmt.splitMille(millions) + ' million hours'
    let percent = '(' + (millions / sum * 100).toFixed(2) + '%)'
    let tooltip = gamename + time + percent
    resp.push(tooltip)
  })
}

class EsportsGamesList {
  constructor (divID, days) {
    this.divID = divID
    this.data = null
  }

  draw (days) {
    if (this.table && days === this.days) {
      console.log('redrawn')
      return
    }
    this.days = days
    let render = (msg) => {
      this.data = []
      msg.forEach((m) => {
        let game = m[0]
        let esh = ((m[2] + m[3])/1000000).toFixed(2)
        let allh = ((m[4] + m[5])/1000000).toFixed(2)
        let per = fmt.formatPercent((m[2] + m[3]) / (m[4] + m[5]))
        this.data.push([game, esh, allh, per])
      })
      if (this.table) {
        this.table.clear()
        this.data.forEach((d) => {
          this.table.row.add(d)
        })
        this.table.draw()
      } else {
        this.table = new $('#' + this.divID).DataTable(
          {
            data: this.data,
            searching: false,
            paging: false,
            bInfo: false,
            bAutoWidth: false,
            order: [[1, 'desc']],
            columns: [
              {
                width: '30%',
                title: 'Game'
              },
              {
                width: '20%',
                title: 'Esports Hours\n (millions)'
              },
              {
                width: '20%',
                title: 'Total Hours\n (millions)'
              },
              {
                width: '20%',
                title: 'Percent Esports'
              }
            ]
          }
        )
      }
    }
    $.ajax({
      url: '/api/esportshoursbygame',
      data: {days: days},
      dataType: 'json',
      async: true,
      success: function (msg) {
        render(msg)
      }
    })
  }
}

export let charts = {
  TwitchGameViewership: TwitchGameViewership,
  Marketshare: Marketshare,
  HourlyGameViewership: HourlyGameViewership,
  OrganizerMarketshare: OrganizerMarketshare,
  EsportsGamesList: EsportsGamesList
}
