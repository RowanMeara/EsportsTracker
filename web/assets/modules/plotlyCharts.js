import $ from 'jquery'
const Plotly = require('plotly.js/lib/core')
const dateFormat = require('dateformat')

// Register modules needed outside of the core to reduce build size.
Plotly.register([

])

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
        title: 'Concurrent Viewers'
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

function epochToDateString (epoch) {
  let d = new Date(0)
  d.setUTCSeconds(epoch)
  return dateFormat(d, 'yyyy-mm-dd HH:MM:ss')
}

module.exports = {
  HourlyGameViewership: HourlyGameViewership
}
