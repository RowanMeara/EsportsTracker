google.charts.load('44', {'packages': ['corechart', 'line']})
google.charts.setOnLoadCallback(drawCharts)

function drawCharts (resize = false) {
  let pn = window.location.pathname
  if (pn === '/') {
    twitchGameViewershipLast30(resize)
    marketshareLast30(resize)
  } else if (pn.includes('/game/')) {
    let gameID = parseInt(pn.substring(6, pn.length))
    hourlyGameViewership(gameID, resize)
  }
}

$(window).resize(() => {
  drawCharts(true)
})

let chartTGV
let dataTGV
function twitchGameViewershipLast30 (resize = false) {
  let options = {
    title: 'Twitch Viewership by Game Last 30 Days',
    vAxis: {format: '# years'},
    width: '100%',
    height: 300
  }
  if (resize) {
    chartTGV.draw(dataTGV, options)
    return
  }
  let render = function (numbers) {
    formatTooltip(numbers)
    dataTGV = new google.visualization.DataTable()
    dataTGV.addColumn('string', 'Game')
    dataTGV.addColumn('number', 'Viewer Years')
    dataTGV.addColumn({type: 'string', role: 'tooltip'})
    dataTGV.addRows(numbers)

    // Instantiate and draw our chart, passing in some options.
    chartTGV = new google.visualization.PieChart(document.getElementById('twitchgamevh30'))
    chartTGV.draw(dataTGV, options)
  }

  $.ajax({
    url: '/api/twitchgamescumlast30/',
    dataType: 'json',
    async: true,
    success: function (msg) {
      render(msg)
    }
  })
}

let dataMks
let chartMks
function marketshareLast30 (resize = false) {
  let options = {
    title: 'Platform Marketshare',
    vAxis: {format: '# years'},
    width: '100%',
    height: 300
  }
  if (resize) {
    chartMks.draw(dataMks, options)
    return
  }
  let render = function (numbers) {
    formatTooltip(numbers)
    dataMks = new google.visualization.DataTable()
    dataMks.addColumn('string', 'Platform')
    dataMks.addColumn('number', 'Viewer Years')
    dataMks.addColumn({type: 'string', role: 'tooltip'})
    dataMks.addRows(numbers)

    // Instantiate and draw our chart, passing in some options.
    chartMks = new google.visualization.PieChart(document.getElementById('marketshare'))
    chartMks.draw(dataMks, options)
  }

  $.ajax({
    url: '/api/marketsharelast30/',
    dataType: 'json',
    async: true,
    success: function (msg) {
      render(msg)
    }
  })
}

let dataHGV
let chartHGV
let optionsHGV = {
  width: '100%',
  height: 600,
  legend: {position: 'none'},
  hAxis: {
    textStyle: {
      fontSize: 20
    }
  },
  vAxis: {
    textPosition: 'in',
    title: 'Number of Concurrent Viewers'
  },
  chart: {}
}
function hourlyGameViewership (gameID, resize = false) {
  if (resize) {
    chartHGV.draw(dataHGV, optionsHGV)
    return
  }

  let render = function (data) {
    dataHGV = new google.visualization.DataTable()
    chartHGV = new google.charts.Line(document.getElementById('gameviewership'))
    data.data.forEach((ts) => {
      ts[0] = new Date(ts[0] * 1000)
    })
    // formatTooltip(data)
    dataHGV.addColumn('date', 'Date')
    dataHGV.addColumn('number', 'Concurrent Viewers')
    // dataHGV.addColumn({type: 'string', role: 'tooltip'})
    dataHGV.addRows(data.data)
    optionsHGV.chart.title = data.name + ' Concurrent Viewership'
    optionsHGV.chart.subtitle = 'in English Language Streams'
    // Instantiate and draw our chart, passing in some options.
    chartHGV.draw(dataHGV, optionsHGV)
  }

  $.ajax({
    url: '/api/gameviewership?id=' + gameID,
    dataType: 'json',
    async: true,
    success: function (msg) {
      render(msg)
    }
  })
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
