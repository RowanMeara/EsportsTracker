google.charts.load('current', {'packages':['corechart']})
google.charts.setOnLoadCallback(drawCharts)

function drawCharts (resize = false) {
  twitchGameViewershipLast30(resize)
  marketshareLast30(resize)
}

$(window).resize(() => {
  drawCharts(true)
  console.log('Charts redrawn.')
})

function twitchGameViewershipLast30 (resize = false) {
  let options = {
    title: 'Twitch Viewership by Game Last 30 Days',
    vAxis: {format: '# years'},
    width: '100%',
    height: '100%'
  }
  if (resize) {
    this.chart1.draw(this.data1, options)
    console.log('redrew tgv')
    return
  }
  let render = function (numbers) {
    toYears(numbers)
    this.data1 = new google.visualization.DataTable()
    this.data1.addColumn('string', 'Game')
    this.data1.addColumn('number', 'Viewer Years')
    this.data1.addColumn({type: 'string', role: 'tooltip'})
    this.data1.addRows(numbers)

    // Instantiate and draw our chart, passing in some options.
    this.chart1 = new google.visualization.PieChart(document.getElementById('twitchgamevh30'))
    this.chart1.draw(this.data1, options)
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

function marketshareLast30 (resize = false) {
  let options = {
    title: 'Platform Marketshare',
    vAxis: {format: '# years'},
    width: '100%',
    height: '100%'
  }
  if (resize) {
    this.chart.draw(this.data, options)
    console.log('marketshare')
    return
  }
  let render = function (numbers) {
    toYears(numbers)
    this.data = new google.visualization.DataTable()
    this.data.addColumn('string', 'Platform')
    this.data.addColumn('number', 'Viewer Years')
    this.data.addColumn({type: 'string', role: 'tooltip'})
    this.data.addRows(numbers)

    // Instantiate and draw our chart, passing in some options.
    this.chart = new google.visualization.PieChart(document.getElementById('marketshare'))
    this.chart.draw(this.data, options)
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

function toYears (apiResponse) {
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
