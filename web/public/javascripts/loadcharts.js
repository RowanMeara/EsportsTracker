google.charts.load('current', {'packages':['corechart']})
google.charts.setOnLoadCallback(drawCharts)

function drawCharts () {
  twitchGameViewershipLast30()
  marketshareLast30()
}

function twitchGameViewershipLast30 () {
  let render = function (numbers) {
    let data = new google.visualization.DataTable()
    data.addColumn('string', 'Game')
    data.addColumn('number', 'Viewer Years')
    data.addColumn({type: 'string', role: 'tooltip'})
    toYears(numbers)
    data.addRows(numbers)

    let options = {
      title: 'Twitch Viewership by Game Last 30 Days',
      vAxis: {format: '# years'},
      width: '100%',
      height: '100%'
    }
    // Instantiate and draw our chart, passing in some options.
    let chart = new google.visualization.PieChart(document.getElementById('twitchgamevh30'))
    chart.draw(data, options)
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

function marketshareLast30 () {
  let render = function (numbers) {
    toYears(numbers)
    let data = new google.visualization.DataTable()
    data.addColumn('string', 'Platform')
    data.addColumn('number', 'Viewer Years')
    data.addColumn({type: 'string', role: 'tooltip'})
    data.addRows(numbers)

    let options = {
      title: 'Platform Marketshare',
      vAxis: {format: '# years'},
      width: '100%',
      height: '100%'
    }

    // Instantiate and draw our chart, passing in some options.
    let chart = new google.visualization.PieChart(document.getElementById('marketshare'))
    chart.draw(data, options)
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
