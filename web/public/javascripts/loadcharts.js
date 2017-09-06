google.charts.load('current', {'packages':['corechart']})
google.charts.setOnLoadCallback(drawChart)

function drawChart () {
  // Create the data table.
  let numbers = new Array()
  var jsonData = $.ajax({
    url: '/api/twitchgamescumlast30/',
    dataType: 'json',
    async: false,
    success: function (msg) {
      numbers = msg
    }
  }).responseText

  let data = new google.visualization.DataTable()
  data.addColumn('string', 'Game')
  data.addColumn('number', 'Viewer Years')
  data.addRows(numbers)

  let options = {
    width: '100%',
    height: '100%'
  }
  // Instantiate and draw our chart, passing in some options.
  let chart = new google.visualization.PieChart(document.getElementById('chart_div'))
  chart.draw(data, options)
}
