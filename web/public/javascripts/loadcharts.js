google.charts.load('current', {'packages':['corechart']})
google.charts.setOnLoadCallback(drawChart)

function drawChart () {
  // Create the data table.
  let numbers = new Array();
  var jsonData = $.ajax({
    url: '/api/',
    dataType: 'json',
    async: false,
    success: function (msg) {
      console.info(msg)
      numbers = msg
    }
  }).responseText

  var data = new google.visualization.DataTable()
  data.addColumn('string', 'Game')
  data.addColumn('number', 'Viewer Hours')
  data.addRows(numbers)

  var options = {
    width: 400,
    height: 400
  }
  // Instantiate and draw our chart, passing in some options.
  var chart = new google.visualization.PieChart(document.getElementById('chart_div'))
  chart.draw(data, options)
}
