let path = require('path')

module.exports = {
  entry: './assets/javascripts/loadcharts.js',
  output: {
    path: path.join(__dirname, 'public', 'javascripts'),
    filename: 'bundle.js'
  },
  devtool: 'inline-source-map'
}
