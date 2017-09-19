let path = require('path')
let webpack = require('webpack')
const Uglify = require('uglifyjs-webpack-plugin')
const ExtractTextPlugin = require('extract-text-webpack-plugin')

const extractSass = new ExtractTextPlugin({
  filename: 'style.css'
})

module.exports = {
  entry: {
    index: './assets/entrypoints/index.js',
    game: './assets/entrypoints/game.js',
    req: './assets/entrypoints/styles.js'
  },
  output: {
    path: path.join(__dirname, 'public', 'webpack'),
    filename: '[name].entry.js',
    chunkFilename: '[id].chunk.js'
  },
  module: {
    rules: [
      {
        test: /\.sass$/,
        use: extractSass.extract({
          use: ['css-loader', 'sass-loader']
        })
      },
      {
        test: /\.woff(2)?(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        loader: 'url-loader?limit=10000000&mimetype=application/font-woff'
      },
      { test: /\.(ttf|eot|svg)(\?v=[0-9]\.[0-9]\.[0-9])?$/,
        loader: 'base64-inline-loader?limit=1000000&name=[name].[ext]' }
    ]
  },
  plugins: [
    // Bootstrap
    new webpack.ProvidePlugin({
      jQuery: 'jquery',
      $: 'jquery',
      'window.jQuery': 'jquery',
      Tether: 'tether'
    }),
    // Sass
   extractSass,
    // JavaScript
    new webpack.optimize.CommonsChunkPlugin({
      filename: 'commons.js',
      name: 'commons'
    }),
    new Uglify()
  ],
  //devtool: 'inline-source-map'
}
