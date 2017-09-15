let path = require('path')
let webpack = require('webpack')
const Uglify = require('uglifyjs-webpack-plugin')

module.exports = {
  entry: {
    index: './assets/entrypoints/index.js',
    game: './assets/entrypoints/game.js'
  },
  output: {
    path: path.join(__dirname, 'public', 'javascripts'),
    filename: '[name].entry.js',
    chunkFilename: '[id].chunk.js'
  },
  plugins: [
    new webpack.optimize.CommonsChunkPlugin({
      filename: 'commons.js',
      name: 'commons'
    }),
    new Uglify()
  ],
  //devtool: 'inline-source-map'
}
