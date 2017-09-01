const express = require('express')
const path = require('path')
const favicon = require('serve-favicon')
const logger = require('morgan')
const cookieParser = require('cookie-parser')
const bodyParser = require('body-parser')
const sassMiddleware = require('node-sass-middleware')
const api = require('./routes/api')
const index = require('./routes/index')

let app = express()
let env = app.get('env')

// View Engine Setup
app.set('views', path.join(__dirname, 'views'))
app.set('view engine', 'pug')

app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')))
if (env === 'development') {
  app.use(logger('dev'))
} else {
  app.use(logger('tiny'))
}
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(cookieParser())

const MCPath = path.join(__dirname, 'node_modules', 'material-components-web')
const mainSassPath = path.join(__dirname, 'public')
app.use(sassMiddleware({
  src: path.join(mainSassPath, MCPath, path.delimiter),
  dest: path.join(__dirname, 'public'),
  indentedSyntax: true, // true = .sass and false = .scss
  sourceMap: true
}))
app.use(express.static(path.join(__dirname, 'public')))

// Routes
app.use('/', index)
app.use('/api', api)

// 404 Handling
app.use(function (req, res, next) {
  let err = new Error('Not Found')
  err.status = 404
  next(err)
})

// Development Error Handling
app.use(function (err, req, res, next) {
  // Only provide error during development
  res.locals.message = err.message
  res.locals.error = req.app.get('env') === 'development' ? err : {}

  res.status(err.status || 500)
  res.render('error')
})

module.exports = app
