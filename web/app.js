const express = require('express')
const path = require('path')
const favicon = require('serve-favicon')
const logger = require('morgan')
const cookieParser = require('cookie-parser')
const bodyParser = require('body-parser')
const api = require('./routes/api')
const index = require('./routes/index')
const game = require('./routes/game')

let app = express()
let env = app.get('env')

// View Engine Setup
app.set('views', path.join(__dirname, 'views'))
app.set('view engine', 'pug')

app.use(favicon(path.join(__dirname, 'public', 'favicon.ico')))
if (env === 'development') {
  app.use(logger('dev'))
} else {
  let format = '[:date[clf]] ":method :url HTTP/:http-version" :status :res[content-length]'
  app.use(logger(format + ' - :response-time ms'))
}
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(cookieParser())

if (env === 'development') {
  app.use(express.static(path.join(__dirname, 'public')))
} else {
  app.use(express.static(path.join(__dirname, 'public'), { maxage: '1d' }))
}

// Routes
app.use('/', index)
app.use('/api', api.router)
app.use('/game', game)

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
