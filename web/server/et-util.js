
/**
 * Normalize a port into a number, string, or false.
 */
function normalizePort (val) {
  let port = parseInt(val, 10)

  if (isNaN(port)) {
    // named pipe
    return val
  }

  if (port >= 0) {
    // port number
    return port
  }

  return false
}

function getPort () {
  return normalizePort(process.env.PORT || '3000')
}

module.exports = {
  getPort: getPort
}
