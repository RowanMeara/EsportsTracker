const QueryFile = require('pg-promise').QueryFile
const path = require('path')

function sql (file) {
  const fullPath = path.join(__dirname, file)
  return new QueryFile(fullPath, {minify: true})
}

module.exports = {
  twitch_top_games: {
    totalHours: sql('twitch_top_games/totalviewerhours.sql'),
    cumHours: sql('twitch_top_games/cumulativeviewership.sql')
  },
  youtube_stream: {
    cumHours: sql('youtube_stream/cumulativeviewership.sql')
  },
  game: {
    esportsGames: sql('game/esportsgamelist.sql')
  },
  twitch_stream: {
    gameViewershipHourly: sql('twitch_stream/gameviewershiphourly.sql')
  }
}
