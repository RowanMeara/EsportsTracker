SELECT g.name, ts.epoch, SUM(ts.viewers) AS viewers
FROM game AS g, twitch_stream AS ts
WHERE ts.game_id = $1 AND
      ts.epoch >= $2  AND
      ts.epoch < $3 AND
      g.game_id = ts.game_id
GROUP BY g.name, ts.epoch
ORDER BY ts.epoch