SELECT g.name, ys.epoch, SUM(ys.viewers) AS viewers
FROM game AS g, youtube_stream AS ys
WHERE ys.game_id = $1 AND
      ys.epoch >= $2  AND
      ys.epoch < $3 AND
      g.game_id = ys.game_id AND
      (ys.language = 'en' OR ys.language = 'd_en')
GROUP BY g.name, ys.epoch
ORDER BY ys.epoch