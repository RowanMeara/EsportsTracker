SELECT game.name,
       v.viewers
FROM   game
       INNER JOIN (SELECT game_id,
                          Sum(viewers) AS viewers
                   FROM   twitch_game_vc
                   WHERE  epoch >= $1
                          AND epoch < $2
                          AND language = 'en'
                   GROUP  BY game_id) AS v
               ON v.game_id = game.game_id
ORDER  BY v.viewers DESC
LIMIT  $3;