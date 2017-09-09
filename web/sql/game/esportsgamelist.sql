SELECT g.name,
       g.game_id
FROM   (SELECT DISTINCT game.name,
                        game.game_id
        FROM   game
               INNER JOIN twitch_stream
                       ON game.game_id = twitch_stream.game_id) AS g,
       (SELECT game.name,
               v.viewers
        FROM   game
               INNER JOIN (SELECT game_id,
                                  Sum(viewers) AS viewers
                           FROM   twitch_game_vc
                           WHERE  epoch >= ( (SELECT Extract(epoch FROM Now()))
                                             - 2592000 )
                                  AND epoch < (SELECT Extract(epoch FROM Now()))
                           GROUP  BY game_id) AS v
                       ON v.game_id = game.game_id
        ORDER  BY v.viewers DESC
        LIMIT  100) AS t
WHERE  t.name = g.name
ORDER  BY t.viewers DESC;