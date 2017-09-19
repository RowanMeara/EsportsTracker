SELECT game.NAME,
       t.epoch,
       t.viewers,
       t.ytviewers
FROM   game,
       (SELECT COALESCE(ts.epoch, ys.epoch) AS epoch,
               COALESCE(ts.viewers, 0)      AS viewers,
               COALESCE(ys.ytviewers, 0)    AS ytviewers
        FROM   (SELECT epoch,
                       Sum(viewers) AS ytviewers
                FROM   youtube_stream
                WHERE  game_id = $1
                       AND epoch >= $2
                       AND epoch < $3
                       AND ( language = 'en'
                              OR language = 'd_en' )
                GROUP  BY epoch) AS ys
               FULL OUTER JOIN (SELECT epoch,
                                       Sum(viewers) AS viewers
                                FROM   twitch_stream
                                WHERE  game_id = $1
                                       AND epoch >= $2
                                       AND epoch < $3
                                GROUP  BY epoch) AS ts
                            ON ys.epoch = ts.epoch) AS t
WHERE  game.game_id = $1
ORDER  BY epoch;