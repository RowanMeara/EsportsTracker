SELECT          t.game_id,
                game.name,
                COALESCE(t.ythours, 0) AS ythours,
                COALESCE(t.twhours, 0) AS twhours,
                COALESCE(ys.yshours, 0) AS ytallhours,
                COALESCE(ts.tshours, 0) AS twallhours
FROM            (
                                SELECT          COALESCE(game.game_id, g1.game_id) AS game_id,
                                                COALESCE(ytesports.hours, 0) AS ythours,
                                                COALESCE(twesports.hours, 0) AS twhours
                                FROM            (
                                                           SELECT     game_id,
                                                                      Sum(viewers) AS hours
                                                           FROM       (
                                                                             SELECT channel_id
                                                                             FROM   youtube_channel
                                                                             WHERE  affiliation IS NOT NULL) AS c
                                                           INNER JOIN youtube_stream                         AS y
                                                           ON         y.channel_id = c.channel_id
                                                           WHERE      game_id IS NOT NULL
                                                           AND        LEFT(y.language, 2) = 'en'
                                                           AND        y.epoch >= $1
                                                           AND        y.epoch < $2
                                                           GROUP BY   game_id) AS ytesports
                                FULL OUTER JOIN
                                                (
                                                           SELECT     game_id,
                                                                      Sum(viewers) AS hours
                                                           FROM       (
                                                                             SELECT channel_id
                                                                             FROM   twitch_channel
                                                                             WHERE  affiliation IS NOT NULL) AS c
                                                           INNER JOIN twitch_stream                          AS t
                                                           ON         t.channel_id = c.channel_id
                                                           WHERE      game_id IS NOT NULL
                                                           AND        LEFT(t.language, 2) = 'en'
                                                           AND        t.epoch >= $1
                                                           AND        t.epoch < $2
                                                           GROUP BY   game_id) AS twesports
                                ON              ytesports.game_id = twesports.game_id
                                LEFT JOIN       game
                                ON              game.game_id = ytesports.game_id
                                LEFT JOIN       game AS g1
                                ON              g1.game_id = twesports.game_id) AS t
LEFT OUTER JOIN
                (
                         SELECT   ts.game_id,
                                  Sum(ts.viewers) AS tshours
                         FROM     twitch_stream   AS ts
                         WHERE    (
                                           LEFT(ts.language, 2) = 'en'
                                  OR       ts.language = 'd_en')
                         AND      epoch >= $1
                         AND      epoch < $2
                         GROUP BY game_id) AS ts
ON              ts.game_id = t.game_id
LEFT OUTER JOIN
                (
                         SELECT   ys.game_id,
                                  Sum(ys.viewers) AS yshours
                         FROM     youtube_stream  AS ys
                         WHERE    (
                                           LEFT(ys.language, 2) = 'en'
                                  OR       ys.language = 'd_en')
                         AND      epoch >= $1
                         AND      epoch < $2
                         GROUP BY game_id) AS ys
ON              ys.game_id = t.game_id
INNER JOIN      game
ON              game.game_id = t.game_id
ORDER BY        t.ythours + t.twhours DESC
LIMIT $3;