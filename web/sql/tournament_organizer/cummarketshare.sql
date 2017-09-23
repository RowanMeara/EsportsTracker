SELECT org.org_name,
       COALESCE(tviewers, 0) + COALESCE(yviewers, 0) AS viewers
FROM   tournament_organizer AS org
       LEFT JOIN (SELECT eo.org_name,
                         Sum(ts.viewers) AS tviewers
                  FROM   twitch_channel AS tc
                         INNER JOIN tournament_organizer AS eo
                                 ON tc.affiliation = eo.org_name
                         INNER JOIN twitch_stream AS ts
                                 ON ts.channel_id = tc.channel_id
                  WHERE  ts.epoch >= $1
                         AND ts.epoch <= $2
                  GROUP  BY eo.org_name) AS twitch
              ON org.org_name = twitch.org_name
       LEFT JOIN (SELECT eo.org_name,
                         Sum(ys.viewers) AS yviewers
                  FROM   youtube_channel AS yc
                         INNER JOIN tournament_organizer AS eo
                                 ON yc.affiliation = eo.org_name
                         INNER JOIN youtube_stream AS ys
                                 ON ys.channel_id = yc.channel_id
                  WHERE  ys.epoch >= $1
                         AND ys.epoch <= $2
                  GROUP  BY eo.org_name) AS youtube
              ON org.org_name = youtube.org_name
ORDER  BY viewers DESC;