SELECT eo.org_name, SUM(ts.viewers) AS viewers
FROM twitch_channel AS tc
    INNER JOIN tournament_organizer AS eo ON tc.affiliation=eo.org_name
    INNER JOIN twitch_stream AS ts ON ts.channel_id=tc.channel_id
WHERE ts.epoch >= $1 AND ts.epoch <= $2
GROUP BY eo.org_name
ORDER BY viewers DESC;
