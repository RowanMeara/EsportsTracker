SELECT games.name, v.viewers
FROM games
INNER JOIN
(
    SELECT game_id, SUM(viewers) AS viewers
    FROM twitch_top_games
    WHERE epoch >= $1 AND epoch < $2
    GROUP BY game_id
) AS v ON v.game_id = games.game_id
ORDER BY v.viewers DESC
LIMIT $3;
