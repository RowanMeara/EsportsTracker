SELECT SUM(viewers)
FROM twitch_game_vc
WHERE epoch >= $1 AND epoch < $2;
