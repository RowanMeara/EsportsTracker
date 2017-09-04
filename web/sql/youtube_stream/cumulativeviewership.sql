SELECT SUM(viewers)
FROM youtube_stream
WHERE epoch >= $1 AND epoch < $2;
