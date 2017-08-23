import langdetect
import time


def main():
    titles = [
        'مباراة  الأهلي والقطن الكاميروني بث مباشر ',
        'VER BARCELONA VS REAL MADRID EN VIVO 2017 CLÁSICO ONLINE',
        '6月29日19:00放送予定】リアル版 超･獣神祭 十二支再競争生中継 ！動物達の本気の戦い！1位と2位を当てたら、3億円分を山分け ',
        'XFLAG PARK2017 DAY2【モンスト公式】',
        '二支再競争生中継 '
    ]
    start = time.time()
    iterations = 2000
    for i in range(iterations):
        [langdetect.detect(x) for x in titles]
    end = time.time()
    total = end - start
    num_detected = iterations * len(titles)
    entries_per_second = num_detected/total
    print(f"Total Time for {num_detected} entries: {total:.2}")
    print(f"Entries/Second: {entries_per_second}")


if __name__ == '__main__':
    main()
