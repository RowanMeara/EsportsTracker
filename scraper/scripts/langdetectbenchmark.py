import langdetect
import time
import langid


def main():
    titles = [
        'مباراة  الأهلي والقطن الكاميروني بث مباشر ',
        'VER BARCELONA VS REAL MADRID EN VIVO 2017 CLÁSICO ONLINE',
        '6月29日19:00放送予定】リアル版 超･獣神祭 十二支再競争生中継 ！動物達の本気の戦い！1位と2位を当てたら、3億円分を山分け ',
        'XFLAG PARK2017 DAY2【モンスト公式】',
        '二支再競争生中継'
    ]
    iterations = 100


    start = time.time()
    for i in range(iterations):
        det = [langdetect.detect(x) for x in titles]
    end = time.time()
    total = end - start
    num_detected = iterations * len(titles)
    entries_per_second = num_detected/total
    print(f"Using the detect method from langdetect: {entries_per_second} e/s")
    print(det)

    start = time.time()
    for i in range(iterations):
        det = [langid.classify(x) for x in titles]
    end = time.time()
    total = end - start
    num_detected = iterations * len(titles)
    entries_per_second = num_detected/total
    print(f"Using the detect method from langid: {entries_per_second} e/s")
    print(det)
    # Results are 200p/s for langdetect and 402p/s for langid

if __name__ == '__main__':
    main()
