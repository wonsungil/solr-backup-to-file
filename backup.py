import pysolr, logging, logging.handlers, json, time, os, argparse, re
from pathlib import Path
from datetime import datetime

ZOOKEEPERS = "${zookeeper-cluster-hosts}"
COLLECTION = "${solr-cloud-collection}"

def dir_init(dirname):
    Path("./log").mkdir(parents=True, exist_ok=True)
    Path("./dump/{0}".format(dirname)).mkdir(parents=True, exist_ok=True)

def log_init(filename, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    handler = logging.handlers.TimedRotatingFileHandler(filename, when="D")
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger

if __name__ == "__main__":
    abs_path = os.path.dirname(os.path.abspath(__file__))
    os.path.join(abs_path)

    parser = argparse.ArgumentParser(description="Solr Back args")
    parser.add_argument('--date', '-d', default=False, help='Solr Backup Date["regdate_group"] (YYYY or YYYY-MM or YYYY-MM-DD)')
    parser.add_argument('--path', '-p', default='dump', help='backup file path')
    parser.add_argument('--rows', '-r', default=5000, help='paging rows')
    args = parser.parse_args()

    target = None

    if args.date:
        # YYYY OR YYYY-MM OR YYYY-MM-DD
        patterns = '^(?:(\d{4})|(\d{4}-\d{2})|(\d{4}-\d{2}-\d{2}))$'
        r = re.compile(patterns)

        if len(r.findall(args.date)) == 0:
            print("{0} invalid date type".format(args.date))
            exit(0)

        if args.date in r.findall(args.date)[0]:
            target = args.date
    else:
        print("date is not defined")
        exit(0)

    dir_init(target)
    logger = log_init("log/{0}-backup.log".format(target))

    zookeeper = pysolr.ZooKeeper(ZOOKEEPERS, timeout=180)
    solr = pysolr.SolrCloud(zookeeper, COLLECTION, timeout=180)

    query = "regdate_group:{0}*".format(target)

    isFirst = True
    perPage = args.rows
    cursorMark = '*'
    backCount = 0
    done = False

    docs = []

    filename = '{0}/{1}/solr-backup-{2}'.format(args.path, target, target)
    fileNumbering = 0

    while not done:
        options = {'sort':'id asc','rows':perPage,'cursorMark':cursorMark,'start':0}
        results = solr.search(query, **options)

        # 첫 요청 시 numFound 출력
        if isFirst:
            logger.info("hits : {0}".format(results.hits))
            isFirst = False

        # 조회 결과 없을 시 종료
        if results.hits == 0:
            exit(0)

        docs = docs + results.docs
        if len(docs) >= 100000:
            with open("{0}_{1}.json".format(filename, fileNumbering), "w+", encoding="UTF-8") as file:
                json.dump(docs, file)
                docs = []
                fileNumbering = fileNumbering+1

        # 커서가 마지막인 경우 처리 문서 카운팅 제외
        if len(results.docs) > 0:
            backCount = backCount + len(results.docs)
            logger.info("{0} docs backup done".format(backCount))

        # 커서 위치가 마지막인 경우 완료 처리
        if cursorMark == results.nextCursorMark:
            done = True
            with open("{0}_{1}.json".format(filename, fileNumbering), "w+", encoding="UTF-8") as file:
                json.dump(docs, file)
                docs = []
                fileNumbering = fileNumbering+1

        # 커서 마커 변경
        cursorMark = results.nextCursorMark



