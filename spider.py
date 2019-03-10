#coding = utf8
import os, re, requests, shutil, time, json, sqlite3
from queue import Queue, deque
from urllib.parse import urljoin, urlparse
from page import WebPage

max_count = 12000

def parse_url(url):
    """解析url"""
    res = urlparse(url)
    return {'protocal': res[0], 'host': res[1], 'path': res[2]}

# 对象序列化，在json.dump时需要序列化
class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

class Spider:
    pages_dir = "./frontend/views/pages" # pages目录
    data_dir = "./backend/src/data/" # 数据存储目录，存储包括url列表等数据
    db_path = "./searchengine.sqlite" # 数据库路径
    log_path = "./log.txt" # 日志文件路径
    timeout = 2 # 超时时间
    allow_new_link = True # 如果为True这允许边爬边扩充新url到待爬列表中
    to_crawl_list = Queue() # 将要爬取得url
    pages_db = None # pages数目
    pages_count = 0 # 页面数量
    crawled_list = [] # 成功爬取过的url
    seeked_list = set() # 爬取过的url
    blocked_list = {} # 暂时被禁止爬取的url
    black_list = {} # 黑名单
    white_list = set() # 白名单
    acceptable_suffix = {
        "htm", "html", "xhtml", "shtml", "php", "do", "jsp", "asp", "aspx", "action", "txt",  # text
        "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt"  # binary
    } # ok的后缀名

    def __init__(self, start_sites=None, black_list=None, resume=False, new_link=True):
        self.allow_new_link = new_link
        if start_sites is not None:
            for url in start_sites:
                self.to_crawl_list.put(url)
        if black_list is not None:
            self.black_list = black_list
        if resume:
            # 回档，即从上次结束的地方开始，需要先读取上次的数据
            self.resume_data()
            self.pages_db = sqlite3.connect(self.db_path)
        else:
            # 不回档，将所有数据重新生成
            self.create_db()
            if not os.path.exists(self.data_dir):
                os.mkdir(self.data_dir)
            if os.path.exists(self.pages_dir):
                shutil.rmtree(self.pages_dir)
            os.mkdir(self.pages_dir)
            if os.path.exists(self.log_path):
                os.remove(self.log_path)

    def create_db(self):
        """创建数据库"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.pages_db = sqlite3.connect(self.db_path)
        cursor = self.pages_db.cursor()
        try:
            # page 单章页面信息表
            # pageid 页面id
            # url 页面url
            # final_url 由于涉及到重定向之类的，故有该字段
            # is_text 1表示文本文件 2表示二进制文件
            # header_content_type 这个在reponse的header中可见，表示文件类型
            # header_date 获取文件的时间
            # raw_html 页面内容
            # title 页面title
            sql = '''create table page(
                pageid integer primary key,
                url text,
                final_url text,
                is_text integer,
                header_content_type text,
                header_date text,
                raw_html text,
                title text
            );'''
            cursor.execute(sql)
            # link 页面链接关系表
            # linkid 链接关系id
            # src_id 源页面id
            # dst_url 目的页面url(由于可能该url尚未加入到page表中，故此处用url)
            # anchor 锚文本(很有用，将来检索的时候用到)
            sql = '''create table link(
                linkid integer primary key autoincrement,
                src_id integer,
                dst_url text,
                anchor text,
                foreign key(src_id) references page(pageid)
            );'''
            cursor.execute(sql)
        except Exception as e:
            self.log(e)
        finally:
            self.pages_db.commit()

    def makerequest(self, page_url, body=False):
        """返回请求得到的response"""
        host = parse_url(page_url)['host']
        try:
            if body:
                response = requests.get(page_url, timeout=self.timeout)
            else:
                response = requests.head(page_url, timeout=self.timeout)
            self.white_list.add(host)
            return response
        except Exception as e:
            if isinstance(e, requests.ConnectTimeout) or isinstance(e, requests.ReadTimeout):
                if host in self.white_list:
                    self.blocked_list[host] = [page_url]
                else:
                    if self.black_list.get(host) is None:
                        self.black_list[host] = 1
                    else:
                        self.black_list[host] += 1
            self.save_log("爬取失败，错误信息为:" + str(e))
            return None

    def check_url_acpt(self, url):
        """返回该url是否可达，可达到话返回连接信息"""
        acceptable = True
        suffix_pattern = r"\/[^\/]+\.([^\/]+)$"
        path = parse_url(url)['path']
        if re.search(suffix_pattern, path):
            suffix = re.search(suffix_pattern, path).group(1)
            if suffix not in self.acceptable_suffix:
                acceptable = False
        if not parse_url(url)['protocal'].startswith("http"):
            acceptable = False
        if url.find("nankai") == -1 and url.find("nku") == -1 and not re.search(
                r"http://\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?:/.+)?", url):
            acceptable = False

        return acceptable

    def get_last_url(self, page_url):
        """返回是否可达，不可达返回None，可达返回，可以处理重定向问题"""
        host = parse_url(page_url)['host']
        response = self.makerequest(page_url, False)
        if response is None:
            return None
        while int(response.status_code / 100) == 3:
            location = response.headers.get("location")
            if location is None or location == "":
                return None
            else:
                page_url = urljoin(page_url, location)
            response = self.makerequest(page_url, False)
            if response is None:
                return None
        if response.status_code == 200:
            content_type = response.headers.get("content-type")
            if content_type is not None and self.check_url_acpt(page_url):
                return response
        return None

    def save_data(self):
        """将数据从内存存入磁盘"""
        with open(self.data_dir + "to_crawl_list.json", "w") as f:
            json.dump(list(self.to_crawl_list.queue), f, indent=4)
            f.close()
        with open(self.data_dir + "seeked_list.json", "w") as f:
            json.dump(self.seeked_list, f, cls=SetEncoder, indent=4)
            f.close()
        with open(self.data_dir + "crawled_list.json", "w") as f:
            json.dump(self.crawled_list, f, indent=4)
            f.close()
        with open(self.data_dir + "black_list.json", "w") as f:
            json.dump(self.black_list, f, indent=4)
            f.close()
        with open(self.data_dir + "white_list.json", "w") as f:
            json.dump(self.white_list, f, cls=SetEncoder, indent=4)
            f.close()
        with open(self.data_dir + "blocked_list.json", "w") as f:
            json.dump(self.blocked_list, f, indent=4)
            f.close()

    def resume_data(self):
        """回档"""
        with open(self.data_dir + "to_crawl_list.json", 'r') as f:
            to_crawl_list = json.load(f)
            self.to_crawl_list.queue = deque(to_crawl_list)
            f.close()
        with open(self.data_dir + "seeked_list.json", 'r') as f:
            seeked_list = json.load(f)
            self.seeked_list = set(seeked_list)
            f.close()
        with open(self.data_dir + "crawled_list.json", 'r') as f:
            self.crawled_list = json.load(f)
            f.close()
        with open(self.data_dir + "black_list.json", 'r') as f:
            self.black_list = json.load(f)
            f.close()
        with open(self.data_dir + "white_list.json", 'r') as f:
            white_list = json.load(f)
            self.white_list = set(white_list)
            f.close()
        with open(self.data_dir + "blocked_list.json", 'r') as f:
            self.blocked_list = json.load(f)
            f.close()
        self.pages_count = len(self.crawled_list)
        print('pages_count is ' + str(self.pages_count))

    def save_log(self, msg):
        """将msg存入日志文件中并在控制台中进行输出"""
        msg = str(time.strftime("%Y-%m-%d %H:%M:%S\t ", time.localtime())) + str(msg)
        print(msg)
        try:
            with open(self.log_path, 'a') as f:
                f.write(msg + '\n')
        except Exception as e:
            print(e)

    def check_url_valid(self, url):
        """检查url是否在黑名单或者已爬列表中"""
        host = parse_url(url)['host']
        if self.black_list.get(host) is not None and self.black_list.get(host) >= 5:
            self.save_log("该url不可访问")
            return False
        if self.blocked_list.get(host) is not None:
            self.blocked_list[host].append(url)
            self.save_log("该url不可访问")
            return False
        if url in self.seeked_list:
            self.save_log("该url已被访问过")
            return False
        return True

    def run(self):
        """开爬啦"""
        last_count = self.pages_count
        while not self.to_crawl_list.empty():
            if self.pages_count >= max_count:
                break
            # 每爬取一定数量的网页就将数据存入磁盘，休息，添加新url（这里判断last_count和self.page_count是为了防止你pop出一个又push进来一个，然后就一直进行下面操作，代价太大）
            if 0 < self.pages_count != last_count:
                if self.pages_count % 100 == 0:
                    self.save_data()
                if self.pages_count % 300 == 0:
                    self.save_log("爬累了，休息30秒......\n")
                    time.sleep(30)
                if self.pages_count % 500 == 0 or self.to_crawl_list.qsize() <= 5:
                    if len(self.blocked_list) != 0:
                        host = ""
                        for k in self.blocked_list:
                            host = k
                            break
                        page_urls = self.blocked_list.pop(host)
                        for page_url in page_urls:
                            self.to_crawl_list.put(page_url)
            last_count = self.pages_count
            url = self.to_crawl_list.get()
            self.save_log("爬取 " + url)
            if not self.check_url_valid(url):
                self.save_log('该url无效\n')
                continue
            final_header_response = self.get_last_url(url)
            if final_header_response is None:
                self.save_log(url + "该url不可访问\n")
                continue
            final_url = final_header_response.url
            # 处理重定向
            if final_url != url:
                if not self.check_url_valid(final_url):
                    self.save_log('该url无效\n')
                    continue
            self.save_log("爬取第 " + str(self.pages_count) + " 张页面")
            response = self.makerequest(final_url, True)
            self.seeked_list.add(url)
            if response is None:
                continue
            page = WebPage(response, url, self.pages_count)
            self.crawled_list.append(url)
            self.pages_count += 1
            if self.allow_new_link:
                new_links = page.parse_links_and_anchors()
                for new_link in new_links:
                    new_url = new_link.get_url()
                    if self.check_url_acpt(new_url) and new_url not in self.seeked_list:
                        self.to_crawl_list.put(new_url)
            self.save_data()
            page.store(self.pages_db, self.pages_dir)
            time.sleep(0.5)
            self.save_log("页面信息保存成功\n")


if __name__ == '__main__':
    start = ["http://www.nankai.edu.cn/"]
    black = {}
    # 检查是否需要resume
    check_path = './src/data/crawled_list.json'
    resume = False
    if os.path.exists(check_path):
        resume = True
    spider = Spider(start_sites=start, black_list=black, resume=resume)
    spider.run()
